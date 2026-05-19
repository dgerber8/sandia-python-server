"""
Simulation Data Forwarder (dual-port, self-describing format)
=============================================================

Receives simulation data on two UDP ports and POSTs a merged snapshot to
the API Gateway every 5 seconds.

Architecture:
    Sim Computer  --UDP:5005--+
                               +--> This Script --> API Gateway
    Sim Computer  --UDP:5006--+

Packet format (little-endian, identical on both ports):
    header      <dII    timestamp_epoch_s (double), bus_count (uint32), pv_count (uint32)

    per bus:    <H      id_len
                        id_len bytes  UTF-8 bus ID (e.g. "bus01")
                <dddddd voltage, active P, reactive P, face0, face1, face2

    per PV:     <H      id_len
                        id_len bytes  UTF-8 PV name (e.g. "PVSystem.PVSY19")
                <dd     active P, reactive P

Because each entry carries its own ID, the forwarder has no knowledge of
the network topology. Adding ports, buses, or PVs only requires changes to
the sender — nothing here needs to change.

POST tick (every POST_INTERVAL_S):
    - Grabs the latest parsed snapshot for each port.
    - Concatenates their devices and PVSystems lists (A then B).
    - Skips the tick if neither port received anything since the last one.

Configuration:
    API_URL   Target endpoint (or set FORWARDER_API_URL env var).
    PORT_A/B  UDP ports to listen on.

Usage:
    python simulation_forwarder.py
"""

import json
import logging
import os
import socket
import struct
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

# ── Configuration ────────────────────────────────────────────────────────────
API_URL = "https://byvtfz9728.execute-api.us-west-1.amazonaws.com/prod/ingest"

UDP_HOST        = "0.0.0.0"
PORT_A          = 5005
PORT_B          = 5006
UDP_BUFFER_SIZE = 65535
HTTP_TIMEOUT_S  = 5
POST_INTERVAL_S = 5.0

# ── Packet layout ─────────────────────────────────────────────────────────────
HEADER_FMT   = "<dII"                    # timestamp_epoch_s, bus_count, pv_count
HEADER_BYTES = struct.calcsize(HEADER_FMT)
BUS_FMT      = "<dddddd"                 # voltage, active P, reactive P, face0, face1, face2
BUS_BYTES    = struct.calcsize(BUS_FMT)
PV_FMT       = "<dd"                     # active P, reactive P
PV_BYTES     = struct.calcsize(PV_FMT)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("forwarder")

# ── Shared state ─────────────────────────────────────────────────────────────
_state_lock = threading.Lock()

_latest_a_packet: dict | None = None
_latest_b_packet: dict | None = None

_a_received = 0;  _a_dropped = 0
_b_received = 0;  _b_dropped = 0

_stop_event = threading.Event()


# ── Helpers ──────────────────────────────────────────────────────────────────

def resolve_config() -> str:
    url = API_URL or os.environ.get("FORWARDER_API_URL")
    if not url:
        raise SystemExit("API_URL is not set. Edit the script or set FORWARDER_API_URL.")
    return url


def epoch_to_iso(t: float) -> str:
    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Packet reader ─────────────────────────────────────────────────────────────

def read_id(raw: bytes, offset: int) -> tuple[str, int]:
    """Read a length-prefixed UTF-8 ID string. Returns (id, new_offset)."""
    (id_len,) = struct.unpack_from("<H", raw, offset)
    offset += 2
    return raw[offset : offset + id_len].decode("utf-8"), offset + id_len


def parse_combined_packet(raw: bytes, addr) -> "dict | None":
    """
    Parse a self-describing bus+PV packet.
    IDs and names come directly from the packet — no lookup tables needed.
    """
    if len(raw) < HEADER_BYTES:
        log.warning(f"Bad packet from {addr}: {len(raw)} bytes < header {HEADER_BYTES}")
        return None

    try:
        ts_s, n_buses, n_pvs = struct.unpack_from(HEADER_FMT, raw, 0)
        offset = HEADER_BYTES

        devices = []
        for _ in range(n_buses):
            bus_id, offset = read_id(raw, offset)
            v, ap, rp, f0, f1, f2 = struct.unpack_from(BUS_FMT, raw, offset)
            offset += BUS_BYTES
            devices.append({
                "id":             bus_id,
                "voltage":        v,
                "active power":   ap,
                "reactive power": rp,
                "faces":          [f0, f1, f2],
            })

        pvsystems = []
        for _ in range(n_pvs):
            pv_id, offset = read_id(raw, offset)
            ap, rp = struct.unpack_from(PV_FMT, raw, offset)
            offset += PV_BYTES
            pvsystems.append({
                "id":             pv_id,
                "active power":   ap,
                "reactive power": rp,
            })

    except (struct.error, UnicodeDecodeError, IndexError) as e:
        log.warning(f"Bad packet from {addr}: {e}")
        return None

    if offset != len(raw):
        log.warning(f"Packet from {addr}: {len(raw) - offset} unexpected trailing bytes")

    return {
        "timestamp": epoch_to_iso(ts_s),
        "devices":   devices,
        "PVSystems": pvsystems,
    }


# ── UDP receiver loop ────────────────────────────────────────────────────────

def udp_receiver(port: int, topic: str) -> None:
    global _latest_a_packet, _latest_b_packet
    global _a_received, _a_dropped, _b_received, _b_dropped

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_HOST, port))
    sock.settimeout(0.5)
    log.info(f"UDP listener bound to {UDP_HOST}:{port} ({topic})")

    while not _stop_event.is_set():
        try:
            raw, addr = sock.recvfrom(UDP_BUFFER_SIZE)
        except socket.timeout:
            continue
        except OSError as e:
            log.error(f"UDP {topic} socket error: {e}")
            continue

        parsed = parse_combined_packet(raw, addr)
        if parsed is None:
            continue

        with _state_lock:
            if topic == "a":
                if _latest_a_packet is not None:
                    _a_dropped += 1
                _latest_a_packet = parsed
                _a_received += 1
            else:
                if _latest_b_packet is not None:
                    _b_dropped += 1
                _latest_b_packet = parsed
                _b_received += 1

    sock.close()


# ── POST logic ───────────────────────────────────────────────────────────────

def post_payload(url: str, payload: dict) -> None:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
            status = resp.status
            if status >= 300:
                log.warning(f"API returned {status}: {resp.read()[:200]!r}")
            else:
                log.info(f"POST ok ({status})")
    except urllib.error.HTTPError as e:
        log.warning(f"HTTP {e.code}: {e.read()[:200]!r}")
    except urllib.error.URLError as e:
        log.warning(f"Network error: {e.reason}")
    except socket.timeout:
        log.warning("POST timed out")


def post_timer(api_url: str) -> None:
    global _latest_a_packet, _latest_b_packet

    next_tick = time.monotonic() + POST_INTERVAL_S
    while not _stop_event.is_set():
        sleep_for = next_tick - time.monotonic()
        if sleep_for > 0:
            if _stop_event.wait(timeout=sleep_for):
                break
        next_tick += POST_INTERVAL_S

        with _state_lock:
            pkt_a = _latest_a_packet
            pkt_b = _latest_b_packet
            _latest_a_packet = None
            _latest_b_packet = None
            ar, ad = _a_received, _a_dropped
            br, bd = _b_received, _b_dropped

        if pkt_a is None and pkt_b is None:
            log.info("Tick: no new packets on either port")
            continue

        # Merge: A's lists first, then B's. IDs came from the sender so there's
        # nothing to remap here.
        all_devices   = []
        all_pvsystems = []
        for pkt in (pkt_a, pkt_b):
            if pkt:
                all_devices.extend(pkt["devices"])
                all_pvsystems.extend(pkt["PVSystems"])

        payload: dict = {}
        if all_devices:
            payload["devices"] = all_devices
        if all_pvsystems:
            payload["PVSystems"] = all_pvsystems

        ts_candidates = [p["timestamp"] for p in (pkt_a, pkt_b) if p and p.get("timestamp")]
        payload["timestamp"] = max(ts_candidates) if ts_candidates else now_iso()

        log.info(
            f"Tick: portA={ar}r/{ad}d  portB={br}r/{bd}d  "
            f"buses={len(all_devices)}  pvs={len(all_pvsystems)}"
        )
        post_payload(api_url, payload)


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    api_url = resolve_config()
    log.info(f"Forwarding to {api_url}")
    log.info(f"POST interval: {POST_INTERVAL_S} seconds")

    threading.Thread(target=udp_receiver, args=(PORT_A, "a"), daemon=True).start()
    threading.Thread(target=udp_receiver, args=(PORT_B, "b"), daemon=True).start()

    post_timer(api_url)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Shutting down...")
        _stop_event.set()
