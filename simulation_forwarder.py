"""
Simulation Data Forwarder (dual-port, unified format)
======================================================

Receives simulation data on *two* UDP ports, each carrying the same packet
structure (buses + PVSystems for its half of the network), then merges both
into a single JSON body and POSTs it to the API Gateway every 5 seconds.

Architecture:
    Sim Computer  --UDP:5005 (buses 1-11  + PVs  0-16)--+
                                                          +--> This Script --> API Gateway
    Sim Computer  --UDP:5006 (buses 12-22 + PVs 17-34)--+

Packet format (little-endian, identical on both ports):
    header  <dII      timestamp_epoch_s (double), bus_count (uint32), pv_count (uint32)
    body    bus_count × <dddddd   voltage, active P, reactive P, face0, face1, face2
            pv_count  × <dd       active P, reactive P

POST tick (every POST_INTERVAL_S):
    - Grabs the latest packet cached for each port.
    - Merges their devices lists and PVSystems lists into one payload.
    - If neither port has received anything since the last tick, nothing is sent.

Configuration:
    Set API_URL below, OR set FORWARDER_API_URL in the environment.
    PORT_A/B_BUS_ID_START and PORT_A/B_PV_ID_START must match whatever the
    sender uses to split the data.

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
PORT_A          = 5005   # first half:  buses 1-11,  PVs index 0-16
PORT_B          = 5006   # second half: buses 12-22, PVs index 17-34
UDP_BUFFER_SIZE = 65535
HTTP_TIMEOUT_S  = 5
POST_INTERVAL_S = 5.0

# Starting offsets so bus IDs and PV names are assigned correctly when merging.
# PORT_B offsets must equal however many buses/PVs port A carries per packet.
PORT_A_BUS_ID_START = 0   # bus IDs become bus01 .. bus11
PORT_B_BUS_ID_START = 11  # bus IDs become bus12 .. bus22
PORT_A_PV_ID_START  = 0   # PV_NAMES[0..16]
PORT_B_PV_ID_START  = 17  # PV_NAMES[17..34]

# ── Packet layout ─────────────────────────────────────────────────────────────
COMBINED_HEADER_FMT   = "<dII"   # timestamp_epoch_s, bus_count, pv_count
COMBINED_HEADER_BYTES = struct.calcsize(COMBINED_HEADER_FMT)
FIELDS_PER_BUS        = 6        # voltage, active P, reactive P, face0, face1, face2
BYTES_PER_BUS         = FIELDS_PER_BUS * 8
FIELDS_PER_PV         = 2        # active P, reactive P
BYTES_PER_PV          = FIELDS_PER_PV * 8

# PV name order — index in the wire format maps to this list.
PV_NAMES = [
    "PVSystem.PVSY19",  "PVSystem.PVSY35",
    "PVSystem.PVSY291", "PVSystem.PVSY292", "PVSystem.PVSY293",
    "PVSystem.PVSY294", "PVSystem.PVSY295", "PVSystem.PVSY296",
    "PVSystem.PVSY297", "PVSystem.PVSY298", "PVSystem.PVSY299",
    "PVSystem.PVSY300", "PVSystem.PVSY301", "PVSystem.PVSY302",
    "PVSystem.PVSY303", "PVSystem.PVSY304", "PVSystem.PVSY305",
    "PVSystem.PVSY306", "PVSystem.PVSY307", "PVSystem.PVSY308",
    "PVSystem.PVSY309", "PVSystem.PVSY310", "PVSystem.PVSY311",
    "PVSystem.PVSY312", "PVSystem.PVSY313", "PVSystem.PVSY314",
    "PVSystem.PVSY315", "PVSystem.PVSY316", "PVSystem.PVSY317",
    "PVSystem.PVSY318", "PVSystem.PVSY319", "PVSystem.PVSY320",
    "PVSystem.PVSY321", "PVSystem.PVSY322", "PVSystem.PVSY323",
]

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("forwarder")

# ── Shared state ─────────────────────────────────────────────────────────────
_state_lock = threading.Lock()

# Latest parsed snapshot per port. Each dict has the shape:
#   { "timestamp": "<ISO>", "devices": [...], "PVSystems": [...] }
# Cleared to None after every POST tick.
_latest_a_packet: dict | None = None
_latest_b_packet: dict | None = None

# Diagnostic counters (never reset, reported each tick).
_a_received = 0
_a_dropped  = 0
_b_received = 0
_b_dropped  = 0

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


# ── Packet parser ─────────────────────────────────────────────────────────────

def parse_combined_packet(
    raw: bytes,
    addr,
    bus_id_start: int,
    pv_id_start: int,
) -> "dict | None":
    """
    Parse a combined bus+PV binary packet.

    bus_id_start: added to the in-packet bus index to form the bus ID string
                  (e.g. bus_id_start=11 → first bus in packet becomes bus12)
    pv_id_start:  index into PV_NAMES for the first PV in this packet
    """
    n_bytes = len(raw)
    if n_bytes < COMBINED_HEADER_BYTES:
        log.warning(f"Bad packet from {addr}: {n_bytes} bytes < header {COMBINED_HEADER_BYTES}")
        return None

    try:
        ts_s, n_buses, n_pvs = struct.unpack(COMBINED_HEADER_FMT, raw[:COMBINED_HEADER_BYTES])
    except struct.error as e:
        log.warning(f"Bad header from {addr}: {e}")
        return None

    expected = COMBINED_HEADER_BYTES + n_buses * BYTES_PER_BUS + n_pvs * BYTES_PER_PV
    if n_bytes != expected:
        log.warning(
            f"Bad packet from {addr}: size {n_bytes} != expected {expected} "
            f"(buses={n_buses}, pvs={n_pvs})"
        )
        return None

    try:
        bus_offset = COMBINED_HEADER_BYTES
        bus_values = struct.unpack(
            "<" + "dddddd" * n_buses,
            raw[bus_offset : bus_offset + n_buses * BYTES_PER_BUS],
        )

        pv_offset = bus_offset + n_buses * BYTES_PER_BUS
        pv_values = struct.unpack(
            "<" + "dd" * n_pvs,
            raw[pv_offset : pv_offset + n_pvs * BYTES_PER_PV],
        )
    except struct.error as e:
        log.warning(f"Bad packet body from {addr}: {e}")
        return None

    devices = [
        {
            "id":             f"bus{bus_id_start + b + 1:02d}",
            "voltage":        bus_values[b * FIELDS_PER_BUS],
            "active power":   bus_values[b * FIELDS_PER_BUS + 1],
            "reactive power": bus_values[b * FIELDS_PER_BUS + 2],
            "faces": [
                bus_values[b * FIELDS_PER_BUS + 3],
                bus_values[b * FIELDS_PER_BUS + 4],
                bus_values[b * FIELDS_PER_BUS + 5],
            ],
        }
        for b in range(n_buses)
    ]

    pvsystems = [
        {
            "id":             PV_NAMES[pv_id_start + p] if (pv_id_start + p) < len(PV_NAMES)
                              else f"PVSystem.UNKNOWN{pv_id_start + p}",
            "active power":   pv_values[p * FIELDS_PER_PV],
            "reactive power": pv_values[p * FIELDS_PER_PV + 1],
        }
        for p in range(n_pvs)
    ]

    return {
        "timestamp": epoch_to_iso(ts_s),
        "devices":   devices,
        "PVSystems": pvsystems,
    }


# ── UDP receiver loop ────────────────────────────────────────────────────────

def udp_receiver(port: int, topic: str, bus_id_start: int, pv_id_start: int) -> None:
    """Receive UDP packets on `port` and cache the most recent parsed snapshot."""
    global _latest_a_packet, _latest_b_packet
    global _a_received, _a_dropped, _b_received, _b_dropped

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_HOST, port))
    sock.settimeout(0.5)   # allows the stop flag to be checked periodically
    log.info(f"UDP port {port} ({topic}) listener bound to {UDP_HOST}:{port}")

    while not _stop_event.is_set():
        try:
            raw, addr = sock.recvfrom(UDP_BUFFER_SIZE)
        except socket.timeout:
            continue
        except OSError as e:
            log.error(f"UDP {topic} socket error: {e}")
            continue

        parsed = parse_combined_packet(raw, addr, bus_id_start, pv_id_start)
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
    """Every POST_INTERVAL_S, merge the latest snapshots from both ports and POST."""
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

        # Merge both ports' lists — order is A then B so IDs stay sorted.
        all_devices   = []
        all_pvsystems = []
        for pkt in (pkt_a, pkt_b):
            if pkt:
                all_devices.extend(pkt.get("devices",   []))
                all_pvsystems.extend(pkt.get("PVSystems", []))

        payload: dict = {}
        if all_devices:
            payload["devices"] = all_devices
        if all_pvsystems:
            payload["PVSystems"] = all_pvsystems

        # Use the later of the two packet timestamps; fall back to now().
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
    log.info(f"Port A ({PORT_A}): bus_id_start={PORT_A_BUS_ID_START}, pv_id_start={PORT_A_PV_ID_START}")
    log.info(f"Port B ({PORT_B}): bus_id_start={PORT_B_BUS_ID_START}, pv_id_start={PORT_B_PV_ID_START}")

    threading.Thread(
        target=udp_receiver,
        args=(PORT_A, "a", PORT_A_BUS_ID_START, PORT_A_PV_ID_START),
        daemon=True,
    ).start()
    threading.Thread(
        target=udp_receiver,
        args=(PORT_B, "b", PORT_B_BUS_ID_START, PORT_B_PV_ID_START),
        daemon=True,
    ).start()

    # POST timer runs on the main thread so Ctrl+C works cleanly.
    post_timer(api_url)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Shutting down...")
        _stop_event.set()
