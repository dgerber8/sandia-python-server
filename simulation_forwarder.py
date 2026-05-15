"""
Simulation Data Forwarder (multi-port)
=======================================

Receives simulation data from the simulator over UDP on *two* ports and POSTs
the most recent merged snapshot to a remote API Gateway endpoint over HTTPS on
a fixed 5-second interval.

Architecture:
    Sim Computer  --UDP:5005 (buses)----+
                                        +-->  This Script  --HTTPS POST-->  API Gateway
    Sim Computer  --UDP:5006 (PVs)------+

Per-port behavior:
    - Each port has its own listener thread; the most recent packet for that
      topic is cached. Older packets are silently dropped.
    - Both packets carry their own timestamp (an epoch-seconds double in the
      header). The forwarder converts to ISO before sending.

POST tick (every POST_INTERVAL_S):
    - Whatever is cached for each topic is merged into one JSON body:
        { timestamp, devices?, PVSystems? }
    - If a topic hasn't received anything since the last tick, that field is
      omitted. If neither has, nothing is sent.

Packet formats (little-endian throughout):

    Bus port 5005:
        header  <dI       timestamp_epoch_s, bus_count
        body    bus_count × <dddddd  voltage, active P, reactive P, face0, face1, face2

    PV port 5006:
        header  <dI       timestamp_epoch_s, pv_count
        body    pv_count  × <dd      active P, reactive P
        (id resolved by index against PV_NAMES below)

Configuration:
    Set API_URL below, OR set FORWARDER_API_URL in the environment.

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
BUS_PORT        = 5005
PV_PORT         = 5006
UDP_BUFFER_SIZE = 65535
HTTP_TIMEOUT_S  = 5
POST_INTERVAL_S = 5.0

# ── Bus packet layout ────────────────────────────────────────────────────────
BUS_HEADER_FMT   = "<dI"                 # timestamp (epoch s), bus_count
BUS_HEADER_BYTES = struct.calcsize(BUS_HEADER_FMT)
FIELDS_PER_BUS   = 6                     # voltage, active P, reactive P, face[0..2]
BYTES_PER_BUS    = FIELDS_PER_BUS * 8

# ── PV packet layout ─────────────────────────────────────────────────────────
PV_HEADER_FMT    = "<dI"                 # timestamp (epoch s), pv_count
PV_HEADER_BYTES  = struct.calcsize(PV_HEADER_FMT)
FIELDS_PER_PV    = 2                     # active P, reactive P
BYTES_PER_PV     = FIELDS_PER_PV * 8

# PVSystem id order — index in the wire format maps to this list.
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

# Per-topic cache. Each holds the most recent parsed dict ready to merge into
# a POST body. Cleared after every POST tick.
_latest_bus_packet: dict | None = None     # { "timestamp": "...", "devices": [...] }
_latest_pv_packet:  dict | None = None     # { "timestamp": "...", "PVSystems": [...] }

# Per-topic counters reported once per POST tick for diagnostics.
_bus_received = 0
_bus_dropped  = 0
_pv_received  = 0
_pv_dropped   = 0

_stop_event = threading.Event()


# ── Helpers ──────────────────────────────────────────────────────────────────

def resolve_config() -> str:
    url = API_URL or os.environ.get("FORWARDER_API_URL")
    if not url:
        raise SystemExit("API_URL is not set. Edit the script or set FORWARDER_API_URL.")
    return url


def epoch_to_iso(t: float) -> str:
    """Convert an epoch-seconds double to the ISO-8601 string the API expects."""
    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Packet parsers ───────────────────────────────────────────────────────────

def parse_bus_packet(raw: bytes, addr) -> dict | None:
    n_bytes = len(raw)
    if n_bytes < BUS_HEADER_BYTES:
        log.warning(f"Bad bus packet from {addr}: {n_bytes} bytes < header {BUS_HEADER_BYTES}")
        return None
    try:
        ts_s, n_buses = struct.unpack(BUS_HEADER_FMT, raw[:BUS_HEADER_BYTES])
        expected = BUS_HEADER_BYTES + n_buses * BYTES_PER_BUS
        if n_bytes != expected:
            log.warning(f"Bad bus packet from {addr}: size {n_bytes} ≠ expected {expected} (buses={n_buses})")
            return None
        bus_values = struct.unpack("<" + "dddddd" * n_buses, raw[BUS_HEADER_BYTES:])
        devices = [
            {
                "id": f"bus{b + 1:02d}",
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
        return {"timestamp": epoch_to_iso(ts_s), "devices": devices}
    except struct.error as e:
        log.warning(f"Bad bus packet from {addr}: {e}")
        return None


def parse_pv_packet(raw: bytes, addr) -> dict | None:
    n_bytes = len(raw)
    if n_bytes < PV_HEADER_BYTES:
        log.warning(f"Bad pv packet from {addr}: {n_bytes} bytes < header {PV_HEADER_BYTES}")
        return None
    try:
        ts_s, n_pvs = struct.unpack(PV_HEADER_FMT, raw[:PV_HEADER_BYTES])
        expected = PV_HEADER_BYTES + n_pvs * BYTES_PER_PV
        if n_bytes != expected:
            log.warning(f"Bad pv packet from {addr}: size {n_bytes} ≠ expected {expected} (pvs={n_pvs})")
            return None
        pv_values = struct.unpack("<" + "dd" * n_pvs, raw[PV_HEADER_BYTES:])
        pvsystems = [
            {
                "id":             PV_NAMES[p] if p < len(PV_NAMES) else f"PVSystem.UNKNOWN{p}",
                "active power":   pv_values[p * FIELDS_PER_PV],
                "reactive power": pv_values[p * FIELDS_PER_PV + 1],
            }
            for p in range(n_pvs)
        ]
        return {"timestamp": epoch_to_iso(ts_s), "PVSystems": pvsystems}
    except struct.error as e:
        log.warning(f"Bad pv packet from {addr}: {e}")
        return None


# ── UDP receiver loop (generic over topic) ───────────────────────────────────

def udp_receiver(port: int, topic: str, parser) -> None:
    """Continuously receive UDP packets on `port` and cache the latest one for `topic`."""
    global _latest_bus_packet, _latest_pv_packet
    global _bus_received, _bus_dropped, _pv_received, _pv_dropped

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_HOST, port))
    # Short timeout so we can periodically check the stop flag instead of
    # blocking forever in recvfrom (which would ignore Ctrl+C on Windows).
    sock.settimeout(0.5)
    log.info(f"UDP {topic} listener bound to {UDP_HOST}:{port}")

    while not _stop_event.is_set():
        try:
            raw, addr = sock.recvfrom(UDP_BUFFER_SIZE)
        except socket.timeout:
            continue
        except OSError as e:
            log.error(f"UDP {topic} socket error: {e}")
            continue

        parsed = parser(raw, addr)
        if parsed is None:
            continue

        with _state_lock:
            if topic == "bus":
                if _latest_bus_packet is not None:
                    _bus_dropped += 1
                _latest_bus_packet = parsed
                _bus_received += 1
            else:  # "pv"
                if _latest_pv_packet is not None:
                    _pv_dropped += 1
                _latest_pv_packet = parsed
                _pv_received += 1

    sock.close()


# ── POST timer ───────────────────────────────────────────────────────────────

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
    """Every POST_INTERVAL_S, merge the latest bus + PV caches into one body and POST."""
    global _latest_bus_packet, _latest_pv_packet

    next_tick = time.monotonic() + POST_INTERVAL_S
    while not _stop_event.is_set():
        sleep_for = next_tick - time.monotonic()
        if sleep_for > 0:
            if _stop_event.wait(timeout=sleep_for):
                break
        next_tick += POST_INTERVAL_S

        with _state_lock:
            bus_packet = _latest_bus_packet
            pv_packet  = _latest_pv_packet
            _latest_bus_packet = None
            _latest_pv_packet  = None
            br, bd = _bus_received, _bus_dropped
            pr, pd = _pv_received,  _pv_dropped

        if bus_packet is None and pv_packet is None:
            log.info("Tick: no new packets on either port")
            continue

        payload: dict = {}
        if bus_packet:
            payload["devices"]   = bus_packet["devices"]
        if pv_packet:
            payload["PVSystems"] = pv_packet["PVSystems"]

        # Use the newer of the two received timestamps as the payload timestamp;
        # if neither topic carried one we stamp on egress.
        candidate_ts = [p["timestamp"] for p in (bus_packet, pv_packet) if p and p.get("timestamp")]
        payload["timestamp"] = max(candidate_ts) if candidate_ts else now_iso()

        log.info(
            f"Tick: bus={br}r/{bd}d  pv={pr}r/{pd}d  sending fields={sorted(payload.keys())}"
        )
        post_payload(api_url, payload)


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    api_url = resolve_config()
    log.info(f"Forwarding to {api_url}")
    log.info(f"POST interval: {POST_INTERVAL_S} seconds")

    # One receiver thread per topic/port.
    threading.Thread(
        target=udp_receiver, args=(BUS_PORT, "bus", parse_bus_packet), daemon=True,
    ).start()
    threading.Thread(
        target=udp_receiver, args=(PV_PORT, "pv", parse_pv_packet), daemon=True,
    ).start()

    # Run the POST timer on the main thread so Ctrl+C works cleanly.
    post_timer(api_url)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Shutting down...")
        _stop_event.set()
