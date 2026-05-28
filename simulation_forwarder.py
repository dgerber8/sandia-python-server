"""
Jeewon Forwarder — raw 153-float UDP → API Gateway
====================================================

Listens on a single UDP port for Jeewon's simulation output, decodes the
fixed-layout 153-float payload, and POSTs a merged snapshot to the API
Gateway every POST_INTERVAL_S seconds.

Packet format (from Jeewon's model, port 5005):
    612 bytes — 153 native-endian floats, NO header, NO IDs.
    struct.unpack('153f', data)

    30 groups × 5 floats each: [VA, VB, VC, active_power, reactive_power]
    + 3 trailing floats:        [active_power, reactive_power, (reserved)]

    Groups 0–20  → 21 buses (bus01–bus19, bus21, bus22 — note: bus20 absent)
    Groups 21–29 → 9 PV systems (buses 4, 5, 6, 8, 9, 10, 13, 21, 22)
    Floats 150–151 → Load.LOAD748 (active power, reactive power)

Bus voltage reported to the API is mean(VA, VB, VC).
Faces [VA, VB, VC] are forwarded as-is.

POST tick (every POST_INTERVAL_S):
    - Grabs the latest parsed snapshot.
    - Skips the tick if no new packet arrived since the last one.

Configuration:
    API_URL          Target endpoint (or set FORWARDER_API_URL env var).
    UDP_PORT         Port to listen on (default 5005).
    POST_INTERVAL_S  How often to POST (default 5 s).

Usage:
    python jeewon_forwarder.py
"""

import json
import logging
import os
import socket
import ssl
import struct
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

# ── SSL context ───────────────────────────────────────────────────────────────
# On restricted Windows lab machines the network proxy re-signs TLS with an
# institutional CA that lives in the Windows cert store but not in Python's
# bundled store.  Resolution order:
#   1. truststore  — makes Python use the Windows/macOS/Linux system store
#   2. certifi     — ships its own Mozilla CA bundle
#   3. default ssl — whatever Python has built-in
# Set FORWARDER_NO_SSL_VERIFY=1 to skip verification entirely (demo fallback).
_NO_VERIFY = os.environ.get("FORWARDER_NO_SSL_VERIFY", "").strip() == "1"

if _NO_VERIFY:
    _SSL_CTX = ssl._create_unverified_context()
else:
    try:
        import truststore
        _SSL_CTX = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    except ImportError:
        try:
            import certifi
            _SSL_CTX = ssl.create_default_context(cafile=certifi.where())
        except ImportError:
            _SSL_CTX = ssl.create_default_context()

# ── Configuration ─────────────────────────────────────────────────────────────
API_URL = os.environ.get(
    "FORWARDER_API_URL",
    "https://byvtfz9728.execute-api.us-west-1.amazonaws.com/prod/ingest",
)

UDP_HOST        = "0.0.0.0"
UDP_PORT        = 5005
UDP_BUFFER_SIZE = 65535
HTTP_TIMEOUT_S  = 5
POST_INTERVAL_S = 5.0

# ── Fixed packet layout ───────────────────────────────────────────────────────
# 153 native-endian single-precision floats, 612 bytes total, no header.
FLOAT_COUNT   = 153
PACKET_BYTES  = FLOAT_COUNT * struct.calcsize("f")  # 612
FLOATS_FMT    = f"{FLOAT_COUNT}f"                   # '153f'  (native endian)

# Bus group mapping: group index → API bus ID
# Groups 0–18: bus01–bus19; groups 19–20: bus21–bus22 (bus20 is absent)
BUS_GROUPS = [
    (0,  "bus01"), (1,  "bus02"), (2,  "bus03"), (3,  "bus04"), (4,  "bus05"),
    (5,  "bus06"), (6,  "bus07"), (7,  "bus08"), (8,  "bus09"), (9,  "bus10"),
    (10, "bus11"), (11, "bus12"), (12, "bus13"), (13, "bus14"), (14, "bus15"),
    (15, "bus16"), (16, "bus17"), (17, "bus18"), (18, "bus19"),
    (19, "bus21"),  # bus20 is absent in Jeewon's format
    (20, "bus22"),
]

# PV group mapping: group index → PV system ID sent to the API.
# ⚠  Update these names to match the exact PVSystem IDs used in your
#    OpenDSS model / DynamoDB records if they differ.
PV_GROUPS = [
    (21, "PVSystem.PVSY19"),
    (22, "PVSystem.PVSY303"),
    (23, "PVSystem.PVSY321"),
    (24, "PVSystem.PVSY306"),
    (25, "PVSystem.PVSY294"),
    (26, "PVSystem.PVSY318"),
    (27, "PVSystem.PVSY291"),
    (28, "PVSystem.PVSY315"),
    (29, "PVSystem.PVSY297"),
]

# Load layout: (float_index_of_active_power, load_id)
# Floats 150–152 = active power, reactive power, voltage.
LOAD_ENTRIES = [
    (150, "Load.LOAD748"),
]

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("jeewon_forwarder")

# ── Shared state ─────────────────────────────────────────────────────────────
_state_lock  = threading.Lock()
_latest_pkt: "dict | None" = None
_received    = 0
_dropped     = 0
_stop_event  = threading.Event()


# ── Helpers ──────────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Packet parser ─────────────────────────────────────────────────────────────

def parse_jeewon_packet(raw: bytes, addr) -> "dict | None":
    """
    Decode Jeewon's raw 150-float UDP packet into the API POST schema.

    Returns None and logs a warning if the packet is the wrong size or
    cannot be unpacked.
    """
    if len(raw) != PACKET_BYTES:
        log.warning(
            f"Bad packet from {addr}: expected {PACKET_BYTES} bytes, got {len(raw)}"
        )
        return None

    try:
        floats = struct.unpack(FLOATS_FMT, raw)
    except struct.error as e:
        log.warning(f"Unpack error from {addr}: {e}")
        return None

    # Each group is 5 consecutive floats: [VA, VB, VC, active_power, reactive_power]
    def group(g: int):
        base = g * 5
        return floats[base], floats[base+1], floats[base+2], floats[base+3], floats[base+4]

    devices = []
    for g_idx, bus_id in BUS_GROUPS:
        va, vb, vc, ap, rp = group(g_idx)
        voltage = (va + vb + vc) / 3.0
        devices.append({
            "id":             bus_id,
            "voltage":        round(voltage, 6),
            "active power":   round(ap, 4),
            "reactive power": round(rp, 4),
            "faces":          [round(va, 6), round(vb, 6), round(vc, 6)],
        })

    pvsystems = []
    for g_idx, pv_id in PV_GROUPS:
        _va, _vb, _vc, ap, rp = group(g_idx)
        pvsystems.append({
            "id":             pv_id,
            "active power":   round(ap, 4),
            "reactive power": round(rp, 4),
        })

    loads = []
    for ap_idx, load_id in LOAD_ENTRIES:
        ap      = floats[ap_idx]
        rp      = floats[ap_idx + 1]
        voltage = floats[ap_idx + 2]
        loads.append({
            "active power":   round(ap, 4),
            "reactive power": round(rp, 4),
            "voltage":        round(voltage, 6),
            "id":             load_id,
        })

    return {
        "timestamp": now_iso(),
        "devices":   devices,
        "PVSystems": pvsystems,
        "Loads":     loads,
    }


# ── UDP receiver loop ────────────────────────────────────────────────────────

def udp_receiver() -> None:
    global _latest_pkt, _received, _dropped

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_HOST, UDP_PORT))
    sock.settimeout(0.5)
    log.info(f"UDP listener bound to {UDP_HOST}:{UDP_PORT}")

    while not _stop_event.is_set():
        try:
            raw, addr = sock.recvfrom(UDP_BUFFER_SIZE)
        except socket.timeout:
            continue
        except OSError as e:
            log.error(f"Socket error: {e}")
            continue

        parsed = parse_jeewon_packet(raw, addr)
        if parsed is None:
            continue

        with _state_lock:
            if _latest_pkt is not None:
                _dropped += 1
            _latest_pkt = parsed
            _received  += 1

    sock.close()


# ── POST logic ───────────────────────────────────────────────────────────────

def post_payload(payload: dict) -> None:
    body = json.dumps(payload).encode("utf-8")
    req  = urllib.request.Request(
        API_URL, data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S, context=_SSL_CTX) as resp:
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


def post_timer() -> None:
    global _latest_pkt

    next_tick = time.monotonic() + POST_INTERVAL_S
    while not _stop_event.is_set():
        sleep_for = next_tick - time.monotonic()
        if sleep_for > 0:
            if _stop_event.wait(timeout=sleep_for):
                break
        next_tick += POST_INTERVAL_S

        with _state_lock:
            pkt = _latest_pkt
            _latest_pkt = None
            rx, dr = _received, _dropped

        if pkt is None:
            log.info("Tick: no new packet since last POST")
            continue

        log.info(
            f"Tick: rx={rx}  dropped={dr}  "
            f"buses={len(pkt['devices'])}  pvs={len(pkt['PVSystems'])}  "
            f"loads={len(pkt['Loads'])}"
        )
        post_payload(pkt)


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    if not API_URL:
        raise SystemExit("API_URL is not set. Edit the script or set FORWARDER_API_URL.")

    log.info(f"Forwarding to {API_URL}")
    log.info(f"Expecting {PACKET_BYTES}-byte packets (153 floats) on port {UDP_PORT}")
    log.info(f"POST interval: {POST_INTERVAL_S} s  |  "
             f"{len(BUS_GROUPS)} buses  |  {len(PV_GROUPS)} PV systems  |  "
             f"{len(LOAD_ENTRIES)} loads")

    threading.Thread(target=udp_receiver, daemon=True).start()
    post_timer()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Shutting down...")
        _stop_event.set()
