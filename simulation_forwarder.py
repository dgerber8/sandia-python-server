"""
Simulation Data Forwarder
==========================

Receives simulation data from Speedgoat via UDP and POSTs the most recent
packet to a remote API Gateway endpoint over HTTPS, on a fixed 5-second
interval.

Architecture:
    Speedgoat  --UDP-->  This Script (Host PC)  --HTTPS POST-->  API Gateway

Behavior:
    - UDP packets are received continuously and the latest one is cached.
    - Every POST_INTERVAL_S seconds, the cached latest packet is POSTed.
    - Older packets are silently dropped; only the freshest data is sent.
    - If no packets have arrived since the last POST, nothing is sent.

Configuration:
    Set the API_URL value below, OR set the FORWARDER_API_URL environment
    variable before running.

Usage:
    python simulation_forwarder.py

Speedgoat should send JSON-encoded UDP packets to UDP_HOST:UDP_PORT in the
agreed-upon format:

    {
        "timestamp": "2025-05-04T12:34:56Z",
        "devices": [
            {"id": "bus01", "voltage": 380.2, "power": 1939},
            {"id": "bus02", "voltage": 379.8, "power": 1861}
        ]
    }
"""

import json
import logging
import os
import socket
import threading
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
import struct

# -------- Configuration --------
# Either edit this value, or leave it as None and set the matching
# FORWARDER_API_URL environment variable.
API_URL = "https://byvtfz9728.execute-api.us-west-1.amazonaws.com/prod/ingest"

UDP_HOST = "0.0.0.0"       # Listen on all interfaces (Speedgoat + loopback for testing)
UDP_PORT = 5005        # UDP port the simulation will send to
UDP_BUFFER_SIZE = 65535
HTTP_TIMEOUT_S = 5     # Don't let a slow API call back up the UDP queue
POST_INTERVAL_S = 5.0  # Target seconds between POSTs

# -------- Logging --------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("forwarder")


# -------- Shared state between UDP receiver and POST timer --------
_state_lock = threading.Lock()
_latest_packet: dict | None = None
_received_count: int = 0       # total UDP packets received
_dropped_count: int = 0        # packets dropped because a newer one arrived
_stop_event = threading.Event()  # set by Ctrl+C to signal shutdown


def resolve_config() -> str:
    url = API_URL or os.environ.get("FORWARDER_API_URL")
    if not url:
        raise SystemExit(
            "API_URL is not set. Edit the script or set FORWARDER_API_URL."
        )
    return url


def post_payload(url: str, payload: dict) -> None:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
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


def udp_receiver() -> None:
    """Continuously receive UDP packets and cache the most recent valid one."""
    global _latest_packet, _received_count, _dropped_count

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_HOST, UDP_PORT))
    # Short timeout so we can periodically check the stop flag instead of
    # blocking forever in recvfrom (which would ignore Ctrl+C on Windows).
    sock.settimeout(0.5)
    log.info(f"UDP listener bound to {UDP_HOST}:{UDP_PORT}")

    while not _stop_event.is_set():
        try:
            raw, addr = sock.recvfrom(UDP_BUFFER_SIZE)
        except socket.timeout:
            continue
        except OSError as e:
            log.error(f"UDP socket error: {e}")
            continue

    # sock.sendto(json.dumps(payload).encode("utf-8"), (UDP_HOST, UDP_PORT))

        try:
            v1, p1, v2, p2 = struct.unpack('<dddd', raw)
            data = {
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "devices": [
                    {"id": "bus01", "voltage": v1, "power": p1},
                    {"id": "bus02", "voltage": v2, "power": p2},
                ],
            }
        except (struct.error, UnicodeDecodeError, json.JSONDecodeError) as e:
            log.warning(f"Bad packet from {addr}: {e}")
            continue

        if not isinstance(data, dict) or "devices" not in data:
            log.warning(f"Packet from {addr} missing required fields")
            continue

        # Stamp on receipt if Speedgoat didn't include a timestamp
        if "timestamp" not in data:
            data["timestamp"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        with _state_lock:
            if _latest_packet is not None:
                _dropped_count += 1
            _latest_packet = data
            _received_count += 1

    sock.close()


def post_timer(api_url: str) -> None:
    """Every POST_INTERVAL_S seconds, POST the latest cached packet (if any).

    Uses an absolute schedule (next_tick += interval) rather than sleeping a
    fixed duration, so timing drift from the POST itself doesn't accumulate.
    Sleeps via Event.wait() so Ctrl+C wakes us immediately.
    """
    global _latest_packet

    next_tick = time.monotonic() + POST_INTERVAL_S
    while not _stop_event.is_set():
        sleep_for = next_tick - time.monotonic()
        if sleep_for > 0:
            # Returns True if the event was set during the wait → exit cleanly
            if _stop_event.wait(timeout=sleep_for):
                break
        next_tick += POST_INTERVAL_S

        with _state_lock:
            packet = _latest_packet
            _latest_packet = None
            received = _received_count
            dropped = _dropped_count

        if packet is None:
            log.info("Tick: no new packets to send")
            continue

        log.info(
            f"Tick: sending latest of {received} received "
            f"({dropped} dropped this run)"
        )
        post_payload(api_url, packet)


def main() -> None:
    api_url = resolve_config()
    log.info(f"Forwarding to {api_url}")
    log.info(f"POST interval: {POST_INTERVAL_S} seconds")

    receiver_thread = threading.Thread(target=udp_receiver, daemon=True)
    receiver_thread.start()

    # Run the POST timer on the main thread so Ctrl+C works cleanly
    post_timer(api_url)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Shutting down...")
        _stop_event.set()