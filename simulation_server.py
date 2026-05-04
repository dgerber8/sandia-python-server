"""
Simulation Data Bridge Server
==============================

Receives simulation data from Speedgoat via UDP and exposes it through a
GET endpoint over HTTP.

Architecture:
    Speedgoat  --UDP-->  This Server (Host PC)  --HTTP GET-->  Client/MIDAAS

Usage:
    python simulation_server.py

Then test with:
    curl http://localhost:8000/data
    curl http://localhost:8000/health
    curl http://localhost:8000/history

Speedgoat should send JSON-encoded UDP packets to UDP_HOST:UDP_PORT in this
format:

    {
        "timestamp": "2025-05-04T12:34:56Z",
        "devices": [
            {"id": "bus01", "voltage": 380.2, "power": 1939},
            {"id": "bus02", "voltage": 379.8, "power": 1861}
        ]
    }
"""

import json
import socket
import threading
import logging
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

# -------- Configuration --------
UDP_HOST = "0.0.0.0"      # Listen on all interfaces for Speedgoat packets
UDP_PORT = 5005           # UDP port the simulation will send to
HTTP_HOST = "0.0.0.0"     # HTTP host for the GET endpoint
HTTP_PORT = 8000          # HTTP port for the GET endpoint
UDP_BUFFER_SIZE = 65535   # Max UDP packet size
HISTORY_SIZE = 100        # Number of recent samples to retain in memory

# -------- Logging --------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("sim-bridge")


class DataStore:
    """Thread-safe store for the latest simulation data and a small history."""

    def __init__(self, history_size: int = HISTORY_SIZE):
        self._lock = threading.Lock()
        self._latest = None
        self._history = deque(maxlen=history_size)
        self._packets_received = 0
        self._last_received_at = None

    def update(self, data: dict) -> None:
        with self._lock:
            self._latest = data
            self._history.append(data)
            self._packets_received += 1
            self._last_received_at = datetime.now(timezone.utc).isoformat()

    def latest(self) -> dict | None:
        with self._lock:
            return self._latest

    def history(self) -> list:
        with self._lock:
            return list(self._history)

    def stats(self) -> dict:
        with self._lock:
            return {
                "packets_received": self._packets_received,
                "last_received_at": self._last_received_at,
                "history_length": len(self._history),
            }


store = DataStore()


# -------- UDP Listener --------
def udp_listener():
    """Listen for UDP packets from Speedgoat and store them."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_HOST, UDP_PORT))
    log.info(f"UDP listener bound to {UDP_HOST}:{UDP_PORT}")

    while True:
        try:
            raw, addr = sock.recvfrom(UDP_BUFFER_SIZE)
        except OSError as e:
            log.error(f"UDP socket error: {e}")
            continue

        try:
            text = raw.decode("utf-8").strip()
            data = json.loads(text)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            log.warning(f"Bad packet from {addr}: {e}")
            continue

        # Light validation against the agreed-upon schema
        if not isinstance(data, dict) or "devices" not in data:
            log.warning(f"Packet from {addr} missing required fields: {data}")
            continue

        # Stamp the packet on receipt if Speedgoat didn't include a timestamp
        if "timestamp" not in data:
            data["timestamp"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        store.update(data)
        log.info(
            f"Received packet from {addr[0]}:{addr[1]} "
            f"({len(data.get('devices', []))} devices)"
        )


# -------- HTTP Handler --------
class APIHandler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, body: dict | list) -> None:
        payload = json.dumps(body, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if self.path in ("/", "/data", "/latest"):
            data = store.latest()
            if data is None:
                self._send_json(
                    503,
                    {"error": "No simulation data received yet."},
                )
            else:
                self._send_json(200, data)

        elif self.path == "/history":
            self._send_json(200, store.history())

        elif self.path == "/health":
            self._send_json(
                200,
                {"status": "ok", **store.stats()},
            )

        else:
            self._send_json(
                404,
                {"error": f"Unknown path: {self.path}"},
            )

    # Quiet the default per-request stderr logging; we use our own logger
    def log_message(self, format, *args):
        log.debug("HTTP %s - %s" % (self.address_string(), format % args))


# -------- Entry Point --------
def main():
    # Start UDP listener thread
    t = threading.Thread(target=udp_listener, daemon=True)
    t.start()

    # Start HTTP server on the main thread
    httpd = HTTPServer((HTTP_HOST, HTTP_PORT), APIHandler)
    log.info(f"HTTP server listening on http://{HTTP_HOST}:{HTTP_PORT}")
    log.info("Endpoints: GET /data  GET /history  GET /health")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down.")
        httpd.server_close()


if __name__ == "__main__":
    main()
