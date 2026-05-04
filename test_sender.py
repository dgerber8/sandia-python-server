"""
Quick UDP test sender to verify the simulation server is receiving and
exposing data correctly. Sends a few packets matching the agreed-upon schema.

Usage:
    python test_sender.py
"""

import json
import socket
import time
from datetime import datetime, timezone

UDP_HOST = "127.0.0.1"
UDP_PORT = 5005

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

for i in range(5):
    payload = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "devices": [
            {"id": "bus01", "voltage": 380.2 + i * 0.1, "power": 1939 + i},
            {"id": "bus02", "voltage": 379.8 - i * 0.1, "power": 1861 - i},
        ],
    }
    sock.sendto(json.dumps(payload).encode("utf-8"), (UDP_HOST, UDP_PORT))
    print(f"Sent packet {i + 1}: {payload}")
    time.sleep(1)

sock.close()
