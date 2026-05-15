"""
High-rate UDP test sender to verify the forwarder's 5-second throttle.

Sends 5 packets per second for 600 seconds (3000 packets total). With the
forwarder set to POST every 5 seconds, you should see ~120 records written
to DynamoDB at the end of this run.

Packet layout:
    header:    <II  (bus_count, pv_count)
    bus data:  bus_count × <dddddd  (voltage, active power, reactive power,
                                     face0, face1, face2)
    pv data:   pv_count  × <dd      (active power, reactive power)

Values drift over time so you can verify the forwarder is sending the
*latest* packet each tick (not the first one received in the window).

Usage:
    python test_sender_burst.py
"""

import socket
import struct
import time

UDP_HOST = "127.0.0.1"
UDP_PORT = 5005

BUS_COUNT = 22
PV_COUNT  = 35

HEADER_FMT = "<II"
BUS_FMT    = "dddddd"  # 6 doubles per bus
PV_FMT     = "dd"      # 2 doubles per PV system

RATE_HZ         = 5     # packets per second
DURATION_S      = 600   # how long to send for
TOTAL_PACKETS   = RATE_HZ * DURATION_S
SEND_INTERVAL_S = 1.0 / RATE_HZ

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

print(f"Sending {TOTAL_PACKETS} packets at {RATE_HZ}/s for {DURATION_S}s")
print(f"Buses: {BUS_COUNT}  |  PVSystems: {PV_COUNT}")
print(f"Forwarder should POST ~{DURATION_S // 5} times "
      f"(once every 5s, sending the latest packet seen)\n")

start = time.monotonic()
next_send = start

for i in range(TOTAL_PACKETS):
    sleep_for = next_send - time.monotonic()
    if sleep_for > 0:
        time.sleep(sleep_for)
    next_send += SEND_INTERVAL_S

    # Triangle-wave oscillation: drop 5 steps then rise 5 steps, repeat
    # Voltage step: 0.1 pu  |  Bus power step: 150 MW  |  PV power step: 25 MW
    cycle_pos = i % 10
    if cycle_pos <= 5:
        v_swing  = -cycle_pos * 0.1
        p_swing  = -cycle_pos * 150.0
        pv_swing = -cycle_pos * 25.0
    else:
        v_swing  = -(10 - cycle_pos) * 0.1
        p_swing  = -(10 - cycle_pos) * 150.0
        pv_swing = -(10 - cycle_pos) * 25.0

    # ── Bus values ────────────────────────────────────────────────────────
    bus_values = []
    for b in range(BUS_COUNT):
        voltage        = 1.0    + (b * 0.002) + v_swing
        active_power   = 1900.0 + (b * 50)    + p_swing
        reactive_power = 200.0  + (b * 10)    + p_swing * 0.1  # MVAR tracks power loosely
        # Three face values: each tracks voltage with a fixed per-face offset so they
        # stay visually distinct on a graph but all follow the same oscillation shape.
        face0 = voltage + 0.030
        face1 = voltage - 0.025
        face2 = voltage + 0.055
        bus_values.extend([voltage, active_power, reactive_power, face0, face1, face2])

    # ── PV system values ─────────────────────────────────────────────────
    # Active around 500 MW with per-PV offset; reactive ~10% of active.
    pv_values = []
    for p in range(PV_COUNT):
        active_power   = 500.0 + (p * 8)  + pv_swing
        reactive_power = 50.0  + (p * 1)  + pv_swing * 0.1
        pv_values.extend([active_power, reactive_power])

    payload = (
        struct.pack(HEADER_FMT, BUS_COUNT, PV_COUNT)
        + struct.pack("<" + BUS_FMT * BUS_COUNT, *bus_values)
        + struct.pack("<" + PV_FMT  * PV_COUNT,  *pv_values)
    )
    sock.sendto(payload, (UDP_HOST, UDP_PORT))

    if (i + 1) % RATE_HZ == 0:
        elapsed = time.monotonic() - start
        print(f"  t={elapsed:5.2f}s  sent {i + 1}/{TOTAL_PACKETS}")

sock.close()
print(f"\nDone. Check DynamoDB for ~{DURATION_S // 5} new records.")
print(f"Each record should have {BUS_COUNT} devices and {PV_COUNT} PVSystems.")
