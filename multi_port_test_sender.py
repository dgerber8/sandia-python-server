"""
Multi-port UDP test sender.

Emits to TWO ports at the same cadence to exercise the multi-port forwarder:
    - UDP 127.0.0.1:5005  → bus data  (22 buses, voltage / P / Q / faces)
    - UDP 127.0.0.1:5006  → PV system data (35 PVs, P / Q)

Both packets carry their own timestamp (an epoch-seconds double in the header)
so the forwarder doesn't need to stamp on receipt.

Same triangle-wave oscillation as the single-port `test_sender_burst` so values
fluctuate visibly across the run. 5 packets/sec for 600 s = 3,000 packets per
port. With the forwarder POSTing every 5 s, expect ~120 DynamoDB records.

Packet formats (little-endian throughout):

    Bus port 5005:
        header  <dI       timestamp_epoch_s, bus_count
        body    bus_count × <dddddd  voltage, active P, reactive P, face0, face1, face2

    PV port 5006:
        header  <dI       timestamp_epoch_s, pv_count
        body    pv_count  × <dd      active P, reactive P

Usage:
    python test_sender_multi.py
"""

import socket
import struct
import time

UDP_HOST = "127.0.0.1"
BUS_PORT = 5005
PV_PORT  = 5006

BUS_COUNT = 22
PV_COUNT  = 35

BUS_HEADER_FMT = "<dI"     # timestamp (epoch s), bus_count
PV_HEADER_FMT  = "<dI"     # timestamp (epoch s), pv_count
BUS_FMT        = "dddddd"  # 6 doubles per bus
PV_FMT         = "dd"      # 2 doubles per PV

RATE_HZ         = 5     # packets per second per port
DURATION_S      = 600   # how long to send for
TOTAL_PACKETS   = RATE_HZ * DURATION_S
SEND_INTERVAL_S = 1.0 / RATE_HZ

bus_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
pv_sock  = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

print(f"Sending {TOTAL_PACKETS} packets per port at {RATE_HZ}/s for {DURATION_S}s")
print(f"  Bus port {BUS_PORT}: {BUS_COUNT} buses (voltage, active P, reactive P, 3 faces)")
print(f"  PV  port {PV_PORT}: {PV_COUNT} PV systems (active P, reactive P)")
print(f"Forwarder should POST ~{DURATION_S // 5} times "
      f"(once every 5s, merging the latest of each topic)\n")

start = time.monotonic()
next_send = start

for i in range(TOTAL_PACKETS):
    sleep_for = next_send - time.monotonic()
    if sleep_for > 0:
        time.sleep(sleep_for)
    next_send += SEND_INTERVAL_S

    # Shared wall-clock timestamp for both ports on this tick.
    ts_epoch = time.time()

    # Triangle-wave oscillation: drop 5 steps then rise 5 steps, repeat.
    cycle_pos = i % 10
    if cycle_pos <= 5:
        v_swing  = -cycle_pos * 0.1
        p_swing  = -cycle_pos * 150.0
        pv_swing = -cycle_pos * 25.0
    else:
        v_swing  = -(10 - cycle_pos) * 0.1
        p_swing  = -(10 - cycle_pos) * 150.0
        pv_swing = -(10 - cycle_pos) * 25.0

    # ── Bus packet ───────────────────────────────────────────────────────
    bus_values = []
    for b in range(BUS_COUNT):
        voltage        = 1.0    + (b * 0.002) + v_swing
        active_power   = 1900.0 + (b * 50)    + p_swing
        reactive_power = 200.0  + (b * 10)    + p_swing * 0.1  # MVAR tracks P loosely
        face0 = voltage + 0.030
        face1 = voltage - 0.025
        face2 = voltage + 0.055
        bus_values.extend([voltage, active_power, reactive_power, face0, face1, face2])

    bus_payload = (
        struct.pack(BUS_HEADER_FMT, ts_epoch, BUS_COUNT)
        + struct.pack("<" + BUS_FMT * BUS_COUNT, *bus_values)
    )
    bus_sock.sendto(bus_payload, (UDP_HOST, BUS_PORT))

    # ── PV packet ────────────────────────────────────────────────────────
    pv_values = []
    for p in range(PV_COUNT):
        active_power   = 500.0 + (p * 8) + pv_swing
        reactive_power = 50.0  + (p * 1) + pv_swing * 0.1
        pv_values.extend([active_power, reactive_power])

    pv_payload = (
        struct.pack(PV_HEADER_FMT, ts_epoch, PV_COUNT)
        + struct.pack("<" + PV_FMT * PV_COUNT, *pv_values)
    )
    pv_sock.sendto(pv_payload, (UDP_HOST, PV_PORT))

    if (i + 1) % RATE_HZ == 0:
        elapsed = time.monotonic() - start
        print(f"  t={elapsed:5.2f}s  sent {i + 1}/{TOTAL_PACKETS} to each port")

bus_sock.close()
pv_sock.close()
print(f"\nDone. Check DynamoDB for ~{DURATION_S // 5} new records with both `devices` and `PVSystems`.")
