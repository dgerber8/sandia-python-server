"""
Multi-port UDP test sender (unified format).

Emits to TWO ports at the same cadence to exercise the multi-port forwarder.
Both ports carry the same packet structure — each with half the buses and
half the PVs — which the forwarder merges into one payload per tick.

    PORT_A (5005): buses 1-11   + PVs index 0-16
    PORT_B (5006): buses 12-22  + PVs index 17-34

Packet format (little-endian, identical on both ports):
    header  <dII      timestamp_epoch_s (double), bus_count (uint32), pv_count (uint32)
    body    bus_count × <dddddd   voltage, active P, reactive P, face0, face1, face2
            pv_count  × <dd       active P, reactive P

Same triangle-wave oscillation as test_sender_burst so values fluctuate
visibly across the run. 5 packet pairs/sec for 600 s = 3,000 pairs.
With the forwarder POSTing every 5 s, expect ~120 DynamoDB records each
containing 22 devices and 35 PVSystems.

Usage:
    python multi_port_test_sender.py
"""

import socket
import struct
import time

UDP_HOST = "127.0.0.1"
PORT_A   = 5005
PORT_B   = 5006

# ── Split configuration (must match forwarder PORT_*_BUS_ID_START / PV_ID_START) ──
PORT_A_BUS_COUNT = 11   # buses 1-11   (bus_id_start=0  in forwarder)
PORT_B_BUS_COUNT = 11   # buses 12-22  (bus_id_start=11 in forwarder)
PORT_A_PV_COUNT  = 17   # PV_NAMES indices 0-16   (pv_id_start=0  in forwarder)
PORT_B_PV_COUNT  = 18   # PV_NAMES indices 17-34  (pv_id_start=17 in forwarder)

TOTAL_BUSES = PORT_A_BUS_COUNT + PORT_B_BUS_COUNT   # 22
TOTAL_PVS   = PORT_A_PV_COUNT  + PORT_B_PV_COUNT    # 35

HEADER_FMT = "<dII"   # timestamp_epoch_s, bus_count, pv_count

RATE_HZ         = 5     # packet pairs per second
DURATION_S      = 600   # how long to send for
TOTAL_PACKETS   = RATE_HZ * DURATION_S
SEND_INTERVAL_S = 1.0 / RATE_HZ

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

print(f"Sending {TOTAL_PACKETS} packet pairs at {RATE_HZ}/s for {DURATION_S}s")
print(f"  Port A ({PORT_A}): {PORT_A_BUS_COUNT} buses + {PORT_A_PV_COUNT} PVs")
print(f"  Port B ({PORT_B}): {PORT_B_BUS_COUNT} buses + {PORT_B_PV_COUNT} PVs")
print(f"  Merged payload:   {TOTAL_BUSES} buses + {TOTAL_PVS} PVs")
print(f"Forwarder should POST ~{DURATION_S // 5} times "
      f"(once every 5s, merging the latest of each port)\n")

start = time.monotonic()
next_send = start

for i in range(TOTAL_PACKETS):
    sleep_for = next_send - time.monotonic()
    if sleep_for > 0:
        time.sleep(sleep_for)
    next_send += SEND_INTERVAL_S

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

    # ── Compute values for all buses and PVs up front ─────────────────────
    all_bus_values = []
    for b in range(TOTAL_BUSES):
        voltage        = 1.0    + (b * 0.002) + v_swing
        active_power   = 1900.0 + (b * 50)    + p_swing
        reactive_power = 200.0  + (b * 10)    + p_swing * 0.1
        face0 = voltage + 0.030
        face1 = voltage - 0.025
        face2 = voltage + 0.055
        all_bus_values.append((voltage, active_power, reactive_power, face0, face1, face2))

    all_pv_values = []
    for p in range(TOTAL_PVS):
        active_power   = 500.0 + (p * 8) + pv_swing
        reactive_power = 50.0  + (p * 1) + pv_swing * 0.1
        all_pv_values.append((active_power, reactive_power))

    # ── Port A packet: first half of buses + first half of PVs ───────────
    a_buses = all_bus_values[:PORT_A_BUS_COUNT]
    a_pvs   = all_pv_values[:PORT_A_PV_COUNT]
    pkt_a = (
        struct.pack(HEADER_FMT, ts_epoch, PORT_A_BUS_COUNT, PORT_A_PV_COUNT)
        + struct.pack("<" + "dddddd" * PORT_A_BUS_COUNT, *[v for bus in a_buses for v in bus])
        + struct.pack("<" + "dd"     * PORT_A_PV_COUNT,  *[v for pv  in a_pvs  for v in pv])
    )

    # ── Port B packet: second half of buses + second half of PVs ─────────
    b_buses = all_bus_values[PORT_A_BUS_COUNT:]
    b_pvs   = all_pv_values[PORT_A_PV_COUNT:]
    pkt_b = (
        struct.pack(HEADER_FMT, ts_epoch, PORT_B_BUS_COUNT, PORT_B_PV_COUNT)
        + struct.pack("<" + "dddddd" * PORT_B_BUS_COUNT, *[v for bus in b_buses for v in bus])
        + struct.pack("<" + "dd"     * PORT_B_PV_COUNT,  *[v for pv  in b_pvs  for v in pv])
    )

    sock.sendto(pkt_a, (UDP_HOST, PORT_A))
    sock.sendto(pkt_b, (UDP_HOST, PORT_B))

    if (i + 1) % RATE_HZ == 0:
        elapsed = time.monotonic() - start
        print(f"  t={elapsed:5.2f}s  sent pair {i + 1}/{TOTAL_PACKETS}")

sock.close()
print(f"\nDone. Check DynamoDB for ~{DURATION_S // 5} new records.")
print(f"Each record should have {TOTAL_BUSES} devices and {TOTAL_PVS} PVSystems.")
