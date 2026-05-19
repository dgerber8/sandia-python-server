"""
Multi-port UDP test sender (self-describing format).

Emits to TWO ports at the same cadence. Each packet carries its own bus IDs
and PV names embedded in the payload, so the forwarder requires no lookup
tables or offset configuration.

    PORT_A (5005): buses 1-11  + PVs index 0-16
    PORT_B (5006): buses 12-22 + PVs index 17-34

Packet format (little-endian, identical on both ports):
    header      <dII    timestamp_epoch_s, bus_count, pv_count

    per bus:    <H      id_len
                        id_len bytes  UTF-8 bus ID
                <dddddd voltage, active P, reactive P, face0, face1, face2

    per PV:     <H      id_len
                        id_len bytes  UTF-8 PV name
                <dd     active P, reactive P

To add a bus or PV, update BUS_IDS or PV_NAMES and adjust the split index.
No forwarder changes required.

Usage:
    python multi_port_test_sender.py
"""

import socket
import struct
import time

UDP_HOST = "127.0.0.1"
PORT_A   = 5005
PORT_B   = 5006

# ── Network topology (sender owns these) ─────────────────────────────────────
BUS_IDS = [f"bus{b+1:02d}" for b in range(22)]   # bus01 .. bus22

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

# ── Port split ────────────────────────────────────────────────────────────────
PORT_A_BUS_IDS  = BUS_IDS[:11]    # bus01 .. bus11
PORT_B_BUS_IDS  = BUS_IDS[11:]    # bus12 .. bus22
PORT_A_PV_NAMES = PV_NAMES[:17]   # first 17 PVs
PORT_B_PV_NAMES = PV_NAMES[17:]   # remaining 18 PVs

# ── Packet format ─────────────────────────────────────────────────────────────
HEADER_FMT = "<dII"   # timestamp_epoch_s, bus_count, pv_count

# ── Timing ────────────────────────────────────────────────────────────────────
RATE_HZ         = 5
DURATION_S      = 600
TOTAL_PACKETS   = RATE_HZ * DURATION_S
SEND_INTERVAL_S = 1.0 / RATE_HZ


# ── Packing helpers ───────────────────────────────────────────────────────────

def pack_id(id_str: str) -> bytes:
    encoded = id_str.encode("utf-8")
    return struct.pack("<H", len(encoded)) + encoded


def pack_bus(bus_id: str, voltage, active_p, reactive_p, face0, face1, face2) -> bytes:
    return pack_id(bus_id) + struct.pack("<dddddd", voltage, active_p, reactive_p, face0, face1, face2)


def pack_pv(pv_name: str, active_p, reactive_p) -> bytes:
    return pack_id(pv_name) + struct.pack("<dd", active_p, reactive_p)


def build_packet(ts_epoch: float, bus_ids: list, pv_names: list, bus_offset: int, pv_offset: int,
                 v_swing: float, p_swing: float, pv_swing: float) -> bytes:
    n_buses = len(bus_ids)
    n_pvs   = len(pv_names)
    header  = struct.pack(HEADER_FMT, ts_epoch, n_buses, n_pvs)

    bus_data = b""
    for i, bus_id in enumerate(bus_ids):
        b = bus_offset + i
        voltage        = 1.0    + (b * 0.002) + v_swing
        active_power   = 1900.0 + (b * 50)    + p_swing
        reactive_power = 200.0  + (b * 10)    + p_swing * 0.1
        face0 = voltage + 0.030
        face1 = voltage - 0.025
        face2 = voltage + 0.055
        bus_data += pack_bus(bus_id, voltage, active_power, reactive_power, face0, face1, face2)

    pv_data = b""
    for i, pv_name in enumerate(pv_names):
        p = pv_offset + i
        active_power   = 500.0 + (p * 8) + pv_swing
        reactive_power = 50.0  + (p * 1) + pv_swing * 0.1
        pv_data += pack_pv(pv_name, active_power, reactive_power)

    return header + bus_data + pv_data


# ── Main loop ─────────────────────────────────────────────────────────────────

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

print(f"Sending {TOTAL_PACKETS} packet pairs at {RATE_HZ}/s for {DURATION_S}s")
print(f"  Port A ({PORT_A}): {len(PORT_A_BUS_IDS)} buses + {len(PORT_A_PV_NAMES)} PVs")
print(f"  Port B ({PORT_B}): {len(PORT_B_BUS_IDS)} buses + {len(PORT_B_PV_NAMES)} PVs")
print(f"  Merged payload:   {len(BUS_IDS)} buses + {len(PV_NAMES)} PVs")
print(f"Forwarder should POST ~{DURATION_S // 5} times\n")

start     = time.monotonic()
next_send = start

for i in range(TOTAL_PACKETS):
    sleep_for = next_send - time.monotonic()
    if sleep_for > 0:
        time.sleep(sleep_for)
    next_send += SEND_INTERVAL_S

    ts_epoch  = time.time()
    cycle_pos = i % 10
    depth     = cycle_pos if cycle_pos <= 5 else (10 - cycle_pos)
    v_swing   = -depth * 0.1
    p_swing   = -depth * 150.0
    pv_swing  = -depth * 25.0

    pkt_a = build_packet(ts_epoch, PORT_A_BUS_IDS, PORT_A_PV_NAMES,
                         bus_offset=0,  pv_offset=0,
                         v_swing=v_swing, p_swing=p_swing, pv_swing=pv_swing)
    pkt_b = build_packet(ts_epoch, PORT_B_BUS_IDS, PORT_B_PV_NAMES,
                         bus_offset=len(PORT_A_BUS_IDS), pv_offset=len(PORT_A_PV_NAMES),
                         v_swing=v_swing, p_swing=p_swing, pv_swing=pv_swing)

    sock.sendto(pkt_a, (UDP_HOST, PORT_A))
    sock.sendto(pkt_b, (UDP_HOST, PORT_B))

    if (i + 1) % RATE_HZ == 0:
        elapsed = time.monotonic() - start
        print(f"  t={elapsed:5.2f}s  sent pair {i + 1}/{TOTAL_PACKETS}")

sock.close()
print(f"\nDone. Check DynamoDB for ~{DURATION_S // 5} new records.")
print(f"Each record should have {len(BUS_IDS)} devices and {len(PV_NAMES)} PVSystems.")
