"""
Jeewon Test Sender — synthetic 260-float UDP packets
=====================================================

Generates synthetic simulation data in Jeewon's exact raw-float format and
sends it to localhost:5005 so you can test jeewon_forwarder.py without
needing her live simulation model.

Packet format (matches Jeewon's model output):
    1020 bytes — 255 native-endian single-precision floats, no header.
    struct.unpack('255f', data)

    51 groups × 5 floats: [VA, VB, VC, active_power, reactive_power]

    Groups 0–20  → 21 buses  (bus01–bus19, bus21, bus22)
    Groups 21–29 → 9  PV systems (buses 4, 5, 6, 8, 9, 10, 13, 21, 22)
    Groups 30–50 → 21 loads  (b_1–b_19, b_21–b_22 — b_20 absent)

Synthetic values:
    Voltages (per-phase, per-unit): ~1.00 pu with ±0.05 pu cyclic swing
    Bus active power:               ~1900 W/bus with slow drift
    Bus reactive power:             ~200 VAR/bus
    PV active power:                ~500 W  (rises when voltage is high,
                                            drops toward 0 at night-sim)
    PV reactive power:              ~50 VAR

Usage:
    python jeewon_sender.py                     # default: 5 Hz, 10 min
    python jeewon_sender.py --hz 10 --dur 60    # 10 Hz for 60 s
"""

import argparse
import math
import socket
import struct
import time

# ── Target ─────────────────────────────────────────────────────────────────────
UDP_HOST = "127.0.0.1"
UDP_PORT = 5005

# ── Packet constant ────────────────────────────────────────────────────────────
FLOAT_COUNT = 255
FLOATS_FMT  = f"{FLOAT_COUNT}f"   # '255f' — native endian, matches Jeewon's unpack

# ── Fixed layout (must stay in sync with jeewon_forwarder.py) ─────────────────
# 30 entries total; each is (group_index, label) for documentation only.
# Groups 0–20: buses
BUS_LAYOUT = [
    (0,  "bus01"), (1,  "bus02"), (2,  "bus03"), (3,  "bus04"), (4,  "bus05"),
    (5,  "bus06"), (6,  "bus07"), (7,  "bus08"), (8,  "bus09"), (9,  "bus10"),
    (10, "bus11"), (11, "bus12"), (12, "bus13"), (13, "bus14"), (14, "bus15"),
    (15, "bus16"), (16, "bus17"), (17, "bus18"), (18, "bus19"),
    (19, "bus21"),  # bus20 absent
    (20, "bus22"),
]
# Groups 21–29: PV systems
PV_LAYOUT = [
    (21, "PVSystem.bus04"), (22, "PVSystem.bus05"), (23, "PVSystem.bus06"),
    (24, "PVSystem.bus08"), (25, "PVSystem.bus09"), (26, "PVSystem.bus10"),
    (27, "PVSystem.bus13"), (28, "PVSystem.bus21"), (29, "PVSystem.bus22"),
]

# Groups 30–50: 21 loads (b_1–b_19, b_21–b_22) — same 5-float layout as buses/PVs
LOAD_LAYOUT = [
    (30, "Load.LOAD1681"),  # b_1
    (31, "Load.LOAD1680"),  # b_2
    (32, "Load.LOAD1650"),  # b_3
    (33, "Load.LOAD1687"),  # b_4
    (34, "Load.LOAD1672"),  # b_5
    (35, "Load.LOAD1679"),  # b_6
    (36, "Load.LOAD1668"),  # b_7
    (37, "Load.LOAD1688"),  # b_8
    (38, "Load.LOAD1662"),  # b_9
    (39, "Load.LOAD1686"),  # b_10
    (40, "Load.LOAD1656"),  # b_11
    (41, "Load.LOAD1665"),  # b_12
    (42, "Load.LOAD1659"),  # b_13
    (43, "Load.LOAD1653"),  # b_14
    (44, "Load.LOAD1682"),  # b_15
    (45, "Load.LOAD1684"),  # b_16
    (46, "Load.LOAD1685"),  # b_17
    (47, "Load.LOAD1676"),  # b_18
    (48, "Load.LOAD1675"),  # b_19
    (49, "Load.LOAD1683"),  # b_21
    (50, "Load.LOAD748"),   # b_22
]

assert len(BUS_LAYOUT) + len(PV_LAYOUT) == 30, "layout must have exactly 30 groups"


# ── Synthetic value generators ────────────────────────────────────────────────

def _pu_voltage(t: float, bus_idx: int, phase: int) -> float:
    """
    Per-unit voltage for a given bus and phase.
    Slow sinusoidal swing ±0.05 pu over a 60-second cycle.
    Buses spread slightly around nominal (0.98–1.02 pu at rest).
    Phases offset by 120° from each other.
    """
    base    = 1.0 + (bus_idx - 10) * 0.001   # gentle per-bus spread
    swing   = 0.05 * math.sin(2 * math.pi * t / 60.0)
    phase_v = 0.01 * math.sin(2 * math.pi * t / 60.0 + phase * (2 * math.pi / 3))
    return base + swing + phase_v


def _bus_active_power(t: float, bus_idx: int) -> float:
    """Active power drawn by a bus (W). Ramps up over the day, ±150 W ripple."""
    base   = 1900.0 + bus_idx * 50.0
    ripple = 150.0 * math.sin(2 * math.pi * t / 30.0 + bus_idx)
    return base + ripple


def _bus_reactive_power(t: float, bus_idx: int) -> float:
    """Reactive power for a bus (VAR)."""
    return 200.0 + bus_idx * 10.0 + 20.0 * math.cos(2 * math.pi * t / 45.0)


def _pv_active_power(t: float, pv_idx: int) -> float:
    """PV output active power (W). Slow 90-second ramp cycle; floored at 0."""
    peak   = 500.0 + pv_idx * 8.0
    factor = max(0.0, math.sin(math.pi * (t % 90.0) / 90.0))
    return peak * factor


def _pv_reactive_power(t: float, pv_idx: int) -> float:
    """PV reactive power (VAR) — small and in phase with active output."""
    return _pv_active_power(t, pv_idx) * 0.10


def _load_active_power(t: float, load_idx: int) -> float:
    """Active power consumed by a load (W). Varies per load index."""
    base   = 190.0 + load_idx * 10.0
    ripple = 15.0 * math.sin(2 * math.pi * t / 60.0 + load_idx)
    return base + ripple


def _load_reactive_power(t: float, load_idx: int) -> float:
    """Reactive power for a load (VAR). Varies per load index."""
    return 51.0 + load_idx * 3.0 + 5.0 * math.cos(2 * math.pi * t / 60.0 + load_idx)


# ── Packet builder ────────────────────────────────────────────────────────────

def build_packet(t: float) -> bytes:
    """
    Build one 1040-byte packet of 260 native floats for wall-clock time `t`.
    """
    floats: list[float] = [0.0] * FLOAT_COUNT

    for group_idx, (_, _label) in enumerate(BUS_LAYOUT):
        base = group_idx * 5
        va = _pu_voltage(t, group_idx, 0)
        vb = _pu_voltage(t, group_idx, 1)
        vc = _pu_voltage(t, group_idx, 2)
        ap = _bus_active_power(t, group_idx)
        rp = _bus_reactive_power(t, group_idx)
        floats[base : base + 5] = [va, vb, vc, ap, rp]

    for pv_seq, (group_idx, _label) in enumerate(PV_LAYOUT):
        base = group_idx * 5
        # PV voltage phases trail the grid slightly
        va = _pu_voltage(t, pv_seq, 0) - 0.005
        vb = _pu_voltage(t, pv_seq, 1) - 0.005
        vc = _pu_voltage(t, pv_seq, 2) - 0.005
        ap = _pv_active_power(t, pv_seq)
        rp = _pv_reactive_power(t, pv_seq)
        floats[base : base + 5] = [va, vb, vc, ap, rp]

    for load_seq, (group_idx, _label) in enumerate(LOAD_LAYOUT):
        base = group_idx * 5
        va = _pu_voltage(t, load_seq, 0)
        vb = _pu_voltage(t, load_seq, 1)
        vc = _pu_voltage(t, load_seq, 2)
        ap = _load_active_power(t, load_seq)
        rp = _load_reactive_power(t, load_seq)
        floats[base : base + 5] = [va, vb, vc, ap, rp]

    return struct.pack(FLOATS_FMT, *floats)


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Jeewon format UDP test sender")
    parser.add_argument("--hz",  type=float, default=5.0,   help="Packets per second (default 5)")
    parser.add_argument("--dur", type=float, default=600.0, help="Duration in seconds (default 600)")
    parser.add_argument("--host", default=UDP_HOST,         help=f"Target host (default {UDP_HOST})")
    parser.add_argument("--port", type=int, default=UDP_PORT, help=f"Target port (default {UDP_PORT})")
    args = parser.parse_args()

    rate     = args.hz
    duration = args.dur
    total    = int(rate * duration)
    interval = 1.0 / rate
    target   = (args.host, args.port)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    print(f"Jeewon test sender")
    print(f"  Target  : {target[0]}:{target[1]}")
    print(f"  Rate    : {rate} Hz  |  Duration: {duration} s  |  Packets: {total}")
    print(f"  Payload : {FLOAT_COUNT * 4} bytes ({FLOAT_COUNT} floats, no header)")
    print(f"  Layout  : {len(BUS_LAYOUT)} bus groups + {len(PV_LAYOUT)} PV groups + {len(LOAD_LAYOUT)} load(s)")
    print(f"  Forwarder POSTs every 5 s → expect ~{int(duration // 5)} API calls\n")

    start     = time.monotonic()
    next_send = start

    for i in range(total):
        sleep_for = next_send - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        next_send += interval

        t   = time.monotonic() - start
        pkt = build_packet(t)
        sock.sendto(pkt, target)

        if (i + 1) % max(1, int(rate)) == 0:
            elapsed = time.monotonic() - start
            print(f"  t={elapsed:6.2f}s  sent {i + 1}/{total}")

    sock.close()
    print(f"\nDone. {total} packets sent in {time.monotonic() - start:.1f} s.")
    print("Check DynamoDB for new records — each should have "
          f"{len(BUS_LAYOUT)} devices, {len(PV_LAYOUT)} PVSystems, and {len(LOAD_LAYOUT)} load(s).")


if __name__ == "__main__":
    main()
