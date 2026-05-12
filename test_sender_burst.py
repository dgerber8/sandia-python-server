"""
High-rate UDP test sender to verify the forwarder's 5-second throttle.

Sends 5 packets per second for 60 seconds (300 packets total). With the
forwarder set to POST every 5 seconds, you should see exactly 12 records
written to DynamoDB at the end of this run.

Each packet contains voltage + reactive power for 22 buses. Voltage values
drift linearly across the run so you can verify the forwarder is sending
the *latest* packet each tick (not the first one received in the window).

Usage:
    python test_sender_burst.py
"""

import socket
import struct
import time

UDP_HOST = "127.0.0.1"
UDP_PORT = 5005

BUS_COUNT        = 22
STRUCT_FMT       = "<" + "ddd" * BUS_COUNT  # voltage, active power, reactive power per bus

RATE_HZ          = 5     # packets per second
DURATION_S       = 60    # how long to send for
TOTAL_PACKETS    = RATE_HZ * DURATION_S
SEND_INTERVAL_S  = 1.0 / RATE_HZ

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

print(f"Sending {TOTAL_PACKETS} packets at {RATE_HZ}/s for {DURATION_S}s")
print(f"Buses: {BUS_COUNT}  |  Fields per bus: voltage, active power, reactive power")
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
    # Voltage step: 0.1 pu  |  Power step: 150 MW
    cycle_pos = i % 10
    if cycle_pos <= 5:
        v_swing = -cycle_pos * 0.1
        p_swing = -cycle_pos * 150.0
    else:
        v_swing = -(10 - cycle_pos) * 0.1
        p_swing = -(10 - cycle_pos) * 150.0

    values = [] 
    for b in range(BUS_COUNT):
        voltage        = 1.0    + (b * 0.002) + v_swing
        active_power   = 1900.0 + (b * 50)    + p_swing
        reactive_power = 200.0  + (b * 10)    + p_swing * 0.1  # MVAR tracks power loosely
        values.extend([voltage, active_power, reactive_power])

    payload = struct.pack(STRUCT_FMT, *values)
    sock.sendto(payload, (UDP_HOST, UDP_PORT))

    if (i + 1) % RATE_HZ == 0:
        elapsed = time.monotonic() - start
        print(f"  t={elapsed:5.2f}s  sent {i + 1}/{TOTAL_PACKETS}")

sock.close()
print(f"\nDone. Check DynamoDB for ~{DURATION_S // 5} new records.")
print("Each record should have 22 devices with 'voltage', 'active power', and 'reactive power' fields.")