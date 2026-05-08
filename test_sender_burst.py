"""
High-rate UDP test sender to verify the forwarder's 5-second throttle.
 
Sends 5 packets per second for 20 seconds (100 packets total). With the
forwarder set to POST every 5 seconds, you should see exactly 4 records
written to DynamoDB at the end of this run.
 
Each packet's voltage values are linearly increasing across the run, so you
can also verify the forwarder is sending the *latest* packet each tick
(not, e.g., the first one received in the window).
 
Usage:
    python test_sender_burst.py
"""
 
import socket
import struct
import time
 
UDP_HOST = "127.0.0.1"
UDP_PORT = 5005
 
RATE_HZ          = 5     # packets per second
DURATION_S       = 60    # how long to send for
TOTAL_PACKETS    = RATE_HZ * DURATION_S
SEND_INTERVAL_S  = 1.0 / RATE_HZ
 
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
 
print(f"Sending {TOTAL_PACKETS} packets at {RATE_HZ}/s for {DURATION_S}s")
print(f"Forwarder should POST ~{DURATION_S // 5} times "
      f"(once every 5s, sending the latest packet seen)\n")
 
start = time.monotonic()
next_send = start
 
for i in range(TOTAL_PACKETS):
    sleep_for = next_send - time.monotonic()
    if sleep_for > 0:
        time.sleep(sleep_for)
    next_send += SEND_INTERVAL_S
 
    v1 = 380.0 + (i * 500) * 0.05
    p1 = float(1939 + i)
    v2 = 380.0 + (i * 500) * 0.05
    p2 = float(1861 - i)
    payload = struct.pack('<dddd', v1, p1, v2, p2)
    sock.sendto(payload, (UDP_HOST, UDP_PORT))
    if (i + 1) % RATE_HZ == 0:
        elapsed = time.monotonic() - start
        print(f"  t={elapsed:5.2f}s  sent {i + 1}/{TOTAL_PACKETS}")
 
sock.close()
print(f"\nDone. Check DynamoDB for ~{DURATION_S // 5} new records.")
print("Their packetNumber values should be near 25, 50, 75, 100 "
      "(the last packet seen before each tick).")