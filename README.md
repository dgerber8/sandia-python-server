# Simulation Data Forwarder

Bridges Speedgoat simulation output to an AWS API Gateway endpoint over HTTPS.

```
Sim Computer  --UDP:5005 (first half  of buses + PVs)--+
                                                         +--> simulation_forwarder.py --> API Gateway
Sim Computer  --UDP:5006 (second half of buses + PVs)--+
```

The forwarder listens on two UDP ports, each receiving a combined bus + PV
packet for its half of the network. Every 5 seconds it merges the latest
snapshot from each port into one JSON payload and POSTs it to the API Gateway.

## Requirements

- Python 3.10+ (uses `dict | None` syntax)
- No third-party dependencies — standard library only

## Files

| File | Purpose |
|------|---------|
| `simulation_forwarder.py` | Main forwarder — receives UDP, merges, POSTs to API |
| `multi_port_test_sender.py` | Test sender that emits to both ports simultaneously |
| `test_sender_burst.py` | Single-port high-rate sender for throttle testing |
| `test_sender.py` | Simple one-shot test sender |

## Running

```bash
python simulation_forwarder.py
```

Default ports: `0.0.0.0:5005` and `0.0.0.0:5006`. Adjust `PORT_A` / `PORT_B`
at the top of the script if those conflict with anything on the host.

The API endpoint is set via `API_URL` in the script, or the
`FORWARDER_API_URL` environment variable.

## UDP Packet Format

Both ports use the same self-describing binary format (little-endian throughout).
Each bus and PV entry embeds its own ID string, so the forwarder requires no
hardcoded lookup tables or offset configuration.

**Header — 16 bytes**
```
<dII    timestamp_epoch_s (float64), bus_count (uint32), pv_count (uint32)
```

**Per bus entry**
```
<H             id_len            byte length of the ID string
[id_len bytes] UTF-8 string      bus ID, e.g. "bus01"
<dddddd        float64 × 6      voltage, active P, reactive P, face0, face1, face2
```

**Per PV entry**
```
<H             id_len            byte length of the name string
[id_len bytes] UTF-8 string      PV name, e.g. "PVSystem.PVSY319"
<dd            float64 × 2      active P, reactive P
```

All bus entries appear first in the packet body, followed by all PV entries,
consistent with the counts in the header.

## POST Payload

The forwarder merges the two port snapshots and POSTs a single JSON body:

```json
{
  "timestamp": "2025-05-04T12:34:56Z",
  "devices": [
    { "id": "bus01", "voltage": 1.002, "active power": 1900.0, "reactive power": 200.0, "faces": [1.032, 0.977, 1.057] },
    { "id": "bus02", "voltage": 1.004, "active power": 1950.0, "reactive power": 210.0, "faces": [1.034, 0.979, 1.059] }
  ],
  "PVSystems": [
    { "id": "PVSystem.PVSY19",  "active power": 500.0, "reactive power": 50.0 },
    { "id": "PVSystem.PVSY35",  "active power": 508.0, "reactive power": 51.0 }
  ]
}
```

If only one port has data in a given 5-second window, the payload contains
only that port's buses and PVs. If neither has data, nothing is sent.

## Testing

**Multi-port test** — exercises both ports and the merge logic:
```bash
python multi_port_test_sender.py
```
Sends 5 packet pairs/sec for 600 s to both ports. Expect ~120 DynamoDB records,
each containing all 22 buses and 35 PVSystems.

**Single-port throttle test** — verifies the 5-second POST interval:
```bash
python test_sender_burst.py
```
Sends 5 packets/sec to port 5005 only. Expect ~120 records with data from
port A only.

## Adding Buses, PVs, or Ports

Because IDs and names are embedded in each packet, topology changes only
require sender-side updates:

- **Add a bus or PV**: append to `BUS_IDS` / `PV_NAMES` in the sender and
  adjust the port split slice. No forwarder changes needed.
- **Add a port**: start a new sender targeting the new port, and add one
  `udp_receiver` thread call in `simulation_forwarder.py`'s `main()`.

## Sending from Speedgoat / MATLAB

Configure a UDP Send block (or `udpport` + `write`) targeting
`<host-pc-ip>:5005` and `<host-pc-ip>:5006` with packets matching the binary
format above. The host PC runs `simulation_forwarder.py`; the Speedgoat IP
does not need to be configured anywhere in the forwarder.
