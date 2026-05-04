# Simulation Data Bridge Server

A small Python server that bridges Speedgoat simulation output to an HTTP GET
endpoint.

```
Speedgoat  --UDP-->  This Server (Host PC)  --HTTP GET-->  Client
```

The server listens for UDP packets from the Speedgoat simulation, caches the
most recent payload, and serves it through a simple HTTP GET endpoint.

## Requirements

- Python 3.10+ (uses `dict | None` syntax)
- No third-party dependencies — only the Python standard library

## Running

```bash
python simulation_server.py
```

By default:
- UDP listener: `0.0.0.0:5005`
- HTTP server:  `0.0.0.0:8000`

Adjust the constants at the top of `simulation_server.py` if those ports
conflict with anything on the host.

## Endpoints

| Method | Path        | Description                                  |
|--------|-------------|----------------------------------------------|
| GET    | `/data`     | Returns the latest payload received via UDP  |
| GET    | `/latest`   | Alias for `/data`                            |
| GET    | `/history`  | Returns up to the last 100 payloads          |
| GET    | `/health`   | Server status + packet counts                |

## Expected UDP Payload

The server expects JSON-encoded UDP packets matching the agreed-upon schema:

```json
{
  "timestamp": "2025-05-04T12:34:56Z",
  "devices": [
    { "id": "bus01", "voltage": 380.2, "power": 1939 },
    { "id": "bus02", "voltage": 379.8, "power": 1861 }
  ]
}
```

If `timestamp` is omitted, the server stamps the packet on receipt.

## Quick Test

In one terminal:

```bash
python simulation_server.py
```

In another:

```bash
python test_sender.py
curl http://localhost:8000/data
```

## Sending from MATLAB / Speedgoat

From the host PC side, a UDP send block (or `udpport` + `write` in MATLAB)
configured for `127.0.0.1:5005` with a JSON-encoded payload will work. The
JSON construction in MATLAB is the same one already prototyped for the MIDAAS
API call — just `jsonencode(payload)` and ship the bytes over UDP instead of
`webwrite`.
