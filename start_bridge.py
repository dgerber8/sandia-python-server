"""
start_bridge.py
================

Cross-platform launcher (Windows, macOS, Linux) for the simulation data
bridge. Starts the Python server and a Cloudflare quick tunnel, captures
the public trycloudflare.com URL, and prints it.

Usage:
    python start_bridge.py

Requirements:
    - Python 3.8+
    - cloudflared on PATH
        Windows:  winget install --id Cloudflare.cloudflared
        macOS:    brew install cloudflared
        Linux:    https://pkg.cloudflare.com/  (or download the binary)
    - simulation_server.py in the same directory as this script

Press Ctrl+C to stop both the server and the tunnel cleanly.
"""

import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

# -------- Configuration --------
SERVER_SCRIPT = "simulation_server.py"
LOCAL_PORT    = 8000
URL_TIMEOUT_S = 30  # how long to wait for the trycloudflare URL to appear

URL_RE = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")


def fail(msg: str) -> None:
    print(f"\n[ERROR] {msg}", file=sys.stderr)
    sys.exit(1)


def stream_reader(proc: subprocess.Popen, buffer: list, url_holder: list) -> None:
    """Read cloudflared output line by line, save it, and watch for the URL."""
    assert proc.stdout is not None
    for raw in proc.stdout:
        line = raw.decode("utf-8", errors="replace").rstrip()
        buffer.append(line)
        if not url_holder:
            m = URL_RE.search(line)
            if m:
                url_holder.append(m.group(0))


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    server_path = script_dir / SERVER_SCRIPT

    # ---- sanity checks ----
    if not server_path.exists():
        fail(f"{SERVER_SCRIPT} not found next to this script ({server_path}).")

    if not shutil.which("cloudflared"):
        fail(
            "cloudflared not found on PATH.\n"
            "  Windows: winget install --id Cloudflare.cloudflared\n"
            "  macOS:   brew install cloudflared\n"
            "  Linux:   https://pkg.cloudflare.com/"
        )

    # ---- start Python server ----
    print(f"Starting Python server on port {LOCAL_PORT}...")
    server_proc = subprocess.Popen(
        [sys.executable, str(server_path)],
        cwd=str(script_dir),
    )

    # Brief pause so the server binds before cloudflared connects
    time.sleep(2)

    if server_proc.poll() is not None:
        fail(f"Server exited immediately (code {server_proc.returncode}).")

    # ---- start cloudflared ----
    print("Starting Cloudflare quick tunnel...")
    tunnel_proc = subprocess.Popen(
        ["cloudflared", "tunnel", "--url", f"http://localhost:{LOCAL_PORT}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # cloudflared logs to stderr; merge them
    )

    log_lines: list = []
    url_holder: list = []
    reader = threading.Thread(
        target=stream_reader,
        args=(tunnel_proc, log_lines, url_holder),
        daemon=True,
    )
    reader.start()

    # ---- wait for the URL ----
    print("Waiting for tunnel URL...")
    deadline = time.time() + URL_TIMEOUT_S
    while time.time() < deadline and not url_holder:
        if tunnel_proc.poll() is not None:
            print("\n--- cloudflared output ---")
            print("\n".join(log_lines))
            fail(f"cloudflared exited (code {tunnel_proc.returncode}).")
        time.sleep(0.25)

    print()
    print("=" * 60)
    if url_holder:
        public_url = url_holder[0]
        print(" Public URL:")
        print(f"   {public_url}")
    else:
        print(" Could not detect tunnel URL within "
              f"{URL_TIMEOUT_S} seconds.")
        print(" Recent cloudflared output:")
        for line in log_lines[-15:]:
            print(f"   {line}")
    print("=" * 60)
    print()
    print("Press Ctrl+C to stop both processes.")
    print()

    # ---- supervise ----
    def shutdown(*_):
        print("\nShutting down...")
        for p in (tunnel_proc, server_proc):
            if p.poll() is None:
                try:
                    if sys.platform.startswith("win"):
                        p.terminate()
                    else:
                        p.send_signal(signal.SIGINT)
                except Exception:
                    pass
        # Give them a moment, then force-kill anything left
        deadline = time.time() + 5
        for p in (tunnel_proc, server_proc):
            while p.poll() is None and time.time() < deadline:
                time.sleep(0.1)
            if p.poll() is None:
                try:
                    p.kill()
                except Exception:
                    pass
        print("Done.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, shutdown)

    try:
        while True:
            if server_proc.poll() is not None:
                print(f"\n[!] Python server exited (code {server_proc.returncode}).")
                shutdown()
            if tunnel_proc.poll() is not None:
                print(f"\n[!] cloudflared exited (code {tunnel_proc.returncode}).")
                shutdown()
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()