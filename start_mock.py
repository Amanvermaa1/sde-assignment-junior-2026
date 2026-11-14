"""Start the mock telephony provider."""
import subprocess
import sys
import os

# Change to the assignment-b-workflow directory
workdir = "F:/sde-assignment-junior-2026/assignment-b-workflow"

# Start the mock provider
proc = subprocess.Popen(
    [sys.executable, "-m", "mock_telephony", "--failure-scale", "0"],
    cwd=workdir,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
)

print(f"Mock provider started with PID: {proc.pid}")
print("Waiting for it to be ready...")

# Wait a bit for it to start
import time
time.sleep(3)

# Check if it's still running
if proc.poll() is None:
    print("Mock provider is running!")
    # Print the URL info from the output
    # The mock prints: "Mock telephony on http://127.0.0.1:8099, docs at /docs"
    # Let's try to access it
    import httpx
    try:
        resp = httpx.get("http://127.0.0.1:8099/v1/clock", timeout=5.0)
        print(f"Clock response: {resp.json()}")
    except Exception as e:
        print(f"Could not connect: {e}")
        proc.terminate()
else:
    print(f"Mock provider exited with code: {proc.returncode}")
    stdout, stderr = proc.communicate()
    print(f"stdout: {stdout}")
    print(f"stderr: {stderr}")