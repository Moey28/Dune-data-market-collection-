import os
import time
import json
import requests
import pathlib
import sys

API_BASE = "https://api.dune.com/api/v1"
KEY = os.environ.get("DUNE_API_KEY")
QID = os.environ.get("DUNE_QUERY_ID")

# Validate environment variables
if not KEY:
    print("Missing DUNE_API_KEY secret", file=sys.stderr)
    sys.exit(1)
if not QID:
    print("Missing DUNE_QUERY_ID variable", file=sys.stderr)
    sys.exit(1)

headers = {
    "X-Dune-API-Key": KEY,
    "Content-Type": "application/json",
}

def execute_query(query_id: str) -> str:
    """Execute a stored Dune query and return the execution ID."""
    response = requests.post(f"{API_BASE}/query/{query_id}/execute", headers=headers, json={})
    response.raise_for_status()
    data = response.json()
    return data["execution_id"]


def wait_for_completion(execution_id: str, timeout: int = 120) -> str:
    """Poll the execution status until completion or timeout."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        status_resp = requests.get(f"{API_BASE}/execution/{execution_id}/status", headers=headers)
        status_resp.raise_for_status()
        status = status_resp.json()
        state = status.get("state")
        print(f"Status: {state}")
        if state == "QUERY_STATE_COMPLETED":
            return state
        if state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
            raise RuntimeError(f"Query execution failed with state: {state}")
        time.sleep(2)
    raise TimeoutError("Query execution timed out")


def download_csv(execution_id: str) -> bytes:
    """Download results of the execution as CSV and return bytes."""
    response = requests.get(
        f"{API_BASE}/execution/{execution_id}/results/csv", headers=headers
    )
    response.raise_for_status()
    return response.content


def save_csv(data: bytes) -> pathlib.Path:
    """Save the CSV data to the `data` directory with a timestamped filename."""
    output_dir = pathlib.Path("data")
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = int(time.time())
    file_path = output_dir / f"soccer_polymarket_{timestamp}.csv"
    file_path.write_bytes(data)
    return file_path


def main():
    print(f"Executing Dune query {QID}...")
    exec_id = execute_query(QID)
    print(f"Execution ID: {exec_id}")
    wait_for_completion(exec_id)
    csv_bytes = download_csv(exec_id)
    out_file = save_csv(csv_bytes)
    print(f"Saved CSV to {out_file}")


if __name__ == "__main__":
    main()
