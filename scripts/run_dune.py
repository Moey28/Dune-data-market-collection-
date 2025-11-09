import os, time, requests, pathlib, sys

API = "https://api.dune.com/api/v1"
KEY = os.environ.get("DUNE_API_KEY")
SQL = os.environ.get("DUNE_SQL")

if not KEY:
    print("Missing DUNE_API_KEY secret", file=sys.stderr); sys.exit(1)
if not SQL:
    print("Missing DUNE_SQL variable", file=sys.stderr); sys.exit(1)

headers = {"X-Dune-API-Key": KEY, "Content-Type": "application/json"}

# 1) Execute raw SQL (no saved query needed)
resp = requests.post(f"{API}/sql/execute", headers=headers, json={"sql": SQL})
resp.raise_for_status()
exec_id = resp.json()["execution_id"]
print("Execution ID:", exec_id)

# 2) Poll until complete
while True:
    s = requests.get(f"{API}/execution/{exec_id}/status", headers=headers)
    s.raise_for_status()
    state = s.json().get("state")
    print("Status:", state)
    if state in ("QUERY_STATE_COMPLETED", "QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
        break
    time.sleep(2)

if state != "QUERY_STATE_COMPLETED":
    print("Execution did not complete successfully:", state, file=sys.stderr)
    sys.exit(2)

# 3) Fetch results as CSV
res = requests.get(f"{API}/execution/{exec_id}/results/csv", headers=headers)
res.raise_for_status()

# 4) Save to /data with timestamp
pathlib.Path("data").mkdir(parents=True, exist_ok=True)
outfile = pathlib.Path("data") / f"dune_export_{int(time.time())}.csv"
outfile.write_bytes(res.content)
print("Saved:", outfile)
