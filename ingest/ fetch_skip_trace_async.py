#!/usr/bin/env python3
import os, sys, time, argparse
import requests, psycopg2
from sqlalchemy import create_engine, text

API_BASE = "https://api.batchdata.com/v1"
API_KEY  = os.getenv("BATCHDATA_API_KEY")
DB_URL   = os.getenv("DATABASE_URL")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

def get_addresses_to_trace(limit=100):
    engine = create_engine(DB_URL)
    sql = """
    SELECT id, address
      FROM addresses a
 LEFT JOIN skip_traces s ON s.address_id = a.id
     WHERE s.address_id IS NULL
     LIMIT :limit
    """
    return engine.execute(text(sql), {"limit": limit}).fetchall()

def kick_off_job(address_id, address_str):
    payload = {"address": address_str}
    resp = requests.post(f"{API_BASE}/skipTrace/async", json=payload, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()["jobId"]

def poll_job(job_id, timeout=300, interval=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = requests.get(f"{API_BASE}/skipTrace/async/{job_id}", headers=HEADERS)
        r.raise_for_status()
        j = r.json()
        if j["status"] in ("complete","failed"):
            return j
        time.sleep(interval)
    raise RuntimeError("Timed out waiting for skip trace job")

def save_result(address_id, job_id, job_payload):
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        conn.execute(text(
            """
            INSERT INTO skip_traces(address_id, batch_job_id, status, result, completed_at)
            VALUES(:addr, :job, :stat, :res::jsonb, NOW())
            """
        ), {
            "addr": address_id,
            "job":  job_id,
            "stat": job_payload["status"],
            "res":  json.dumps(job_payload)
        })

def main(batch_size):
    to_do = get_addresses_to_trace(batch_size)
    if not to_do:
        print("No new addresses to skip-trace.")
        return

    for addr_id, addr in to_do:
        print(f"[{addr_id}] → tracing “{addr}”")
        job = kick_off_job(addr_id, addr)
        print("  job:", job)
        result = poll_job(job)
        print("  done:", result["status"])
        save_result(addr_id, job, result)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--batch", type=int, default=50)
    args = p.parse_args()
    main(args.batch)