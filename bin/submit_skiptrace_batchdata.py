#!/usr/bin/env python3
import os
import sys
import json
import argparse
import requests
from sqlalchemy import create_engine, text

def die(msg, code=2):
    print(msg, file=sys.stderr)
    sys.exit(code)

def get_db_engine():
    dburl = os.getenv("DATABASE_URL")
    if not dburl:
        die("DATABASE_URL not set")
    return create_engine(dburl)

def fetch_run(conn, run_id: int):
    row = conn.execute(
        text("""
            SELECT run_id, output_path, state, center_lon, center_lat, radius_km, dist_m, target
            FROM public.skiptrace_runs
            WHERE run_id = :rid
        """),
        {"rid": run_id}
    ).mappings().first()
    if not row:
        die(f"No skiptrace_runs row for run_id={run_id}", 1)
    return row

def ensure_columns(conn):
    # Add columns if missing so we can record job metadata
    conn.execute(text("""
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='skiptrace_runs' AND column_name='batch_job_id'
      ) THEN
        ALTER TABLE public.skiptrace_runs ADD COLUMN batch_job_id text;
      END IF;
      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='skiptrace_runs' AND column_name='webhook_url'
      ) THEN
        ALTER TABLE public.skiptrace_runs ADD COLUMN webhook_url text;
      END IF;
      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='skiptrace_runs' AND column_name='submitted_at'
      ) THEN
        ALTER TABLE public.skiptrace_runs ADD COLUMN submitted_at timestamptz;
      END IF;
      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='skiptrace_runs' AND column_name='api_base'
      ) THEN
        ALTER TABLE public.skiptrace_runs ADD COLUMN api_base text;
      END IF;
    END $$;
    """))

def main():
    p = argparse.ArgumentParser(description="Submit a skiptrace CSV to BatchData.")
    p.add_argument("--run-id", type=int, required=True, help="Row in skiptrace_runs to submit")
    p.add_argument("--api-key", default=os.getenv("BATCHDATA_API_KEY"), help="BatchData API key (or set BATCHDATA_API_KEY)")
    p.add_argument("--base-url", default=os.getenv("BATCHDATA_BASE_URL", "https://api.batchdata.com/api/v1"),
                   help="BatchData API base URL (default: https://api.batchdata.com/api/v1)")
    p.add_argument("--webhook-url", default=os.getenv("BATCHDATA_WEBHOOK_URL"),
                   help="Webhook URL to receive async results (or set BATCHDATA_WEBHOOK_URL)")
    p.add_argument("--use-dummy-webhook", action="store_true",
                   help="Use https://httpbin.org/post as a temporary webhook (results will NOT be delivered to you)")
    p.add_argument("--list-name", default=None, help="Optional job/list label")
    p.add_argument("--source", default="storm-leads", help="Optional source tag (default: storm-leads)")
    args = p.parse_args()

    if not args.api_key:
        die("Missing API key. Set BATCHDATA_API_KEY or pass --api-key")

    # Webhook handling
    webhook = args.webhook_url
    if not webhook and args.use_dummy_webhook:
        webhook = "https://httpbin.org/post"
        print("⚠️  Using dummy webhook https://httpbin.org/post — results will be discarded.", file=sys.stderr)
    if not webhook:
        die("The API requires a webhook. Pass --webhook-url or set BATCHDATA_WEBHOOK_URL. "
            "For testing: add --use-dummy-webhook (results will not be delivered).")

    engine = get_db_engine()
    with engine.begin() as conn:
        run = fetch_run(conn, args.run_id)
        ensure_columns(conn)

    csv_path = run["output_path"]
    if not csv_path or not os.path.isfile(csv_path):
        die(f"CSV not found: {csv_path}", 1)

    url = args.base_url.rstrip("/") + "/property/skip-trace/async"
    print(f"→ Submitting to {url}")

    options = {"webhook": webhook}
    # Add a friendly name if provided (helps on vendor dashboard)
    if args.list_name:
        options["listName"] = args.list_name
    # Tag the source if helpful
    if args.source:
        options["source"] = args.source

    headers = {
        "Authorization": f"Bearer {args.api_key}",
        "x-api-key": args.api_key,
        "Accept": "application/json",
    }

    # Multipart form: file + options (as JSON string)
    with open(csv_path, "rb") as f:
        files = {"file": (os.path.basename(csv_path), f, "text/csv")}
        data = {"options": json.dumps(options)}
        try:
            r = requests.post(url, headers=headers, files=files, data=data, timeout=180)
        except requests.RequestException as e:
            die(f"Request failed: {e}", 1)

    ct = r.headers.get("content-type", "")
    print(f"HTTP {r.status_code}")
    resp_json = None
    if "application/json" in ct.lower():
        try:
            resp_json = r.json()
            print("Response JSON:", json.dumps(resp_json, indent=2))
        except Exception:
            print("Response text (non-JSON or parse error):", r.text[:2000])
    else:
        print("Response text:", r.text[:2000])

    # If non-2xx, raise to stop here
    try:
        r.raise_for_status()
    except requests.HTTPError:
        sys.exit(1)

    # Try to pull a job id from the response if present; common keys: id, jobId, job_id
    job_id = None
    if isinstance(resp_json, dict):
        for k in ("id", "jobId", "job_id", "data"):
            if k in resp_json and resp_json[k]:
                if isinstance(resp_json[k], (str, int)):
                    job_id = str(resp_json[k])
                    break
                if isinstance(resp_json[k], dict):
                    # sometimes inside data: { jobId: ... }
                    for kk in ("id", "jobId", "job_id"):
                        if kk in resp_json[k]:
                            job_id = str(resp_json[k][kk])
                            break
                if job_id:
                    break

    # Record job id + webhook in DB for traceability
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE public.skiptrace_runs
                SET batch_job_id = COALESCE(:job_id, batch_job_id),
                    webhook_url  = COALESCE(:webhook, webhook_url),
                    submitted_at = COALESCE(submitted_at, now()),
                    api_base     = COALESCE(:api_base, api_base)
                WHERE run_id = :rid
            """),
            {"job_id": job_id, "webhook": webhook, "api_base": args.base_url, "rid": args.run_id}
        )

    if job_id:
        print(f"✅ Submitted. Vendor job id: {job_id}")
    else:
        print("✅ Submitted. (No job id found in response; recorded webhook & base URL.)")

if __name__ == "__main__":
    main()