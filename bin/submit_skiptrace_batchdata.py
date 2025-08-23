#!/usr/bin/env python3
import os
import sys
import json
import base64
import argparse
import requests
from sqlalchemy import create_engine, text
from pathlib import Path

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
      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='skiptrace_runs' AND column_name='last_error'
      ) THEN
        ALTER TABLE public.skiptrace_runs ADD COLUMN last_error jsonb;
      END IF;
    END $$;
    """))

def mask(s: str, left=4, right=4):
    if not s:
        return s
    if len(s) <= left + right:
        return "…" * len(s)
    return f"{s[:left]}…{s[-right:]}"

def debug_curl(url, headers, files=None, data=None, json_body=None):
    lines = ["curl \\","  -sS \\", f"  -X POST \\", f"  '{url}' \\"]
    for k, v in headers.items():
        if k.lower() == "authorization":
            v_disp = v
            # mask token portion after "Bearer "
            if v.lower().startswith("bearer "):
                tok = v.split(" ",1)[1]
                v_disp = "Bearer " + mask(tok)
        lines += [f"  -H '{k}: {v_disp}' \\"]
    if files:
        for field, tup in files.items():
            _, path, ctype = tup
            lines += [f"  -F '{field}=@{path};type={ctype}' \\"]
    if data:
        # print as -F key=value parts (only for multipart form fields)
        for k, v in data.items():
            if isinstance(v, (dict, list)):
                v = json.dumps(v)
            lines += [f"  -F '{k}={v}' \\"]
    if json_body is not None:
        lines += ["  -H 'Content-Type: application/json' \\", f"  -d '{json.dumps(json_body)}'"]
    print("   curl >", *lines, sep="\n")

def submit_multipart(url, api_key, csv_path, options, field_name, pattern, debug=False):
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    files = {
        field_name: (os.path.basename(csv_path), open(csv_path, "rb"), "text/csv")
    }
    # pattern variants for options
    if pattern == "json_part":
        data = {"options": json.dumps(options)}
    elif pattern == "dot_keys":
        data = {}
        # flatten like options.webhook or options.webhook.url depending on provided
        if isinstance(options.get("webhook"), dict):
            if "url" in options["webhook"]:
                data["options.webhook.url"] = options["webhook"]["url"]
        elif isinstance(options.get("webhook"), str):
            data["options.webhook"] = options["webhook"]
        if options.get("listName"): data["options.listName"] = options["listName"]
        if options.get("source"):   data["options.source"]   = options["source"]
    elif pattern == "bracket_keys":
        data = {}
        if isinstance(options.get("webhook"), dict):
            if "url" in options["webhook"]:
                data["options[webhook][url]"] = options["webhook"]["url"]
        elif isinstance(options.get("webhook"), str):
            data["options[webhook]"] = options["webhook"]
        if options.get("listName"): data["options[listName]"] = options["listName"]
        if options.get("source"):   data["options[source]"]   = options["source"]
    else:
        raise ValueError("unknown pattern")

    if debug:
        # build a debug curl (don’t leak full token)
        curl_headers = dict(headers)
        debug_curl(url, curl_headers, files={field_name: (None, csv_path, "text/csv")}, data=data)

    try:
        r = requests.post(url, headers=headers, files=files, data=data, timeout=180)
    finally:
        try:
            files[field_name][1].close()
        except Exception:
            pass
    return r

def submit_json_base64(url, api_key, csv_path, options, nested_style, debug=False):
    """
    Submit as JSON with base64 data URL.
    nested_style: 'options_nested' -> {"options": {...}, "fileUrl":"data:text/csv;base64,..." }
                  'options_flat'   -> {"webhook":..., "listName":..., "fileUrl":...}
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    b64 = base64.b64encode(Path(csv_path).read_bytes()).decode("ascii")
    data_url = f"data:text/csv;base64,{b64}"

    if nested_style == "options_nested":
        payload = {"options": options, "fileUrl": data_url}
    elif nested_style == "options_flat":
        # flatten options into top-level
        payload = {"fileUrl": data_url}
        for k, v in options.items():
            payload[k] = v
    else:
        raise ValueError("bad nested_style")

    if debug:
        debug_curl(url, headers, json_body=payload)

    r = requests.post(url, headers=headers, json=payload, timeout=180)
    return r

def main():
    p = argparse.ArgumentParser(description="Submit a skiptrace CSV to BatchData.")
    # You can pass --run-id (to load CSV path from DB row) or --csv directly
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--run-id", type=int, help="Row in skiptrace_runs to submit")
    src.add_argument("--csv", help="Path to CSV to submit directly")

    p.add_argument("--api-key", default=os.getenv("BATCHDATA_API_KEY"),
                   help="BatchData API key (or set BATCHDATA_API_KEY)")
    p.add_argument("--base-url", default=os.getenv("BATCHDATA_BASE_URL", "https://api.batchdata.com/api/v1"),
                   help="BatchData API base URL (default: https://api.batchdata.com/api/v1)")
    p.add_argument("--webhook-url", default=os.getenv("BATCHDATA_WEBHOOK_URL"),
                   help="Webhook URL to receive async results (or set BATCHDATA_WEBHOOK_URL)")
    p.add_argument("--use-dummy-webhook", action="store_true",
                   help="Use https://httpbin.org/post as a temporary webhook (results will NOT be delivered to you)")
    p.add_argument("--list-name", default=None, help="Optional job/list label")
    p.add_argument("--source", default="storm-leads", help="Optional source tag (default: storm-leads)")
    p.add_argument("--no-subset", action="store_true", help="Skip any CSV subsetting before submit")
    p.add_argument("--debug", action="store_true", help="Print curl-equivalent and response headers")
    args = p.parse_args()

    if not args.api_key:
        die("Missing API key. Set BATCHDATA_API_KEY or pass --api-key")

    webhook = args.webhook_url
    if not webhook and args.use_dummy_webhook:
        webhook = "https://httpbin.org/post"
        print("⚠️  Using dummy webhook https://httpbin.org/post — results will be discarded.", file=sys.stderr)
    if not webhook:
        die("The API requires a webhook. Pass --webhook-url or set BATCHDATA_WEBHOOK_URL.")

    # Locate CSV
    if args.csv:
        csv_path = args.csv
        run_id = None
    else:
        engine = get_db_engine()
        with engine.begin() as conn:
            run = fetch_run(conn, args.run_id)
            ensure_columns(conn)
        csv_path = run["output_path"]
        run_id = args.run_id

    if not csv_path or not os.path.isfile(csv_path):
        die(f"CSV not found: {csv_path}", 1)

    url = args.base_url.rstrip("/") + "/property/skip-trace/async"
    print(f"→ Submitting to {url}")

    # Build options. Try both forms webhook:str and webhook:{url:...}
    # We’ll iterate through payload encodings below.
    options_variants = [
        {"webhook": webhook, "listName": args.list_name, "source": args.source},
        {"webhook": {"url": webhook}, "listName": args.list_name, "source": args.source},
    ]

    # Try multipart patterns first, then JSON base64 styles.
    attempts = []
    for opts in options_variants:
        for field in ("file", "csv", "upload"):
            attempts.append(("multipart", f"json_part:{field}", opts, field))
            attempts.append(("multipart", f"dot_keys:{field}", opts, field))
            attempts.append(("multipart", f"bracket_keys:{field}", opts, field))
        attempts.append(("json", "options_nested", opts, None))
        attempts.append(("json", "options_flat",   opts, None))

    last_err = None
    for mode, pattern, opts, field in attempts:
        try:
            if mode == "multipart":
                kind, fname = pattern.split(":")
                print(f"→ Trying multipart with '{fname}' and pattern {kind}:{fname}")
                r = submit_multipart(url, args.api_key, csv_path, opts, fname, kind, debug=args.debug)
            else:
                print(f"→ Trying JSON (pattern={pattern})")
                r = submit_json_base64(url, args.api_key, csv_path, opts, pattern, debug=args.debug)

            if args.debug:
                print(f"HTTP {r.status_code} ({pattern})")
                print("Response headers:", dict(r.headers))
            if "application/json" in (r.headers.get("content-type","").lower()):
                try:
                    body = r.json()
                    print("Response JSON:", json.dumps(body, indent=2))
                except Exception:
                    print("Response text:", r.text[:2000])
            else:
                print("Response text:", r.text[:2000])

            if 200 <= r.status_code < 300:
                # capture job id if present
                job_id = None
                try:
                    resp = r.json()
                    # common shapes: {status:{…}, data:{jobId:…}} OR {jobId:…}
                    for k in ("data","result","job","status"):
                        if isinstance(resp.get(k), dict):
                            for kk in ("jobId","id","job_id"):
                                if kk in resp[k]:
                                    job_id = str(resp[k][kk])
                                    break
                    if not job_id:
                        for kk in ("jobId","id","job_id"):
                            if kk in resp:
                                job_id = str(resp[kk])
                                break
                except Exception:
                    pass

                if run_id is not None:
                    engine = get_db_engine()
                    with engine.begin() as conn:
                        conn.execute(
                            text("""
                                UPDATE public.skiptrace_runs
                                SET batch_job_id = COALESCE(:job_id, batch_job_id),
                                    webhook_url  = COALESCE(:webhook, webhook_url),
                                    submitted_at = COALESCE(submitted_at, now()),
                                    api_base     = COALESCE(:api_base, api_base),
                                    last_error   = NULL
                                WHERE run_id = :rid
                            """),
                            {"job_id": job_id, "webhook": webhook, "api_base": args.base_url, "rid": run_id}
                        )
                if job_id:
                    print(f"✅ Submitted via pattern={pattern}. Vendor job id: {job_id}")
                else:
                    print(f"✅ Submitted via pattern={pattern}. (No job id found in response.)")
                return  # done

            # non-2xx
            # If webhook complaint persists, keep trying other shapes; otherwise raise
            msg = ""
            try:
                msg = r.json()
            except Exception:
                msg = r.text
            if isinstance(msg, dict):
                msg_s = json.dumps(msg)
            else:
                msg_s = str(msg)
            if "webhook" in msg_s.lower():
                last_err = requests.HTTPError(f"{r.status_code} {msg_s}", response=r)
                continue
            r.raise_for_status()

        except requests.HTTPError as e:
            last_err = e
            continue
        except Exception as e:
            last_err = e
            continue

    # If we get here, all patterns failed
    if run_id is not None:
        engine = get_db_engine()
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE public.skiptrace_runs
                    SET last_error = to_jsonb(:err::text)
                    WHERE run_id = :rid
                """),
                {"err": str(last_err), "rid": run_id}
            )
    die(f"Request failed: {last_err}", 1)

if __name__ == "__main__":
    main()