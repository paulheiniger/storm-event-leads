#!/usr/bin/env python3
"""
enrich_and_filter_properties.py

Batch-enrich addresses with BatchData "All Attributes" API, then filter:
  - ownerOccupied == True
  - vacant == False
  - value <= 1,000,000  (value = totalMarketValue if present, else totalAssessedValue)

Finally, select the top N (default: 1000) by value (descending) and write:
  1) a full CSV with diagnostics,
  2) a vendor-ready CSV (street, city, state, zip) for skiptrace.

Requirements:
  - env BATCHDATA_API_KEY must be set (or pass --api-key)
  - input CSV should include columns: street, city, state, zip
  - Python deps: requests, pandas (pip install requests pandas)

Usage example:
  python bin/enrich_and_filter_properties.py \
    --in-csv exports/skiptrace_KY_20250810-111031_40km_200m.csv \
    --out-prefix exports/ky_louisville_filtered \
    --target 1000
"""

import os
import sys
import time
import json
import math
import argparse
import base64
from typing import List, Dict, Any

import requests
import pandas as pd


API_URL = "https://api.batchdata.com/api/v1/property/lookup/all-attributes"


def die(msg: str, code: int = 2):
    print(msg, file=sys.stderr)
    sys.exit(code)


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]


def pick_value(p: Dict[str, Any]) -> float:
    """Choose market value if present else assessed value, else NaN."""
    assess = (p.get("assessment") or {})
    mv = assess.get("totalMarketValue")
    av = assess.get("totalAssessedValue")
    if isinstance(mv, (int, float)):
        return float(mv)
    if isinstance(av, (int, float)):
        return float(av)
    return float("nan")


def get_bool(d: Dict[str, Any], path: List[str]) -> Any:
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def batch_call(api_key: str, rows: List[Dict[str, Any]], skip_trace: bool = True, retries: int = 3, backoff: float = 1.5):
    """
    Call BatchData All Attributes for a batch of rows (each row has street/city/state/zip).
    Returns list of property dicts (may be empty for no matches).
    """
    payload = {
        "requests": [
            {
                "address": {
                    "street": r["street"],
                    "city": r["city"],
                    "state": r["state"],
                    "zip": str(r["zip"]) if pd.notna(r["zip"]) else ""
                }
            }
            for r in rows
        ],
        "options": {
            "skipTrace": bool(skip_trace)
        }
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.post(API_URL, headers=headers, json=payload, timeout=60)
        except requests.RequestException as e:
            if attempt <= retries:
                sleep = backoff ** attempt
                print(f"Network error ({e}); retrying in {sleep:.1f}s …")
                time.sleep(sleep)
                continue
            die(f"Request failed after {retries} retries: {e}", 1)

        # Respect rate limits if provided
        if resp.status_code == 429 and attempt <= retries:
            retry_after = float(resp.headers.get("Retry-After", "1"))
            sleep = max(retry_after, backoff ** attempt)
            print(f"Rate-limited; retrying in {sleep:.1f}s …")
            time.sleep(sleep)
            continue

        # Print helpful error body if non-2xx
        if not (200 <= resp.status_code < 300):
            try:
                body = resp.json()
                pretty = json.dumps(body, indent=2)
            except Exception:
                pretty = resp.text
            die(f"HTTP {resp.status_code}\n{pretty}", 1)

        data = resp.json()
        props = (data.get("results") or {}).get("properties") or []
        return props


def main():
    ap = argparse.ArgumentParser(description="Enrich addresses with BatchData, filter, and export top N candidates.")
    ap.add_argument("--in-csv", required=True, help="Input CSV with columns street,city,state,zip (your export).")
    ap.add_argument("--out-prefix", required=False, default=None,
                    help="Output prefix for CSVs. Defaults to input name without extension.")
    ap.add_argument("--api-key", default=os.getenv("BATCHDATA_API_KEY"), help="BatchData API key (or set env).")
    ap.add_argument("--batch-size", type=int, default=50, help="Requests per API call (default: 50).")
    ap.add_argument("--target", type=int, default=1000, help="Top N to keep after filtering (default: 1000).")
    ap.add_argument("--max-value", type=float, default=1_000_000.0, help="Max value cap (default: 1,000,000).")
    ap.add_argument("--no-skiptrace", action="store_true", help="Do NOT request skipTrace (default: skipTrace enabled).")
    args = ap.parse_args()

    if not args.api_key:
        die("Missing API key: set BATCHDATA_API_KEY or pass --api-key")

    if not os.path.isfile(args.in_csv):
        die(f"Input CSV not found: {args.in_csv}")

    # Out paths
    if not args.out_prefix:
        base, _ = os.path.splitext(args.in_csv)
        args.out_prefix = base + "_filtered"

    out_full = f"{args.out_prefix}_full.csv"
    out_vendor = f"{args.out_prefix}_vendor.csv"

    # Load input and sanity-check schema
    df = pd.read_csv(args.in_csv)
    needed = ["street", "city", "state", "zip"]
    for col in needed:
        if col not in df.columns:
            die(f"Input is missing required column: {col}")

    # Basic cleanup: drop rows missing any of the 4 parts
    before = len(df)
    df = df.dropna(subset=needed)
    df = df[(df["street"].astype(str).str.strip() != "")
            & (df["city"].astype(str).str.strip() != "")
            & (df["state"].astype(str).str.strip() != "")
            & (df["zip"].astype(str).str.strip() != "")]
    after = len(df)
    if after == 0:
        die("No usable rows after cleaning (need street/city/state/zip).")
    print(f"Loaded {before} rows; {after} rows are usable.")

    # Enrich in batches
    all_props: List[Dict[str, Any]] = []
    rows = df[needed].to_dict(orient="records")

    total_batches = math.ceil(len(rows) / args.batch_size)
    for bi, batch in enumerate(chunked(rows, args.batch_size), start=1):
        print(f"→ Batch {bi}/{total_batches} … ({len(batch)} addrs)")
        props = batch_call(
            api_key=args.api_key,
            rows=batch,
            skip_trace=(not args.no_skiptrace),
        )
        all_props.extend(props)

    if not all_props:
        die("API returned no properties.", 1)

    # Flatten results we care about
    flat = []
    for p in all_props:
        addr = p.get("address") or {}
        gen = p.get("general") or {}
        owner = p.get("owner") or {}
        row = {
            "street": addr.get("streetNoUnit") or addr.get("formattedStreet") or addr.get("street"),
            "city": addr.get("city"),
            "state": addr.get("state"),
            "zip": addr.get("zip"),
            "ownerOccupied": owner.get("ownerOccupied"),
            "vacant": gen.get("vacant"),
            "totalMarketValue": (p.get("assessment") or {}).get("totalMarketValue"),
            "totalAssessedValue": (p.get("assessment") or {}).get("totalAssessedValue"),
        }
        row["valueUsed"] = pick_value(p)
        flat.append(row)

    gf = pd.DataFrame(flat)
    # Filter
    gf = gf[
        (gf["ownerOccupied"] == True) &
        (gf["vacant"] == False) &
        (gf["valueUsed"].fillna(float("inf")) <= float(args.max_value))
    ].copy()

    if gf.empty:
        die("After filtering (ownerOccupied=true, vacant=false, value<=max), no rows remained.", 1)

    # Sort by value desc and cap to target
    gf.sort_values(["valueUsed"], ascending=[False], inplace=True)
    gf_top = gf.head(args.target).copy()

    # Write full CSV
    cols_full = ["street", "city", "state", "zip",
                 "ownerOccupied", "vacant",
                 "totalMarketValue", "totalAssessedValue", "valueUsed"]
    gf_top.to_csv(out_full, index=False, columns=cols_full)
    print(f"✅ Wrote full results: {out_full}  ({len(gf_top)} rows)")

    # Write vendor-ready CSV
    gf_top[["street", "city", "state", "zip"]].to_csv(out_vendor, index=False)
    print(f"✅ Wrote vendor CSV: {out_vendor}  ({len(gf_top)} rows)")

    # Helpful next step hint
    print("\nNext step idea:")
    print(f"  python bin/submit_skiptrace_batchdata.py --csv {out_vendor} "
          f"--webhook-url \"$BATCHDATA_WEBHOOK_URL\" --list-name \"Filtered Top {len(gf_top)}\" --source storm-leads")

if __name__ == "__main__":
    main()