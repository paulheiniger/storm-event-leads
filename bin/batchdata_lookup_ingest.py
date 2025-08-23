#!/usr/bin/env python3
"""
batchdata_lookup_ingest.py

- Reads an addresses CSV (expects at least: street, city, state, zip)
- Calls BatchData "property/lookup/all-attributes" with options.skipTrace=true
- Stores results in Postgres:
    * batchdata_properties
    * batchdata_owner_contacts
- Exports a filtered CSV of <=$1M assessed, ownerOccupied=true, vacant=false
  capped to --top-n rows (default 1000), highest assessed first.

Env:
  DATABASE_URL           postgresql://… connection string
  BATCHDATA_API_KEY      BatchData API key (Bearer token)

Usage example:
  python bin/batchdata_lookup_ingest.py \
    --input-csv exports/skiptrace_KY_20250810-111031_40km_200m.csv \
    --export-csv exports/batchdata_targets_filtered.csv \
    --top-n 1000 --assessed-cap 1000000
"""
import os
import sys
import json
import time
import math
import argparse
import requests
import pandas as pd
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text

API_BASE = "https://api.batchdata.com/api/v1"

REQ_CHUNK = 50  # number of addresses per POST (safe default)

REQ_TEMPLATE = {
    "requests": [],
    "options": {"skipTrace": True}
}

def die(msg, code=2):
    print(msg, file=sys.stderr)
    sys.exit(code)

def need_cols(df, cols):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        die(f"Input CSV missing required columns: {missing}\n"
            f"Found columns: {list(df.columns)}")

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for c in df.columns:
        lc = c.strip().lower()
        if lc in ("street_address", "addr", "address1", "address"):
            rename_map[c] = "street"
        elif lc in ("city_name",):
            rename_map[c] = "city"
        elif lc in ("state_code", "region"):
            rename_map[c] = "state"
        elif lc in ("zipcode", "postal_code", "postcode", "zip_code"):
            rename_map[c] = "zip"
        else:
            # keep original if already correct name
            if lc in ("street", "city", "state", "zip"):
                rename_map[c] = lc
    if rename_map:
        df = df.rename(columns=rename_map)
    return df

def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def create_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS public.batchdata_properties (
          property_id           text PRIMARY KEY,
          street                text,
          city                  text,
          state                 text,
          zip                   text,
          zip_plus4             text,
          latitude              double precision,
          longitude             double precision,
          county                text,
          fips_code             text,
          total_assessed_value  integer,
          total_market_value    integer,
          year_built            integer,
          living_area_sqft      integer,
          owner_occupied        boolean,
          vacant                boolean,
          standardized_land_use text,
          property_type_detail  text,
          last_sold_date        timestamptz,
          last_sold_price       integer,
          created_at            timestamptz NOT NULL DEFAULT now(),
          updated_at            timestamptz NOT NULL DEFAULT now()
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS public.batchdata_owner_contacts (
          property_id   text NOT NULL,
          email         text,
          phone         text,
          phone_type    text,
          carrier       text,
          score         integer,
          dnc           boolean,
          reachable     boolean,
          last_reported timestamptz,
          PRIMARY KEY (property_id, email, phone)
        );
        """))
        # Helpful indexes
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_bd_props_owner_vacant
          ON public.batchdata_properties(owner_occupied, vacant);
        """))
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_bd_props_assessed
          ON public.batchdata_properties(total_assessed_value);
        """))

def upsert_property(conn, p: Dict[str, Any]):
    # Build an UPSERT statement
    sql = text("""
    INSERT INTO public.batchdata_properties
      (property_id, street, city, state, zip, zip_plus4,
       latitude, longitude, county, fips_code,
       total_assessed_value, total_market_value,
       year_built, living_area_sqft,
       owner_occupied, vacant,
       standardized_land_use, property_type_detail,
       last_sold_date, last_sold_price,
       created_at, updated_at)
    VALUES
      (:property_id, :street, :city, :state, :zip, :zip_plus4,
       :latitude, :longitude, :county, :fips_code,
       :total_assessed_value, :total_market_value,
       :year_built, :living_area_sqft,
       :owner_occupied, :vacant,
       :std_land_use, :ptype_detail,
       :last_sold_date, :last_sold_price,
       now(), now())
    ON CONFLICT (property_id) DO UPDATE SET
       street                = EXCLUDED.street,
       city                  = EXCLUDED.city,
       state                 = EXCLUDED.state,
       zip                   = EXCLUDED.zip,
       zip_plus4             = EXCLUDED.zip_plus4,
       latitude              = EXCLUDED.latitude,
       longitude             = EXCLUDED.longitude,
       county                = EXCLUDED.county,
       fips_code             = EXCLUDED.fips_code,
       total_assessed_value  = EXCLUDED.total_assessed_value,
       total_market_value    = EXCLUDED.total_market_value,
       year_built            = EXCLUDED.year_built,
       living_area_sqft      = EXCLUDED.living_area_sqft,
       owner_occupied        = EXCLUDED.owner_occupied,
       vacant                = EXCLUDED.vacant,
       standardized_land_use = EXCLUDED.standardized_land_use,
       property_type_detail  = EXCLUDED.property_type_detail,
       last_sold_date        = EXCLUDED.last_sold_date,
       last_sold_price       = EXCLUDED.last_sold_price,
       updated_at            = now();
    """)
    conn.execute(sql, p)

def insert_contacts(conn, property_id: str, emails: List[str], phones: List[Dict[str, Any]]):
    # Dedup emails
    for e in set([x for x in emails if x]):
        conn.execute(text("""
        INSERT INTO public.batchdata_owner_contacts
          (property_id, email, phone, phone_type, carrier, score, dnc, reachable, last_reported)
        VALUES
          (:pid, :email, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        ON CONFLICT (property_id, email, phone) DO NOTHING;
        """), {"pid": property_id, "email": e})

    # Phones
    for ph in phones or []:
        conn.execute(text("""
        INSERT INTO public.batchdata_owner_contacts
          (property_id, email, phone, phone_type, carrier, score, dnc, reachable, last_reported)
        VALUES
          (:pid, NULL, :phone, :ptype, :carrier, :score, :dnc, :reachable, :lastrep)
        ON CONFLICT (property_id, email, phone) DO NOTHING;
        """), {
            "pid": property_id,
            "phone": ph.get("number"),
            "ptype": ph.get("type"),
            "carrier": ph.get("carrier"),
            "score": ph.get("score"),
            "dnc": ph.get("dnc"),
            "reachable": ph.get("reachable"),
            "lastrep": ph.get("lastReportedDate")
        })

def pull_value(dct, *keys, default=None):
    cur = dct
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def payload_from_rows(rows: List[Dict[str, Any]]):
    reqs = []
    for r in rows:
        reqs.append({
            "address": {
                "street": str(r.get("street", "")).strip(),
                "city": str(r.get("city", "")).strip(),
                "state": str(r.get("state", "")).strip(),
                "zip": str(r.get("zip", "")).strip()
            }
        })
    return {"requests": reqs, "options": {"skipTrace": True}}

def call_batchdata(api_key: str, chunk: List[Dict[str, Any]], timeout=60):
    url = f"{API_BASE}/property/lookup/all-attributes"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    payload = payload_from_rows(chunk)
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    # Raise if non-2xx so caller can see JSON error printed
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text}
    if r.status_code >= 400:
        print("BatchData error:", json.dumps(body, indent=2), file=sys.stderr)
        r.raise_for_status()
    return body

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-csv", required=True, help="CSV containing columns: street, city, state, zip (case-insensitive)")
    ap.add_argument("--export-csv", default="exports/batchdata_targets_filtered.csv",
                    help="Where to write filtered targets CSV")
    ap.add_argument("--top-n", type=int, default=1000, help="Cap number of rows exported (after filtering)")
    ap.add_argument("--assessed-cap", type=int, default=1_000_000, help="Maximum total assessed value to include")
    ap.add_argument("--chunk-size", type=int, default=REQ_CHUNK, help="Requests per API call")
    args = ap.parse_args()

    dburl = os.getenv("DATABASE_URL")
    if not dburl:
        die("DATABASE_URL not set")
    api_key = os.getenv("BATCHDATA_API_KEY")
    if not api_key:
        die("BATCHDATA_API_KEY not set")

    # Read input CSV
    df = pd.read_csv(args.input_csv)
    df = normalize_cols(df)
    need_cols(df, ["street", "city", "state", "zip"])
    rows = df[["street", "city", "state", "zip"]].to_dict(orient="records")

    engine = create_engine(dburl)
    create_tables(engine)

    total_props = 0
    with engine.begin() as conn:
        for chunk in chunked(rows, args.chunk_size):
            body = call_batchdata(api_key, chunk)
            props = pull_value(body, "results", "properties", default=[]) or []
            for item in props:
                pid = item.get("_id")
                addr = item.get("address", {}) or {}
                assess = item.get("assessment", {}) or {}
                bldg = item.get("building", {}) or {}
                gen  = item.get("general", {}) or {}
                intel = item.get("intel", {}) or {}
                quick = item.get("quickLists", {}) or {}
                owner = item.get("owner", {}) or {}

                rowp = {
                    "property_id": pid,
                    "street": addr.get("street") or addr.get("streetNoUnit"),
                    "city": addr.get("city"),
                    "state": addr.get("state"),
                    "zip": addr.get("zip"),
                    "zip_plus4": addr.get("zipPlus4"),
                    "latitude": addr.get("latitude"),
                    "longitude": addr.get("longitude"),
                    "county": addr.get("county"),
                    "fips_code": pull_value(item, "ids", "fipsCode"),
                    "total_assessed_value": assess.get("totalAssessedValue"),
                    "total_market_value": assess.get("totalMarketValue"),
                    "year_built": bldg.get("yearBuilt"),
                    "living_area_sqft": bldg.get("livingAreaSquareFeet"),
                    "owner_occupied": owner.get("ownerOccupied"),
                    "vacant": gen.get("vacant"),
                    "std_land_use": gen.get("standardizedLandUseCode"),
                    "ptype_detail": gen.get("propertyTypeDetail"),
                    "last_sold_date": intel.get("lastSoldDate"),
                    "last_sold_price": intel.get("lastSoldPrice"),
                }
                upsert_property(conn, rowp)

                emails = owner.get("emails") or []
                phones = owner.get("phoneNumbers") or []
                insert_contacts(conn, pid, emails, phones)
                total_props += 1

    print(f"Inserted/updated ~{total_props} properties.")

    # Export filtered top-N
    export_sql = text("""
    COPY (
      SELECT
        p.property_id,
        p.street, p.city, p.state, p.zip,
        p.latitude, p.longitude,
        p.total_assessed_value,
        p.owner_occupied, p.vacant,
        p.property_type_detail,
        p.last_sold_date, p.last_sold_price
      FROM public.batchdata_properties p
      WHERE COALESCE(p.owner_occupied, false) = true
        AND COALESCE(p.vacant, false) = false
        AND COALESCE(p.total_assessed_value, 0) BETWEEN 1 AND :cap
      ORDER BY p.total_assessed_value DESC NULLS LAST, p.property_id
      LIMIT :limit
    ) TO STDOUT WITH CSV HEADER
    """)
    os.makedirs(os.path.dirname(args.export_csv) or ".", exist_ok=True)
    # Use psql-friendly COPY via sqlalchemy raw connection
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur, open(args.export_csv, "w", newline="") as f:
            cur.execute(export_sql, {"cap": args.assessed_cap, "limit": args.top_n})
            while True:
                data = cur.fetchone()
                if data is None:
                    break
            # ^ we can't fetch from COPY; instead use copy_expert:
    except Exception:
        conn.close()
        # Re-do using copy_expert through psycopg2
        import psycopg2
        conn2 = psycopg2.connect(dburl)
        try:
            with conn2.cursor() as cur2, open(args.export_csv, "w", newline="") as fo:
                cur2.copy_expert(export_sql._compiled_cache if hasattr(export_sql, "_compiled_cache") else str(export_sql), fo)
        finally:
            conn2.close()
        print(f"Exported filtered targets → {args.export_csv}")
        return

if __name__ == "__main__":
    main()