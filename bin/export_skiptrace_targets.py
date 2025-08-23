#!/usr/bin/env python3
"""
export_skiptrace_targets.py

Select addresses near hail cluster polygons and write a CSV for skiptrace.

Args:
  --state KY
  --center "-85.7585,38.2527"
  --radius-km 40
  --dist-m 200
  --target 1000
  --hail-table hail_cluster_boundaries_ky_20240101_20250801
  --addr-table addresses
  --outfile exports/skiptrace_....csv
  --include-multiunits
  --source storm-leads
  --debug

Env:
  DATABASE_URL
"""
import os
import sys
import argparse
import pathlib
import json
import re

import pandas as pd
from sqlalchemy import create_engine, text

def die(msg: str, code: int = 2):
    print(msg, file=sys.stderr)
    sys.exit(code)

def sanitize_name(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        die(f"Unsafe identifier: {name!r}")
    return name

def detect_hail_geom_col(conn, hail_table: str) -> str:
    q = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name = :t
          AND column_name IN ('geometry','geom')
        ORDER BY CASE WHEN column_name='geometry' THEN 0 ELSE 1 END
        LIMIT 1
    """)
    col = conn.execute(q, {"t": hail_table.lower()}).scalar()
    return col  # may be None

def main():
    ap = argparse.ArgumentParser(description="Export candidate addresses for skiptrace (near hail clusters).")
    ap.add_argument("--state", required=True)
    ap.add_argument("--center", required=True, help="lon,lat")
    ap.add_argument("--radius-km", type=float, default=40.0)
    ap.add_argument("--dist-m", type=float, default=200.0)
    ap.add_argument("--target", type=int, default=1000)
    ap.add_argument("--hail-table", default=None)
    ap.add_argument("--addr-table", default="addresses")
    ap.add_argument("--outfile", default=None)
    ap.add_argument("--include-multiunits", action="store_true")
    ap.add_argument("--source", default="storm-leads")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    dburl = os.getenv("DATABASE_URL")
    if not dburl:
        die("DATABASE_URL not set")

    try:
        lon_str, lat_str = [s.strip() for s in args.center.split(",", 1)]
        lon = float(lon_str)
        lat = float(lat_str)
    except Exception:
        die(f"Invalid --center (use 'lon,lat'): {args.center!r}")

    radius_m = int(round(args.radius_km * 1000.0))

    state = args.state.strip().upper()
    hail_table = args.hail_table or f"hail_cluster_boundaries_{state.lower()}"
    addr_table = args.addr_table

    # Validate identifiers to avoid SQL injection
    hail_table = sanitize_name(hail_table)
    addr_table = sanitize_name(addr_table)

    # Default outfile path
    if args.outfile:
        outpath = pathlib.Path(args.outfile)
    else:
        pathlib.Path("exports").mkdir(parents=True, exist_ok=True)
        outname = f"skiptrace_{state}_{pd.Timestamp.now().strftime('%Y%m%d-%H%M%S')}_{int(args.radius_km)}km_{int(args.dist_m)}m.csv"
        outpath = pathlib.Path("exports") / outname

    engine = create_engine(dburl)

    with engine.begin() as conn:
        hail_geom_col = detect_hail_geom_col(conn, hail_table)
        if not hail_geom_col:
            die(f"Could not find geometry column on public.{hail_table} (looked for geometry/geom)")

    # Quote table/column names safely
    hail_tbl = f'public."{hail_table}"'
    addr_tbl = f'public."{addr_table}"'
    hail_geom_q = f'"{hail_geom_col}"'

    # NOTE: The only change needed for your error is below:
    # use (:include_multi = 1) instead of (:include_multi::int = 1)
    sql = f"""
WITH params AS (
  SELECT ST_Transform(
           ST_Buffer(
             ST_Transform(ST_SetSRID(ST_MakePoint(:lon, :lat), 4326), 3857),
             :radius_m
           ),
           4326
         ) AS target_area
),
hail AS (
  SELECT cluster_id, {hail_geom_q} AS geom,
         NULLIF(start_time, TIMESTAMP 'epoch') AS start_time,
         NULLIF(end_time,   TIMESTAMP 'epoch') AS end_time
  FROM {hail_tbl}
  WHERE ST_Intersects({hail_geom_q}, (SELECT target_area FROM params))
),
addr AS (
  SELECT DISTINCT ON (lower(coalesce(address,'')), coalesce(zip,''))
         id, address, street, city, state, zip, geom
  FROM {addr_tbl} a
  WHERE state = :state
    AND address IS NOT NULL
    AND address !~* 'PO[[:space:]]*BOX'
    AND (:include_multi = 1 OR address !~* '(APT|UNIT|STE|SUITE|#[[:space:]]*[0-9]+)')
    AND ST_Intersects(geom, (SELECT target_area FROM params))
)
SELECT
  a.id, a.address, a.street, a.city, a.state, a.zip,
  ST_X(a.geom) AS lon, ST_Y(a.geom) AS lat,
  h.cluster_id,
  COALESCE(h.end_time, h.start_time) AS storm_time,
  ROUND(ST_Distance(a.geom::geography, h.geom::geography)::numeric, 1) AS distance_m
FROM addr a
JOIN hail h
  ON ST_DWithin(a.geom::geography, h.geom::geography, :dist_m)
ORDER BY storm_time DESC NULLS LAST, distance_m ASC, a.id
LIMIT :target
"""

    params = {
        "lon": lon,
        "lat": lat,
        "radius_m": radius_m,
        "state": state,
        "include_multi": 1 if args.include_multiunits else 0,
        "dist_m": float(args.dist_m),
        "target": int(args.target),
    }

    if args.debug:
        print("---- SQL ----")
        print(sql)
        print("---- params ----")
        print(json.dumps(params, indent=2))

    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(text(sql), conn, params=params)
    except Exception as e:
        die(f"Query failed: {e}")

    outpath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(outpath, index=False)

    # Marker for your orchestrator to pick up
    print(f"::OUTFILE:: {outpath}")
    print(f"✅ Exported {len(df)} rows → {outpath}")

if __name__ == "__main__":
    main()