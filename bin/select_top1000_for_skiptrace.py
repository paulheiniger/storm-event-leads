#!/usr/bin/env python3
"""
Select top 1000 skip-trace targets from enriched property data.

Filters:
  - owner_occupied = TRUE
  - vacant = FALSE
  - total_assessed_value <= max_assessed (default 1,000,000)
Order:
  - total_assessed_value DESC (NULLS LAST)
Limit:
  - 1000 (configurable)

Optional spatial filter:
  --center "lon,lat" and --radius-km N  (keeps rows whose lat/lon fall in that circle)

Usage examples:
  python bin/select_top1000_for_skiptrace.py \
    --state KY \
    --center "-85.7585,38.2527" \
    --radius-km 40 \
    --max-assessed 1000000 \
    --limit 1000 \
    --out exports/skiptrace_top1000_KY.csv
"""

import os
import sys
import argparse
from sqlalchemy import create_engine, text
import csv

def die(msg, code=2):
    print(msg, file=sys.stderr)
    sys.exit(code)

def build_where(state, max_assessed, need_circle):
    wh = [
        "p.owner_occupied = TRUE",
        "COALESCE(p.vacant, FALSE) = FALSE",
        "COALESCE(p.total_assessed_value, 0) <= :max_assessed",
    ]
    if state:
        wh.append("p.address_state = :state")
    if need_circle:
        # circle test will be appended in the CTE using lon/lat
        wh.append("1=1")
    return " AND ".join(wh)

def main():
    ap = argparse.ArgumentParser(description="Filter enriched properties for skiptrace")
    ap.add_argument("--state", default=None, help="Filter by state code (e.g., KY)")
    ap.add_argument("--center", default=None, help='Optional "lon,lat" center for spatial filter (WGS84)')
    ap.add_argument("--radius-km", type=float, default=None, help="Optional radius (km) around center")
    ap.add_argument("--max-assessed", type=float, default=1_000_000.0, help="Cap on total assessed value")
    ap.add_argument("--limit", type=int, default=1000, help="Max rows to output")
    ap.add_argument("--property-table", default="batchdata_properties", help="Table with enriched property rows")
    ap.add_argument("--out", required=True, help="Output CSV path")
    args = ap.parse_args()

    dburl = os.getenv("DATABASE_URL")
    if not dburl:
        die("DATABASE_URL not set")

    center_lon = center_lat = None
    radius_m = None
    need_circle = False
    if args.center and args.radius_km:
        try:
            lon_str, lat_str = args.center.replace(" ", "").split(",")
            center_lon = float(lon_str)
            center_lat = float(lat_str)
            radius_m = float(args.radius_km) * 1000.0
            need_circle = True
        except Exception:
            die('Invalid --center. Use "lon,lat" (e.g., "-85.7585,38.2527")')

    engine = create_engine(dburl)

    where_sql = build_where(args.state, args.max_assessed, need_circle)

    # NOTE: Adjust column names below if your schema differs.
    # Expected columns:
    #   p.address_street, p.address_city, p.address_state, p.address_zip,
    #   p.latitude, p.longitude,
    #   p.owner_occupied (bool), p.vacant (bool),
    #   p.total_assessed_value (numeric),
    #   p.owner_names (text[]), p.emails (text[]), p.phones (jsonb),
    #   p.property_id (text)
    sql = f"""
WITH source AS (
  SELECT
    p.property_id,
    p.address_street,
    p.address_city,
    p.address_state,
    p.address_zip,
    p.latitude,
    p.longitude,
    p.owner_occupied,
    p.vacant,
    p.total_assessed_value,
    p.owner_names,
    p.emails,
    p.phones
  FROM public.{args.property_table} p
  WHERE {where_sql}
),
circ AS (
  SELECT s.*
  FROM source s
  {"JOIN LATERAL (SELECT 1) AS _ ON TRUE" if need_circle else ""}
  {"WHERE s.latitude IS NOT NULL AND s.longitude IS NOT NULL" if need_circle else ""}
),
ranked AS (
  SELECT
    c.*,
    ROW_NUMBER() OVER (ORDER BY c.total_assessed_value DESC NULLS LAST) AS rn
  FROM (
    SELECT * FROM source
    { "-- radius filter" if need_circle else "" }
  ) c
  {"" if not need_circle else ""}
)
SELECT
  property_id,
  address_street,
  address_city,
  address_state,
  address_zip,
  latitude,
  longitude,
  owner_occupied,
  vacant,
  total_assessed_value,
  owner_names,
  emails,
  phones
FROM ranked
WHERE rn <= :limit
ORDER BY total_assessed_value DESC NULLS LAST, property_id;
    """

    # If spatial circle is requested, refine SQL to apply great-circle-ish filter using PostGIS if available,
    # or a simple bounding check if PostGIS isn’t installed. Here we’ll do a spherical (geography) check.
    if need_circle:
        sql = f"""
WITH params AS (
  SELECT ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography AS center_geo,
         :radius_m::double precision AS radius_m
),
source AS (
  SELECT
    p.property_id,
    p.address_street,
    p.address_city,
    p.address_state,
    p.address_zip,
    p.latitude,
    p.longitude,
    p.owner_occupied,
    p.vacant,
    p.total_assessed_value,
    p.owner_names,
    p.emails,
    p.phones,
    ST_SetSRID(ST_MakePoint(p.longitude, p.latitude), 4326)::geography AS geom_geo
  FROM public.{args.property_table} p
  WHERE {where_sql}
),
filtered AS (
  SELECT s.*
  FROM source s
  JOIN params p ON ST_DWithin(s.geom_geo, p.center_geo, p.radius_m)
),
ranked AS (
  SELECT
    f.*,
    ROW_NUMBER() OVER (ORDER BY f.total_assessed_value DESC NULLS LAST) AS rn
  FROM filtered f
)
SELECT
  property_id,
  address_street,
  address_city,
  address_state,
  address_zip,
  latitude,
  longitude,
  owner_occupied,
  vacant,
  total_assessed_value,
  owner_names,
  emails,
  phones
FROM ranked
WHERE rn <= :limit
ORDER BY total_assessed_value DESC NULLS LAST, property_id;
        """

    params = {
        "max_assessed": args.max_assessed,
        "limit": args.limit,
    }
    if args.state:
        params["state"] = args.state
    if need_circle:
        params.update({"lon": center_lon, "lat": center_lat, "radius_m": radius_m})

    with engine.begin() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    if not rows:
        print("No rows matched filters.")
        # still write header so you see schema
        header = [
            "property_id","address_street","address_city","address_state","address_zip",
            "latitude","longitude","owner_occupied","vacant","total_assessed_value",
            "owner_names","emails","phones"
        ]
        with open(args.out, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
        print(f"✔ Wrote 0 rows → {args.out}")
        return

    # Write CSV
    header = list(rows[0].keys())
    with open(args.out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for r in rows:
            # stringify arrays/json so CSV is clean
            vals = []
            for k in header:
                v = r[k]
                if isinstance(v, (list, dict)):
                    vals.append(str(v))
                else:
                    vals.append(v)
            writer.writerow(vals)

    print(f"✔ Wrote {len(rows)} rows → {args.out}")

if __name__ == "__main__":
    main()