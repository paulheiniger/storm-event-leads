#!/usr/bin/env python3
"""
cluster_hail.py

Cluster SWDI hail points into polygons with DBSCAN, adding:
- cluster_id
- num_points
- start_time / end_time (if present on source)
- geometry (POLYGON, EPSG:4326)

Behavior:
- Detect input geometry col ('geometry' or 'geom').
- Write results to a short staging table (keeps identifiers <63 chars).
- If destination exists: TRUNCATE + INSERT (with safe casts), then drop staging.
- If destination doesn't exist: RENAME staging -> destination.
- Create a GiST index on destination geometry.

Usage:
  python cluster/cluster_hail.py \
    --source-table swdi_nx3hail_ky_20240101_20250801 \
    --dest-table   hail_cluster_boundaries_ky_20240101_20250801 \
    --eps 0.03 --min-samples 6
"""

import os
import sys
import argparse
import secrets

import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.ops import unary_union
from shapely.geometry import Point, Polygon
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TIMESTAMP
from geoalchemy2 import Geometry as GA2Geometry


TIME_CANDIDATES = [
    ("begin_time", "end_time"),
    ("start_time", "end_time"),
    ("btm", "etm"),
    ("valid", None),
    ("datetime", None),
    ("obs_time", None),
    ("time", None),
]


def detect_geom_col(engine, table: str) -> str:
    q = text("""
        SELECT lower(column_name) AS c
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=:t
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"t": table.lower()}).fetchall()
    cols = [r[0] for r in rows]
    if "geometry" in cols:
        return "geometry"
    if "geom" in cols:
        return "geom"
    raise RuntimeError(f"No geometry column found on {table}; expected 'geometry' or 'geom'.")


def pick_time_columns(df: pd.DataFrame):
    cols = {c.lower() for c in df.columns}
    for a, b in TIME_CANDIDATES:
        if a and a in cols:
            if b and b in cols:
                return a, b
            return a, None
    return None, None


def to_polygon(hull):
    # Ensure we always return a non-empty Polygon
    if isinstance(hull, Polygon) and hull.area > 0:
        return hull
    try:
        buf = hull.buffer(1e-6)
        if isinstance(buf, Polygon) and buf.area > 0:
            return buf
    except Exception:
        pass
    env = getattr(hull, "envelope", None)
    if env is not None:
        try:
            buf2 = env.buffer(1e-9)
            if isinstance(buf2, Polygon) and buf2.area > 0:
                return buf2
        except Exception:
            pass
    # Last resort: tiny circle at centroid or origin
    try:
        return hull.centroid.buffer(1e-6)
    except Exception:
        return Point(0, 0).buffer(1e-6)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-table", required=True)
    ap.add_argument("--dest-table", required=True)
    ap.add_argument("--eps", type=float, default=0.03, help="DBSCAN eps in degrees")
    ap.add_argument("--min-samples", type=int, default=6)
    ap.add_argument("--in-geom-col", default=None, help="Override input geom col (geometry/geom)")
    args = ap.parse_args()

    dburl = os.getenv("DATABASE_URL")
    if not dburl:
        sys.exit("Error: DATABASE_URL env var is required")

    engine = create_engine(dburl)

    # Detect input geometry column
    in_geom = args.in_geom_col or detect_geom_col(engine, args.source_table)
    print(f"Loading hail points from {args.source_table} (geom='{in_geom}')...")

    # Load points
    gdf = gpd.read_postgis(
        f'SELECT * FROM "{args.source_table}"',
        engine,
        geom_col=in_geom,
    )
    if gdf.empty:
        print("No points — nothing to cluster.")
        return

    # Time columns (if present)
    tA, tB = pick_time_columns(gdf)
    if tA:
        gdf[tA] = pd.to_datetime(gdf[tA], errors="coerce", utc=True)
    if tB:
        gdf[tB] = pd.to_datetime(gdf[tB], errors="coerce", utc=True)

    # DBSCAN cluster
    from sklearn.cluster import DBSCAN
    coords = np.vstack([gdf.geometry.x.values, gdf.geometry.y.values]).T
    labels = DBSCAN(eps=args.eps, min_samples=args.min_samples).fit_predict(coords)
    gdf["__label"] = labels
    uniq = sorted(int(x) for x in np.unique(labels) if x >= 0)
    print(f"Found {len(uniq)} clusters (labels {uniq[0] if uniq else '—'}..{uniq[-1] if uniq else '—'}).")

    out_rows = []
    for cid in uniq:
        block = gdf[gdf["__label"] == cid]
        num_points = int(len(block))
        geom = unary_union(block.geometry.values).convex_hull
        poly = to_polygon(geom)

        start_ts = end_ts = None
        if tA and tB:
            start_ts = pd.to_datetime(block[tA], errors="coerce", utc=True).min()
            end_ts = pd.to_datetime(block[tB], errors="coerce", utc=True).max()
        elif tA:
            col = pd.to_datetime(block[tA], errors="coerce", utc=True)
            start_ts = col.min()
            end_ts = col.max()

        out_rows.append(
            {
                "cluster_id": cid,
                "num_points": num_points,
                "start_time": start_ts,
                "end_time": end_ts,
                "geometry": poly,
            }
        )

    if not out_rows:
        print("No clusters to write.")
        return

    bd = gpd.GeoDataFrame(out_rows, crs="EPSG:4326", geometry="geometry")

    # Short staging name (avoid 63-char limit issues on indexes)
    staging = f"hc_{secrets.token_hex(4)}"
    print(f"Inserting {len(bd)} cluster hulls into {args.dest_table} (via {staging})...")

    # Write staging with explicit types (timestamps + polygon), and no auto spatial index
    bd.to_postgis(
        staging,
        engine,
        if_exists="replace",
        index=False,
        dtype={
            "start_time": PG_TIMESTAMP(timezone=True),
            "end_time": PG_TIMESTAMP(timezone=True),
            "geometry": GA2Geometry("POLYGON", srid=4326, spatial_index=False),
        },
    )

    # Finalize into destination
    with engine.begin() as conn:
        # Does destination table already exist?
        exists = conn.execute(
            text(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.tables
                  WHERE table_schema='public' AND table_name=:t
                )
                """
            ),
            {"t": args.dest_table.lower()},
        ).scalar()

        if not exists:
            # First time: rename staging -> destination
            conn.execute(text(f'ALTER TABLE public."{staging}" RENAME TO "{args.dest_table}"'))
        else:
            # Ensure required columns exist on destination
            for colname, coltype_sql in [
                ("cluster_id", "integer"),
                ("num_points", "integer"),
                ("start_time", "timestamp with time zone"),
                ("end_time", "timestamp with time zone"),
                ("geometry", "geometry(Polygon,4326)"),
            ]:
                conn.execute(
                    text(
                        """
                        DO $$
                        BEGIN
                          IF NOT EXISTS (
                            SELECT 1
                            FROM information_schema.columns
                            WHERE table_schema='public'
                              AND table_name=:t
                              AND column_name=:c
                          ) THEN
                            EXECUTE format('ALTER TABLE public.%I ADD COLUMN %I """  # noqa: E501
                        + coltype_sql
                        + """', :t, :c);
                          END IF;
                        END $$;
                        """
                    ),
                    {"t": args.dest_table, "c": colname},
                )

            # Build common column list (ordered as in staging)
            staging_cols = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=:t
                    ORDER BY ordinal_position
                    """
                ),
                {"t": staging},
            ).scalars().all()

            dest_cols = set(
                conn.execute(
                    text(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema='public' AND table_name=:t
                        """
                    ),
                    {"t": args.dest_table},
                ).scalars().all()
            )

            common = [c for c in staging_cols if c in dest_cols]

            # Build SELECT list with casts for timestamps (safety)
            select_parts = []
            for c in common:
                if c == "start_time" or c == "end_time":
                    select_parts.append(f'("{c}")::timestamptz AS "{c}"')
                else:
                    select_parts.append(f'"{c}"')
            select_list = ", ".join(select_parts)
            col_list = ", ".join(f'"{c}"' for c in common)

            # Replace data
            conn.execute(text(f'TRUNCATE TABLE public."{args.dest_table}";'))
            conn.execute(
                text(
                    f'INSERT INTO public."{args.dest_table}" ({col_list}) '
                    f'SELECT {select_list} FROM public."{staging}";'
                )
            )
            conn.execute(text(f'DROP TABLE public."{staging}";'))

        # Create a short geometry index name (<=63 chars)
        idx = f'idx_{args.dest_table[:45]}_geom'
        conn.execute(
            text(
                f'CREATE INDEX IF NOT EXISTS "{idx}" '
                f'ON public."{args.dest_table}" USING GIST ("geometry");'
            )
        )

    print("Done.")


if __name__ == "__main__":
    main()