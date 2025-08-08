#!/usr/bin/env python3
"""
cluster_hail.py

DBSCAN -> convex hulls -> polygons. Writes to PostGIS without dropping tables
(so dependent views keep working): if table exists => TRUNCATE + append; else create.

Usage:
  python cluster/cluster_hail.py \
    --source-table swdi_nx3hail_in_20240101_20250801 \
    --dest-table   hail_cluster_boundaries_in_20240101_20250801 \
    --eps 0.03 --min-samples 6
"""
import os
import sys
import argparse
import numpy as np
import geopandas as gpd
import pandas as pd
from shapely.ops import unary_union
from sqlalchemy import create_engine, text

# Small buffer to polygonize Point/LineString hulls (degrees)
MIN_HULL_BUFFER_DEG = 0.0005

try:
    from geoalchemy2 import Geometry
    HAVE_GEOALCHEMY2 = True
except Exception:
    HAVE_GEOALCHEMY2 = False


def pick_geom_col(engine, table_name: str) -> str:
    with engine.connect() as conn:
        cols = pd.read_sql(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=:t
            """),
            conn,
            params={"t": table_name.lower()},
        )["column_name"].str.lower().tolist()
    if "geometry" in cols:
        return "geometry"
    if "geom" in cols:
        return "geom"
    raise RuntimeError(f"No geometry/geom column found on {table_name}.")


def table_exists(engine, table_name: str) -> bool:
    with engine.connect() as conn:
        val = conn.execute(
            text("SELECT to_regclass(:qname)"),
            {"qname": f"public.{table_name.lower()}"}
        ).scalar()
    return val is not None


def truncate_table(engine, table_name: str):
    # Double-quote the identifier to be safe with case
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE "{table_name}"'))


def main():
    ap = argparse.ArgumentParser(description="DBSCAN hail point clusters -> polygon hulls")
    ap.add_argument("--source-table", required=True)
    ap.add_argument("--dest-table", required=True)
    ap.add_argument("--eps", type=float, default=0.1)
    ap.add_argument("--min-samples", type=int, default=5)
    args = ap.parse_args()

    dburl = os.getenv("DATABASE_URL")
    if not dburl:
        sys.exit("Error: DATABASE_URL must be set.")
    engine = create_engine(dburl)

    geom_col = pick_geom_col(engine, args.source_table)
    print(f"Loading hail points from {args.source_table} (geom='{geom_col}')...")
    gdf = gpd.read_postgis(text(f"SELECT * FROM {args.source_table}"), engine, geom_col=geom_col)
    if gdf.empty:
        print("No points found; nothing to cluster.")
        return
    if gdf.crs is None:
        gdf.set_crs(epsg=4326, inplace=True)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    from sklearn.cluster import DBSCAN  # lazy import
    coords = np.column_stack([gdf.geometry.x.values, gdf.geometry.y.values])
    labels = DBSCAN(eps=args.eps, min_samples=args.min_samples).fit_predict(coords)
    gdf["label"] = labels

    valid = gdf[gdf["label"] >= 0]
    n_clusters = 0 if valid.empty else int(valid["label"].max() + 1)
    print(f"Found {n_clusters} clusters (labels 0..{valid['label'].max() if n_clusters else -1}).")

    records = []
    cid = 0
    for _, grp in valid.groupby("label"):
        hull = unary_union(grp.geometry.values).convex_hull
        if hull.geom_type in ("Point", "LineString"):
            hull = hull.buffer(MIN_HULL_BUFFER_DEG)
        if hull.geom_type == "MultiPolygon":
            hull = unary_union([g for g in hull.geoms if not g.is_empty])
        hull = hull.buffer(0)  # clean

        if hull.is_empty or hull.geom_type != "Polygon":
            continue

        cid += 1
        records.append({"cluster_id": int(cid), "num_points": int(len(grp)), "geometry": hull})

    if not records:
        print("No valid polygon hulls created.")
        return

    out = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")
    print(f"Inserting {len(out)} cluster hulls into {args.dest_table}...")

    if table_exists(engine, args.dest_table):
        # Keep the existing table (and any views), just empty it
        truncate_table(engine, args.dest_table)
        out.to_postgis(args.dest_table, engine, if_exists="append", index=False)
    else:
        # First time: create the table with a POLYGON column
        if HAVE_GEOALCHEMY2:
            out.to_postgis(
                args.dest_table, engine, if_exists="replace", index=False,
                dtype={"geometry": Geometry("POLYGON", srid=4326)}
            )
        else:
            out.to_postgis(args.dest_table, engine, if_exists="replace", index=False)

    print("Done.")


if __name__ == "__main__":
    main()