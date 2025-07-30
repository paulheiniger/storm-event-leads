#!/usr/bin/env python3
"""
cluster_hail.py

Pull hail points from PostGIS, cluster them, compute per-cluster convex hulls,
and write the boundaries back into PostGIS.
"""
import os
import argparse

import numpy as np
import geopandas as gpd
from shapely.ops import unary_union
from sklearn.cluster import DBSCAN
from sqlalchemy import create_engine

def main():
    p = argparse.ArgumentParser(description="Cluster hail points and store hulls in PostGIS")
    p.add_argument('--source-table',  required=True,
                   help='PostGIS table with hail points (e.g. nx3hail_20250701_20250728)')
    p.add_argument('--dest-table',    default='hail_cluster_boundaries',
                   help='PostGIS table to append cluster hulls')
    p.add_argument('--eps',           type=float, default=0.1,
                   help='DBSCAN epsilon (degrees)')
    p.add_argument('--min-samples',   type=int,   default=5,
                   help='DBSCAN min_samples')
    args = p.parse_args()

    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        p.error("DATABASE_URL env var must be set")

    engine = create_engine(DATABASE_URL)

    # 1) Load hail points from PostGIS
    print(f"Loading hail points from {args.source_table}...")
    gdf = gpd.read_postgis(
        f"SELECT *, geom FROM {args.source_table}",
        engine,
        geom_col='geom'
    )

    if gdf.empty:
        print("No points found; exiting.")
        return

    # 2) Cluster with DBSCAN
    coords = np.vstack([gdf.geometry.x, gdf.geometry.y]).T
    labels = DBSCAN(eps=args.eps, min_samples=args.min_samples).fit_predict(coords)
    gdf['cluster'] = labels

    # 3) Compute convex‐hull per cluster
    records = []
    for cid, sub in gdf.groupby('cluster'):
        if cid < 0:
            continue   # noise
        hull = unary_union(sub.geometry.values).convex_hull
        records.append({
            'dataset':      args.source_table,
            'cluster_id':   int(cid),
            'num_points':   len(sub),
            'geom':         hull
        })

    if not records:
        print("No clusters found; exiting.")
        return

    bd = gpd.GeoDataFrame(records, crs=gdf.crs)

    # 4) Write to PostGIS
    print(f"Inserting {len(bd)} cluster hulls into {args.dest_table}...")
    bd.to_postgis(
        args.dest_table,
        engine,
        if_exists='append',
        index=False
    )
    print("Done.")

if __name__ == "__main__":
    main()