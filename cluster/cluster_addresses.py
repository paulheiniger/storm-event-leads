#!/usr/bin/env python3
"""
cluster_addresses.py

For each hail cluster boundary, find nearby addresses (within a buffer around the cluster centroid),
perform DBSCAN clustering on those addresses, and save address-cluster hulls to PostGIS.

Usage:
    python cluster/cluster_addresses.py \
      --hail-cluster-table hail_cluster_boundaries \
      --address-table      addresses \
      --dest-table         address_clusters_jefferson \
      --buffer             0.02 \
      --eps                0.001 \
      --min-samples        10

Env:
  DATABASE_URL must be set to your PostGIS connection string.
"""
import os
import sys
import argparse
import numpy as np
import geopandas as gpd
import pandas as pd
from shapely.ops import unary_union
from sklearn.cluster import DBSCAN
from sqlalchemy import create_engine, text


def get_geom_col(engine, table_name: str) -> str:
    """Return 'geom' or 'geometry' if present, else raise."""
    q = text("""
        SELECT lower(column_name) AS column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = :t
    """)
    with engine.connect() as conn:
        cols = pd.read_sql(q, conn, params={"t": table_name.lower()})["column_name"].tolist()
    for cand in ("geom", "geometry"):
        if cand in cols:
            return cand
    raise RuntimeError(f"No geometry column ('geom' or 'geometry') found on {table_name}.")


def main():
    parser = argparse.ArgumentParser(description="Cluster addresses near hail cluster centroids")
    parser.add_argument('--hail-cluster-table', required=True,
                        help='Table with hail cluster polygons (geom/geometry column)')
    parser.add_argument('--address-table', required=True,
                        help='Table with address point geometries (geom/geometry column)')
    parser.add_argument('--dest-table', required=True,
                        help='Destination table for address cluster hulls')
    parser.add_argument('--buffer', type=float, default=0.01,
                        help='Buffer around centroid in degrees (EPSG:4326)')
    parser.add_argument('--eps', type=float, default=0.001,
                        help='DBSCAN eps parameter in degrees (EPSG:4326)')
    parser.add_argument('--min-samples', type=int, default=5,
                        help='DBSCAN min_samples')
    args = parser.parse_args()

    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        sys.exit('Error: DATABASE_URL environment variable must be set')

    engine = create_engine(DATABASE_URL)

    # Detect geometry column names
    hail_geom_col = get_geom_col(engine, args.hail_cluster_table)
    addr_geom_col = get_geom_col(engine, args.address_table)

    print(f"[info] Using hail geometry column '{hail_geom_col}' on {args.hail_cluster_table}")
    print(f"[info] Using address geometry column '{addr_geom_col}' on {args.address_table}")

    # 1) Load hail cluster centroids (server-side centroid from detected geom col)
    sql_centroids = text(f"""
        SELECT cluster_id AS hail_cluster_id,
               ST_Centroid({hail_geom_col}) AS geom
        FROM {args.hail_cluster_table}
    """)
    centroids = gpd.read_postgis(sql_centroids, engine, geom_col='geom')
    if centroids.empty:
        print(f"[warn] No records in {args.hail_cluster_table}. Nothing to do.")
        return

    print(f"[info] Loaded {len(centroids)} hail cluster centroids")

    records = []

    # 2) For each hail cluster centroid, buffer and fetch addresses
    for _, row in centroids.iterrows():
        hail_id = int(row['hail_cluster_id'])
        buffer_geom = row.geom.buffer(args.buffer)  # degrees in EPSG:4326

        # Pull candidate addresses intersecting the buffer
        sql_addrs = text(f"""
            SELECT id, {addr_geom_col} AS geom
            FROM {args.address_table}
            WHERE ST_Intersects({addr_geom_col}, ST_GeomFromText(:wkt, 4326))
        """)
        params = {'wkt': buffer_geom.wkt}
        addrs = gpd.read_postgis(sql_addrs, engine, geom_col='geom', params=params)

        if addrs.empty:
            continue

        # 3) DBSCAN on lon/lat (degrees); keep eps consistent with degrees
        coords = np.vstack((addrs.geom.x, addrs.geom.y)).T
        labels = DBSCAN(eps=args.eps, min_samples=args.min_samples).fit_predict(coords)
        addrs['addr_cluster'] = labels

        # 4) Build convex hull per address cluster (skip noise label -1)
        for cid, group in addrs.groupby('addr_cluster'):
            if cid < 0 or group.empty:
                continue
            hull = unary_union(group.geometry.values).convex_hull
            records.append({
                'hail_cluster_id': hail_id,
                'addr_cluster_id': int(cid),
                'num_addresses':   int(len(group)),
                'geom':            hull
            })

    if not records:
        print("[warn] No address clusters generated.")
        return

    # 5) Write address cluster hulls to PostGIS
    addr_gdf = gpd.GeoDataFrame(records, crs='EPSG:4326', geometry='geom')
    print(f"[info] Writing {len(addr_gdf)} address clusters to {args.dest_table}")
    addr_gdf.to_postgis(args.dest_table, engine, if_exists='replace', index=False)
    print("[done] Address clustering complete.")


if __name__ == '__main__':
    main()