#!/usr/bin/env python3
"""
cluster_addresses.py

For each hail cluster boundary, find nearby addresses (within a buffer around the cluster centroid),
perform DBSCAN clustering on those addresses, and save address-cluster hulls to PostGIS.

Usage: python ingest/cluster_addresses.py --hail-cluster-table hail_cluster_boundaries_atlanta \
                                         --address-table addresses \
                                         --dest-table address_clusters_atlanta \
                                         --buffer 0.02 --eps 0.001 --min-samples 10

Environment:
  DATABASE_URL environment variable must be set to your PostGIS connection string.
"""
import os
import sys
import argparse
import numpy as np
import geopandas as gpd
from shapely.ops import unary_union
from sklearn.cluster import DBSCAN
from sqlalchemy import create_engine, text


def main():
    parser = argparse.ArgumentParser(description="Cluster addresses near hail cluster centroids")
    parser.add_argument('--hail-cluster-table', required=True,
                        help='Table with hail cluster polygons (geom column)')
    parser.add_argument('--address-table', required=True,
                        help='Table with address point geometries (geometry column)')
    parser.add_argument('--dest-table', required=True,
                        help='Destination table for address cluster hulls')
    parser.add_argument('--buffer', type=float, default=0.01,
                        help='Buffer around centroid in degrees')
    parser.add_argument('--eps', type=float, default=0.001,
                        help='DBSCAN eps parameter in degrees')
    parser.add_argument('--min-samples', type=int, default=5,
                        help='DBSCAN min_samples')
    args = parser.parse_args()

    DATABASE_URL = os.getenv('DATABASE_URL')
    if not DATABASE_URL:
        sys.exit('Error: DATABASE_URL environment variable must be set')

    engine = create_engine(DATABASE_URL)

    # 1) Load hail cluster centroids
    sql_centroids = text(
        f"SELECT cluster_id AS hail_cluster_id, ST_Centroid(geometry) AS geom "
        f"FROM {args.hail_cluster_table};"
    )
    centroids = gpd.read_postgis(sql_centroids, engine, geom_col='geom')
    if centroids.empty:
        print(f"No records in {args.hail_cluster_table}."); return

    records = []
    # 2) For each hail cluster centroid, buffer and fetch addresses
    for _, row in centroids.iterrows():
        hail_id = row['hail_cluster_id']
        buffer_geom = row.geom.buffer(args.buffer)

        sql_addrs = text(
            f"SELECT id, geom FROM {args.address_table} "
            f"WHERE ST_Intersects(geom, ST_GeomFromText(:wkt, 4326));"
        )
        params = {'wkt': buffer_geom.wkt}
        addrs = gpd.read_postgis(sql_addrs, engine, geom_col='geom', params=params)
        if addrs.empty:
            continue

        # 3) Cluster addresses using DBSCAN
        coords = np.vstack((addrs.geom.x, addrs.geom.y)).T
        labels = DBSCAN(eps=args.eps, min_samples=args.min_samples).fit_predict(coords)
        addrs['addr_cluster'] = labels

        # 4) Build convex hull per address cluster
        for cid, group in addrs.groupby('addr_cluster'):
            if cid < 0:
                continue  # skip noise
            hull = unary_union(group.geometry.values).convex_hull
            records.append({
                'hail_cluster_id': int(hail_id),
                'addr_cluster_id': int(cid),
                'num_addresses':   len(group),
                'geom':            hull
            })

    if not records:
        print("No address clusters generated.")
        return

    # 5) Write address cluster hulls to PostGIS
    addr_gdf = gpd.GeoDataFrame(records, crs='EPSG:4326', geometry='geom')
    print(f"Writing {len(addr_gdf)} address clusters to {args.dest_table}")
    addr_gdf.to_postgis(args.dest_table, engine, if_exists='replace', index=False)
    print("Address clustering complete.")

if __name__ == '__main__':
    main()
