#!/usr/bin/env python3
"""
ingest_openaddresses.py

Load local OpenAddresses GeoJSON files from provided folders into PostGIS.

Usage:
  python ingest_openaddresses.py \
    --folders /Users/paulheiniger/Downloads/us_south \
              /Users/paulheiniger/Downloads/us_west \
              /Users/paulheiniger/Downloads/us_midwest \
              /Users/paulheiniger/Downloads/us_northeast \
    --table addresses

Environment:
  DATABASE_URL: PostgreSQL connection string, e.g. postgresql://user:pass@host:port/db
"""
import os
import sys
import argparse
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine


def collect_geojson_files(folders):
    """
    Recursively collect all .geojson (or .json) files under given folders.
    """
    files = []
    for folder in folders:
        if not os.path.isdir(folder):
            print(f"Warning: {folder} is not a directory, skipping.")
            continue
        for root, _, filenames in os.walk(folder):
            for fn in filenames:
                if fn.lower().endswith(('.geojson', '.json')):
                    files.append(os.path.join(root, fn))
    return files


def main():
    parser = argparse.ArgumentParser(description="Ingest local OpenAddresses GeoJSON into PostGIS")
    parser.add_argument('--folders', nargs='+', required=True,
                        help='List of directories containing GeoJSON files')
    parser.add_argument('--table', default='addresses',
                        help='PostGIS table name to store addresses')
    args = parser.parse_args()

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        sys.exit('Error: DATABASE_URL not set')

    files = collect_geojson_files(args.folders)
    if not files:
        sys.exit(f"Error: No GeoJSON files found in {args.folders}")

    gdfs = []
    for f in files:
        try:
            print(f"Reading {f}...")
            gdf = gpd.read_file(f)
            gdfs.append(gdf)
        except Exception as e:
            print(f"Skipping {f}: {e}")

    if not gdfs:
        sys.exit("Error: No valid GeoDataFrames loaded.")

    # Concatenate all GeoDataFrames, preserving schema
    combined = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)

    # Load into PostGIS
    engine = create_engine(database_url)
    print(f"Loading {len(combined)} address records into table '{args.table}'...")
    combined.to_postgis(args.table, engine, if_exists='replace', index=False)
    print("Ingestion complete.")


if __name__ == '__main__':
    main()