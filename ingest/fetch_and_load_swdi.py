#!/usr/bin/env python3
"""
fetch_and_load_swdi.py

Fetch multiple SWDI datasets as shapefiles via the SWDI Web Services,
then load each into PostGIS.

Usage:
  python fetch_and_load_swdi.py --start 2025-07-01 --end 2025-07-28 \
      [--bbox "minLon,minLat,maxLon,maxLat"] [--datasets warn nx3hail ...]

Environment:
  DATABASE_URL: postgresql://user:pass@host:port/db
"""
import os
import sys
import argparse
import tempfile
import zipfile
import shutil
from urllib.parse import urlencode
import requests
import geopandas as gpd
from sqlalchemy import create_engine

# Base SWDI Web Services root
SWDI_ROOT = "https://www.ncdc.noaa.gov/swdiws"

# Default datasets to pull
DEFAULT_DATASETS = [
    'warn',        # warnings (polygons)
    'nx3tvs',      # tornado vortex signatures (points)
    'nx3meso',     # mesocyclone signatures (points)
    'nx3hail',     # hail signatures (points)
    'nx3structure' # storm cell structure (points)
]


def fetch_shapefile(dataset, date_range, bbox=None, out_dir=None):
    """
    Fetch shapefile ZIP for a dataset/date_range (YYYYMMDD:YYYYMMDD) and optional bbox.
    Returns path to extracted shapefile (.shp).
    """
    fmt = 'shp'
    # build URL path + params
    path = f"{SWDI_ROOT}/{fmt}/{dataset}/{date_range}"
    params = {}
    if bbox:
        params['bbox'] = bbox
    url = path if not params else f"{path}?{urlencode(params)}"
    print(f"Downloading {dataset}: {url}")
    r = requests.get(url, stream=True)
    r.raise_for_status()

    # write to temp zip
    tmp = tempfile.mkdtemp(prefix=f"swdi_{dataset}_")
    zip_path = os.path.join(tmp, f"{dataset}.zip")
    with open(zip_path, 'wb') as f:
        for chunk in r.iter_content(1024*1024):
            f.write(chunk)

    # unzip
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(tmp)
    # find .shp
    for fn in os.listdir(tmp):
        if fn.endswith('.shp'):
            return os.path.join(tmp, fn)
    raise FileNotFoundError(f"No .shp found in {zip_path}")


def load_to_postgis(shp_path, table_name, database_url):
    """
    Load the shapefile at shp_path into PostGIS under table_name.
    """
    print(f"Loading {shp_path} into PostGIS table {table_name}")
    gdf = gpd.read_file(shp_path)
    engine = create_engine(database_url)
    gdf.to_postgis(table_name, engine, if_exists='replace', index=False)
    print(f"Loaded {len(gdf)} records into {table_name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', required=True,
                        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end',   required=True,
                        help='End date (YYYY-MM-DD)')
    parser.add_argument('--bbox',  default=None,
                        help='Optional bbox minLon,minLat,maxLon,maxLat')
    parser.add_argument('--datasets', nargs='+', default=DEFAULT_DATASETS,
                        help='List of SWDI datasets to fetch')
    args = parser.parse_args()

    date_range = args.start.replace('-', '') + ':' + args.end.replace('-', '')
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        sys.exit('Error: set DATABASE_URL env var')

    for ds in args.datasets:
        try:
            shp = fetch_shapefile(ds, date_range, bbox=args.bbox)
            table = f"swdi_{ds}_{args.start.replace('-','')}_{args.end.replace('-','')}"
            load_to_postgis(shp, table, database_url)
        except Exception as e:
            print(f"Failed {ds}: {e}")

if __name__ == '__main__':
    main()