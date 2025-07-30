#!/usr/bin/env python3
"""
fetch_boundaries.py

• Downloads the latest StormEvents_locations CSV for a given year.
• Extracts point locations for each event.
• Groups points by EPISODE_ID and EVENT_ID, computing the convex hull to approximate the event boundary.
• Loads the resulting GeoDataFrame into PostGIS.

Usage:
  python ingest/fetch_boundaries.py <YEAR> [--table <TABLE_NAME>]

Environment:
  DATABASE_URL: PostgreSQL connection string (e.g. postgresql://user:pass@host:port/db)
"""
import os
import sys
import re
import tempfile
import shutil
from urllib.parse import urljoin
from urllib.request import urlretrieve

import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from sqlalchemy import create_engine

# Base URL for NOAA StormEvents locations CSVs
CSV_BASE_URL = "https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
FILENAME_PATTERN = r'href="(StormEvents_details-ftp_v1\.0_d{year}_c(\d{{8}})\.csv\.gz)"'
DEFAULT_TABLE = os.getenv('BOUNDARY_TABLE', 'storm_event_boundaries')
DATABASE_URL = os.getenv('DATABASE_URL')


def fetch_latest_csv_url(year: int) -> str:
    """
    Scrape the CSV directory and return the URL of the latest details CSV for the specified year.
    """
    resp = requests.get(CSV_BASE_URL)
    resp.raise_for_status()
    pattern = FILENAME_PATTERN.format(year=year)
    matches = re.findall(pattern, resp.text)
    if not matches:
        raise RuntimeError(f"No details CSV found for year {year}")
    latest = max(matches, key=lambda x: x[1])[0]
    return urljoin(CSV_BASE_URL, latest)


def download_and_extract_csv(csv_url: str) -> str:
    """
    Download and decompress the .csv.gz file, return local CSV path.
    """
    workdir = tempfile.mkdtemp(prefix="storm_csv_")
    gz_path = os.path.join(workdir, os.path.basename(csv_url))
    print(f"Downloading {csv_url}")
    urlretrieve(csv_url, gz_path)
    csv_path = gz_path[:-3]
    with __import__('gzip').open(gz_path, 'rb') as f_in, open(csv_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    return csv_path


def build_boundaries(csv_path: str) -> gpd.GeoDataFrame:
    """
    Read the locations CSV, build convex hulls per event.
    """
    df = pd.read_csv(csv_path,
                     usecols=["EPISODE_ID", "EVENT_ID", "BEGIN_LAT", "BEGIN_LON"]
                     )
    df = df.dropna(subset=["BEGIN_LAT", "BEGIN_LON"])
    gdf_pts = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.BEGIN_LON, df.BEGIN_LAT),
        crs="EPSG:4326"
    )
    gdf_hulls = (
        gdf_pts
        .dissolve(by=["EPISODE_ID", "EVENT_ID"], as_index=False)
        .assign(geometry=lambda d: d.geometry.convex_hull)
    )
    return gdf_hulls


def load_to_postgis(gdf: gpd.GeoDataFrame, table_name: str):
    """
    Load GeoDataFrame to PostGIS table (replace if exists).
    """
    if not DATABASE_URL:
        sys.exit("Error: DATABASE_URL not set")
    print(f"Loading {len(gdf)} records into PostGIS table '{table_name}'...")
    engine = create_engine(DATABASE_URL)
    gdf.to_postgis(table_name, engine, if_exists='replace', index=False)
    print("Load complete.")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('year', type=int, help='Year of the StormEvents locations CSV')
    parser.add_argument('--table', default=DEFAULT_TABLE, help='PostGIS table name')
    args = parser.parse_args()

    csv_url = fetch_latest_csv_url(args.year)
    csv_path = download_and_extract_csv(csv_url)
    gdf = build_boundaries(csv_path)
    load_to_postgis(gdf, args.table)

if __name__ == '__main__':
    main()
