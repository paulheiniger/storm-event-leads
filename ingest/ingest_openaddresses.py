#!/usr/bin/env python3
"""
ingest_openaddresses.py

Load local OpenAddresses GeoJSON files from provided folders into PostGIS,
tracking processed files so you can resume without reprocessing.
"""

import os
import sys
import argparse
import geopandas as gpd
import pandas as pd
from geoalchemy2 import Geometry
from sqlalchemy import create_engine, text

# candidate field names
NUMBER_FIELDS   = ['number', 'house_number', 'house', 'addr_num']
STREET_FIELDS   = ['street', 'street_name', 'road', 'addr_street']
CITY_FIELDS     = ['city', 'locality', 'town']
REGION_FIELDS   = ['region', 'state']
POSTCODE_FIELDS = ['postcode', 'postal_code', 'zip']


def find_field(row, candidates):
    for col in candidates:
        if col in row and pd.notna(row[col]) and str(row[col]).strip():
            return str(row[col]).strip()
    return None


def make_address_string(row):
    num    = find_field(row, NUMBER_FIELDS)
    street = find_field(row, STREET_FIELDS)
    if not (num or street):
        return None
    base = " ".join([p for p in (num, street) if p])
    extras = [ find_field(row, CITY_FIELDS),
               find_field(row, REGION_FIELDS),
               find_field(row, POSTCODE_FIELDS) ]
    extras = [e for e in extras if e]
    return f"{base}, {', '.join(extras)}" if extras else base


def collect_geojson_files(folders):
    out = []
    for folder in folders:
        if not os.path.isdir(folder):
            print(f"[!] {folder} is not a directory, skipping")
            continue
        for root, _, files in os.walk(folder):
            for fn in files:
                if fn.lower().endswith(('.geojson', '.json')):
                    out.append(os.path.join(root, fn))
    return sorted(out)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--folders', nargs='+', required=True)
    p.add_argument('--table', default='addresses')
    args = p.parse_args()

    DB = os.getenv('DATABASE_URL')
    if not DB:
        sys.exit("ERROR: DATABASE_URL not set")

    engine = create_engine(DB)

    # make the import-log
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS oa_import_log (
              file_path  TEXT PRIMARY KEY,
              imported_at TIMESTAMP DEFAULT NOW()
            );
        """))

    files = collect_geojson_files(args.folders)
    if not files:
        sys.exit("ERROR: no .geojson/.json files found under those folders")

    total_inserted = 0

    for fp in files:
        fp = os.path.abspath(fp)
        # skip if already done
        with engine.connect() as conn:
            if conn.execute(text("SELECT 1 FROM oa_import_log WHERE file_path=:fp"), {'fp':fp}).first():
                continue

        print(f"---\nProcessing {fp}")

        # read
        try:
            gdf = gpd.read_file(fp)
        except Exception as e:
            print(f"  FAILED to read: {e!r}, marking as done")
            with engine.begin() as conn:
                conn.execute(text("INSERT INTO oa_import_log(file_path) VALUES(:fp)"), {'fp':fp})
            continue

        # build an address text
        gdf['address'] = gdf.apply(make_address_string, axis=1)
        gdf = gdf[gdf['address'].notna()]

        # extract street/city/state/zip columns
        gdf['street'] = gdf.apply(lambda r: find_field(r, STREET_FIELDS), axis=1)
        gdf['city']   = gdf.apply(lambda r: find_field(r, CITY_FIELDS),   axis=1)
        gdf['state']  = gdf.apply(lambda r: find_field(r, REGION_FIELDS), axis=1)
        gdf['zip']    = gdf.apply(lambda r: find_field(r, POSTCODE_FIELDS), axis=1)

        # rename geometry → 'geom'
        gdf = gdf.set_geometry('geometry').rename_geometry('geom')
        nonpt = gdf.geom.geom_type != 'Point'
        if nonpt.any():
            print(f"  converting {nonpt.sum()} non-Points to centroids")
            gdf.loc[nonpt, 'geom'] = gdf.loc[nonpt, 'geom'].centroid

        out = gdf[['address','street','city','state','zip','geom']]

        # drop any NULL or empty geometries
        out = out[out.geom.notnull()]
        out = out[[not geom.is_empty for geom in out.geom]]

        if out.empty:
            print("  → no valid address+geom, skipping")
            with engine.begin() as conn:
                conn.execute(text("INSERT INTO oa_import_log(file_path) VALUES(:fp)"), {'fp':fp})
            continue

        # count before
        with engine.connect() as conn:
            before = conn.execute(text(f"SELECT COUNT(*) FROM {args.table}")).scalar()

        # write
        out.to_postgis(
            args.table,
            engine,
            if_exists='append',
            index=False,
            dtype={'geom': Geometry('POINT', srid=4326)}
        )

        # count after
        with engine.connect() as conn:
            after = conn.execute(text(f"SELECT COUNT(*) FROM {args.table}")).scalar()

        inserted = after - before
        print(f"  inserted {inserted} rows")
        total_inserted += inserted

        # log it
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO oa_import_log(file_path) VALUES(:fp)"), {'fp':fp})

    print(f"Done → {total_inserted} total new rows added")


if __name__ == '__main__':
    main()