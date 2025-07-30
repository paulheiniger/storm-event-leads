#!/usr/bin/env python3
"""
query_addresses.py

Spatial join: find addresses within a storm event boundary and export results.
Usage:
  python address_lookup/query_addresses.py \
    --storm-table storm_event_boundaries \
    --address-table addresses \
    [--event-id 43850] \
    [--output addresses_in_event.geojson]
"""
import os
import argparse
import geopandas as gpd
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv('DATABASE_URL')


def get_addresses_in_event(storm_table: str, address_table: str, event_id: int = None) -> gpd.GeoDataFrame:
    """
    Fetch all addresses whose points lie within the geometry of the specified storm event(s).
    If event_id is provided, only that event; otherwise all events in storm_table.
    """
    if not DATABASE_URL:
        raise RuntimeError('DATABASE_URL must be set')
    engine = create_engine(DATABASE_URL)
    # Base SQL
    sql = f"""
    SELECT a.*
    FROM {address_table} a
    JOIN {storm_table} s
      ON ST_Contains(s.geom, a.geometry)
    """
    # Optional filter by event_id
    if event_id is not None:
        sql += f" WHERE s.id = :eid"
        params = {'eid': event_id}
    else:
        params = {}

    return gpd.read_postgis(text(sql), engine, geom_col='geometry', params=params)


def main():
    parser = argparse.ArgumentParser(description='Query addresses within storm event boundaries')
    parser.add_argument('--storm-table', default='storm_event_boundaries',
                        help='PostGIS table name containing storm event geometries')
    parser.add_argument('--address-table', default='addresses',
                        help='PostGIS table name containing address point geometries')
    parser.add_argument('--event-id', type=int,
                        help='Specific storm_event id to filter on')
    parser.add_argument('--output', default='addresses_in_event.geojson',
                        help='Output GeoJSON file')
    args = parser.parse_args()

    gdf = get_addresses_in_event(
        storm_table=args.storm_table,
        address_table=args.address_table,
        event_id=args.event_id
    )
    if gdf.empty:
        print('No addresses found for given criteria.')
    else:
        gdf.to_file(args.output, driver='GeoJSON')
        print(f"Wrote {len(gdf)} addresses to {args.output}")

if __name__ == '__main__':
    main()