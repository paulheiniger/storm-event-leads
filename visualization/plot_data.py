#!/usr/bin/env python3
"""
plot_data.py

Query PostGIS and plot event points or polygons on an interactive map with filtering options.
Usage:
  python visualization/plot_data.py \
    --table storm_event_details \
    [--storm-id 43850] \
    [--event-type Hail] \
    [--start-date 2025-07-01] \
    [--end-date 2025-07-28] \
    [--bbox "-90,36,-85,39"] \
    [--output map.html]
"""
import os
import sys
import argparse
import geopandas as gpd
import folium
from sqlalchemy import create_engine


def build_query(table, storm_id=None, event_type=None, start_date=None, end_date=None, bbox=None):
    """
    Construct SQL for filtering the dataset.
    """
    where_clauses = []
    if storm_id:
        where_clauses.append(f"event_id = {int(storm_id)}")
    if event_type:
        where_clauses.append(f"event_type ILIKE '{event_type}'")
    if start_date:
        where_clauses.append(f"event_date >= '{start_date}'")
    if end_date:
        where_clauses.append(f"event_date <= '{end_date}'")
    if bbox:
        minLon, minLat, maxLon, maxLat = map(float, bbox.split(','))
        where_clauses.append(
            "ST_Intersects(geom, ST_MakeEnvelope(%f, %f, %f, %f, 4326))" % (minLon, minLat, maxLon, maxLat)
        )
    where_sql = ' AND '.join(where_clauses) if where_clauses else 'TRUE'
    sql = f"SELECT *, geometry FROM {table} WHERE {where_sql};"
    return sql


def query_data(database_url, table, **filters):
    engine = create_engine(database_url)
    sql = build_query(table, **filters)
    print(f"Executing SQL: {sql}")
    return gpd.read_postgis(sql, engine, geom_col='geom')


def plot_map(gdf, output_html):
    if gdf.empty:
        print("No records found for given filters.")
        return
    # Determine map center
    centroid = gdf.geometry.unary_union.centroid
    m = folium.Map(location=[centroid.y, centroid.x], zoom_start=6)

    # Add GeoJSON layer for polygons vs points
    geom_type = gdf.geom_type.iloc[0]
    if geom_type in ['Polygon', 'MultiPolygon']:
        folium.GeoJson(gdf).add_to(m)
    else:
        for _, row in gdf.iterrows():
            folium.CircleMarker(
                location=[row.geometry.y, row.geometry.x],
                radius=5,
                popup=str(row.get('event_type', '')),
                color='blue',
                fill=True
            ).add_to(m)

    m.save(output_html)
    print(f"Map saved to {output_html}")


def main():
    parser = argparse.ArgumentParser(description="Plot spatial data from PostGIS with filters.")
    parser.add_argument('--table', required=True, help='PostGIS table name')
    parser.add_argument('--storm-id', help='Filter by storm_event_id (integer)')
    parser.add_argument('--event-type', help='Filter by event_type (string)')
    parser.add_argument('--start-date', help='Filter by event_date >= YYYY-MM-DD')
    parser.add_argument('--end-date', help='Filter by event_date <= YYYY-MM-DD')
    parser.add_argument('--bbox', help='Bounding box minLon,minLat,maxLon,maxLat')
    parser.add_argument('--output', default='map.html', help='Output HTML file')
    args = parser.parse_args()

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        sys.exit('Error: DATABASE_URL env var not set')

    filters = {
        'storm_id': args.storm_id,
        'event_type': args.event_type,
        'start_date': args.start_date,
        'end_date': args.end_date,
        'bbox': args.bbox
    }

    gdf = query_data(database_url, args.table, **filters)
    plot_map(gdf, args.output)

if __name__ == '__main__':
    main()
