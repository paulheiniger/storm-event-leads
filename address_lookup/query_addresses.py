"""
Spatial join: find addresses within a storm event boundary.
"""
import os
import geopandas as gpd
from sqlalchemy import create_engine

DATABASE_URL = os.getenv('DATABASE_URL')

def get_addresses_in_event(storm_table: str = 'storm_events', address_table: str = 'addresses'):
    engine = create_engine(DATABASE_URL)
    sql = f"""
    SELECT a.*
    FROM {address_table} a
    JOIN {storm_table} s
      ON ST_Contains(s.geom, a.geom)
    """
    return gpd.read_postgis(sql, engine, geom_col='geom')

if __name__ == '__main__':
    df = get_addresses_in_event()
    df.to_file('addresses_in_event.geojson', driver='GeoJSON')
