"""
Load GeoDataFrame of storm events into Postgres+PostGIS.
"""
import os
from sqlalchemy import create_engine
import geopandas as gpd

# DSN should be set in ENV: e.g. postgresql://user:pass@host:port/dbname
DATABASE_URL = os.getenv('DATABASE_URL')

def load_to_postgis(gdf: gpd.GeoDataFrame, table_name: str = 'storm_events'):
    engine = create_engine(DATABASE_URL)
    gdf.to_postgis(table_name, engine, if_exists='replace', index=False, dtype={
        'geom': 'Geometry(Polygon,4326)'
    })

if __name__ == '__main__':
    gdf = gpd.read_file('storm_events.geojson')
    load_to_postgis(gdf)
