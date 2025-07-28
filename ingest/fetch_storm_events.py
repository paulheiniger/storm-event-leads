"""
Fetch storm events: first try NOAA Search API, fallback to bulk shapefile download if API fails.
"""
import os
import requests
import tempfile
import zipfile
import geopandas as gpd
from shapely.geometry import Point
from datetime import datetime
from urllib.parse import urljoin
from urllib.request import urlretrieve

NOAA_API_URL = "https://www.ncei.noaa.gov/access/services/search/v1/data"
SHAPEFILE_BASE = (
    "https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/shapefiles/{year}/"  # unzip structure
)
SHAPEFILE_NAME = "StormEvents_details-ftp_v1.0_d{year}_c20210412.zip"


def fetch_events_api(start_date: str, end_date: str, bbox: str = None) -> gpd.GeoDataFrame:
    params = {
        'dataset': 'storm_events',
        'startDate': start_date,
        'endDate': end_date,
        'format': 'json'
    }
    if bbox:
        params['bbox'] = bbox  # 'minLon,minLat,maxLon,maxLat'

    resp = requests.get(NOAA_API_URL, params=params)
    resp.raise_for_status()
    features = resp.json().get('features', [])
    return gpd.GeoDataFrame.from_features(features)


def fetch_events_shapefile(year: int) -> gpd.GeoDataFrame:
    url = urljoin(
        SHAPEFILE_BASE.format(year=year),
        SHAPEFILE_NAME.format(year=year)
    )
    tmp = tempfile.mkdtemp(prefix="storm_shp_")
    zip_path = os.path.join(tmp, os.path.basename(url))
    print(f"Downloading shapefile {url}")
    urlretrieve(url, zip_path)
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(tmp)
    # Find .shp file
    shp_files = [f for f in os.listdir(tmp) if f.endswith('.shp')]
    if not shp_files:
        raise FileNotFoundError("No shapefile found in downloaded archive")
    shp_path = os.path.join(tmp, shp_files[0])
    gdf = gpd.read_file(shp_path)
    return gdf


def filter_date_range(gdf: gpd.GeoDataFrame, start_date: str, end_date: str) -> gpd.GeoDataFrame:
    # Dataset columns BEGIN_DATE_TIME, END_DATE_TIME
    fmt = "%Y-%m-%dT%H:%M:%S"
    gdf['begin'] = gdf['BEGIN_DATE_TIME'].apply(lambda dt: datetime.strptime(dt[:19], fmt))
    gdf['end'] = gdf['END_DATE_TIME'].apply(lambda dt: datetime.strptime(dt[:19], fmt))
    start = datetime.fromisoformat(start_date)
    end = datetime.fromisoformat(end_date)
    return gdf[(gdf['begin'] >= start) & (gdf['end'] <= end)]


def fetch_events(start_date: str, end_date: str, bbox: str = None) -> gpd.GeoDataFrame:
    try:
        print("Trying NOAA Search API...")
        gdf = fetch_events_api(start_date, end_date, bbox)
        if gdf.empty:
            print("API returned no events, falling back to shapefile")
            raise ValueError("No data from API")
        return gdf
    except Exception as e:
        print(f"API error: {e}")
        year = int(start_date.split('-')[0])
        gdf = fetch_events_shapefile(year)
        return filter_date_range(gdf, start_date, end_date)


if __name__ == '__main__':
    # Example usage
    gdf = fetch_events('2025-07-01', '2025-07-28', bbox=None)
    print(gdf.shape)
    gdf.to_file('storm_events.geojson', driver='GeoJSON')
