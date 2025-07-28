"""
Fetch storm events from NOAA API and save as GeoJSON or CSV.
"""
import requests
import geopandas as gpd

NOAA_API_URL = "https://www.ncei.noaa.gov/access/services/search/v1/data"


def fetch_events(start_date: str, end_date: str):
    params = {
        'dataset': 'storm_events',
        'startDate': start_date,
        'endDate': end_date,
        'format': 'json'
    }
    resp = requests.get(NOAA_API_URL, params=params)
    resp.raise_for_status()
    features = resp.json().get('features', [])
    gdf = gpd.GeoDataFrame.from_features(features)
    return gdf

if __name__ == '__main__':
    # Example usage
    gdf = fetch_events('2025-07-01', '2025-07-28')
    gdf.to_file('storm_events.geojson', driver='GeoJSON')
