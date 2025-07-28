"""
Plot storm event boundary using Folium.
"""
import folium
import geopandas as gpd


def plot_event(geojson_path: str, output_html: str = 'map.html'):
    gdf = gpd.read_file(geojson_path)
    centroid = gdf.geometry.unary_union.centroid
    m = folium.Map(location=[centroid.y, centroid.x], zoom_start=6)
    folium.GeoJson(gdf).add_to(m)
    m.save(output_html)

if __name__ == '__main__':
    plot_event('../storm_events.geojson')
