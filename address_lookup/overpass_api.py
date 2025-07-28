import overpy
from shapely.geometry import Polygon

# 1. Load your storm polygon (e.g. from GeoJSON)
import geopandas as gpd
storm_gdf = gpd.read_file("storm_events.geojson")
storm_poly: Polygon = storm_gdf.geometry.unary_union

# 2. Build a “poly” string for the Overpass query
poly_str = " ".join(f"{y} {x}" for x, y in storm_poly.exterior.coords)

# 3. Query Overpass
api = overpy.Overpass()
query = f"""
  node
    ["addr:housenumber"]
    (poly:"{poly_str}");
  out body;
"""
result = api.query(query)

# 4. Extract addresses
addresses = []
for node in result.nodes:
    tags = node.tags
    num = tags.get("addr:housenumber")
    street = tags.get("addr:street")
    city = tags.get("addr:city", "")
    state = tags.get("addr:state", "")
    pc = tags.get("addr:postcode", "")
    addr = f"{num} {street}, {city}, {state} {pc}".strip(", ")
    addresses.append({
        "address": addr,
        "lat": node.lat,
        "lon": node.lon
    })

print(f"Found {len(addresses)} OSM addresses in boundary")