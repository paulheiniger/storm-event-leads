import geopandas as gpd
from shapely.geometry import Point

# 1. Load OpenAddresses CSV for your region
oa = gpd.read_csv("openaddresses/usa/ga.csv")          # ~1–2GB file
oa["geometry"] = oa.apply(lambda r: Point(r.longitude, r.latitude), axis=1)
oa = gpd.GeoDataFrame(oa, geometry="geometry", crs="EPSG:4326")

# 2. Load your storm polygon
storm = gpd.read_file("storm_events.geojson")
poly = storm.geometry.unary_union

# 3. Filter
within = oa[oa.within(poly)]
print(f"Addresses in storm: {len(within)}")