import requests
import geopandas as gpd
from pathlib import Path
import time

# Paths
INPUT_GPKG = Path("data/processed/lap_locations.gpkg")
OUTPUT_GPKG = Path("data/processed/lap_locations_elevation.gpkg")

# Load points
gdf = gpd.read_file(INPUT_GPKG, layer="lap_coffee")

# Function to get elevation
def get_elevation(lat, lon):
    url = f"https://api.open-elevation.com/api/v1/lookup?locations={lat},{lon}"
    res = requests.get(url).json()
    return res['results'][0]['elevation']

# Fetch elevations
elevations = []
for i, row in gdf.iterrows():
    lat, lon = row.geometry.y, row.geometry.x
    elev = get_elevation(lat, lon)
    elevations.append(elev)
    print(f"{row['name']}: elevation = {elev} m")
    time.sleep(0.1)  # polite delay

gdf['elevation_m'] = elevations

# Save updated GeoPackage
gdf.to_file(OUTPUT_GPKG, layer="lap_coffee", driver="GPKG")
print(f"Saved GeoPackage with elevations to {OUTPUT_GPKG}")
