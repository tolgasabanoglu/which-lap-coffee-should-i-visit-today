import os
import requests
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point
from dotenv import load_dotenv

# -------------------------
# 1. Load API key
# -------------------------
load_dotenv()
API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

if not API_KEY:
    raise ValueError("❌ Google API key not found in .env")

# -------------------------
# 2. File paths
# -------------------------
INPUT_GPKG = Path("data/processed/lap_locations.gpkg")
OUTPUT_GPKG = Path("data/processed/lap_locations_with_parks.gpkg")

gdf = gpd.read_file(INPUT_GPKG, layer="lap_coffee")

# -------------------------
# 3. Function to fetch nearby parks
# -------------------------
def fetch_nearby_parks(lat, lon, radius=1000):
    """Fetch nearby parks using Google Places API."""
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": f"{lat},{lon}",
        "radius": radius,
        "keyword": "park",
        "type": "park",
        "key": API_KEY
    }
    res = requests.get(url, params=params).json()
    parks = []
    for place in res.get("results", []):
        parks.append({
            "name": place.get("name"),
            "lat": place["geometry"]["location"]["lat"],
            "lon": place["geometry"]["location"]["lng"]
        })
    return parks

# -------------------------
# 4. Compute nearest park per LAP Coffee
# -------------------------
nearest_parks = []

for i, row in gdf.iterrows():
    lat, lon = row.geometry.y, row.geometry.x
    parks = fetch_nearby_parks(lat, lon)
    
    if parks:
        park_points = [Point(p['lon'], p['lat']) for p in parks]
        distances = [Point(lon, lat).distance(p) for p in park_points]
        min_idx = distances.index(min(distances))
        nearest = parks[min_idx]
        distance_m = distances[min_idx] * 111_139  # meters per degree

        nearest_parks.append({
            "name": row["name"],
            "park_name": nearest["name"],
            "park_lat": nearest["lat"],
            "park_lon": nearest["lon"],
            "park_distance_m": distance_m
        })
    else:
        nearest_parks.append({
            "name": row["name"],
            "park_name": None,
            "park_lat": None,
            "park_lon": None,
            "park_distance_m": None
        })

# -------------------------
# 5. Merge and save
# -------------------------
parks_df = gpd.GeoDataFrame(nearest_parks)
gdf_final = gdf.merge(parks_df, on="name", how="left")

gdf_final.to_file(OUTPUT_GPKG, layer="lap_coffee", driver="GPKG")
print(f"✅ Saved GeoPackage with nearest parks: {OUTPUT_GPKG}")
