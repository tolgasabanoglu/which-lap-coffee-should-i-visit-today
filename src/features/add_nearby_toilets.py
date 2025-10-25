import os
import requests
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# -------------------------
# 1. Load environment
# -------------------------
from dotenv import load_dotenv
load_dotenv()
API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

if not API_KEY:
    raise ValueError("Google API key not found in .env")

# -------------------------
# 2. Input / Output
# -------------------------
INPUT_GPKG = Path("data/processed/lap_locations.gpkg")
OUTPUT_GPKG = Path("data/processed/lap_locations_with_toilets.gpkg")

gdf = gpd.read_file(INPUT_GPKG, layer="lap_coffee")

# -------------------------
# 3. Function to fetch nearby toilets
# -------------------------
def fetch_nearby_toilets(lat, lon, radius=500):
    """Fetch nearby public toilets within radius (meters)"""
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": f"{lat},{lon}",
        "radius": radius,
        "keyword": "public toilet",
        "key": API_KEY
    }
    res = requests.get(url, params=params).json()
    toilets = []
    for place in res.get("results", []):
        toilets.append({
            "name": place.get("name"),
            "lat": place["geometry"]["location"]["lat"],
            "lon": place["geometry"]["location"]["lng"]
        })
    return toilets

# -------------------------
# 4. Collect nearest toilet per LAP Coffee
# -------------------------
nearest_toilets = []

for i, row in gdf.iterrows():
    lat, lon = row.geometry.y, row.geometry.x
    toilets = fetch_nearby_toilets(lat, lon)
    
    if toilets:
        # Find closest toilet
        toilet_points = [Point(t['lon'], t['lat']) for t in toilets]
        distances = [Point(lon, lat).distance(p) for p in toilet_points]
        min_idx = distances.index(min(distances))
        nearest = toilets[min_idx]
        distance_m = distances[min_idx] * 111_139  # approx meters per degree
        nearest_toilets.append({
            "name": row["name"],
            "toilet_name": nearest["name"],
            "toilet_lat": nearest["lat"],
            "toilet_lon": nearest["lon"],
            "toilet_distance_m": distance_m
        })
    else:
        nearest_toilets.append({
            "name": row["name"],
            "toilet_name": None,
            "toilet_lat": None,
            "toilet_lon": None,
            "toilet_distance_m": None
        })

# -------------------------
# 5. Merge toilet info with original GeoDataFrame
# -------------------------
toilets_df = gpd.GeoDataFrame(nearest_toilets)
gdf_final = gdf.merge(toilets_df, on="name", how="left")

# -------------------------
# 6. Save updated GeoPackage
# -------------------------
gdf_final.to_file(OUTPUT_GPKG, layer="lap_coffee", driver="GPKG")
print(f"✅ Saved GeoPackage with nearest toilets to: {OUTPUT_GPKG}")
