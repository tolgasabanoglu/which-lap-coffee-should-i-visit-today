import os
import requests
import geopandas as gpd
import pandas as pd
from pathlib import Path
from shapely.geometry import Point
from dotenv import load_dotenv
import time
import math
import sys
import json

# -------------------------
# 1. Configuration and API Key
# -------------------------
load_dotenv()
API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

if not API_KEY:
    sys.exit("❌ Error: Google API key not found in .env. Please check the .env file.")

# Define the radius for counting parks (in meters)
RADIUS_M = 500 # 1 kilometer radius

# -------------------------
# 2. File paths & Data Loading/Cleaning
# -------------------------
# NOTE: Ensure 'data/processed/lap_locations.gpkg' exists before running
INPUT_GPKG = Path("data/processed/lap_locations.gpkg")
# Updated output file name to reflect the new purpose (counting)
OUTPUT_GPKG = Path("data/processed/lap_locations_with_park_counts.gpkg") 

try:
    gdf = gpd.read_file(INPUT_GPKG, layer="lap_coffee")
except Exception as e:
    sys.exit(f"❌ Error reading input file {INPUT_GPKG}: {e}. Ensure the file exists.")

print(f"Loaded {len(gdf)} initial cafe locations.")

# --- DEDUPLICATION STEP (FIXED): Ensure only unique cafe coordinates are processed ---
initial_count = len(gdf)

# Create a temporary column with the WKT string for stable subset naming
gdf['_wkt_temp'] = gdf.geometry.to_wkt()
# Drop duplicates based on the temporary WKT column
gdf.drop_duplicates(subset=['_wkt_temp'], keep='first', inplace=True)
# Remove the temporary column immediately
gdf.drop(columns=['_wkt_temp'], inplace=True)

final_count = len(gdf)

if initial_count != final_count:
    print(f"⚠️ Removed {initial_count - final_count} duplicate cafe locations based on coordinates.")
else:
    print("✅ No duplicate cafe locations found based on coordinates.")

print(f"Processing {final_count} unique cafe locations.")
# -------------------------
# 3. Fetch nearby parks (Places API) - Updated to return all parks found
# -------------------------
def fetch_nearby_parks(lat, lon, radius=RADIUS_M):
    """Fetch all nearby parks within the specified radius using Google Places API."""
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    parks = []
    params = {
        "location": f"{lat},{lon}",
        "radius": radius,
        "type": "park",
        "key": API_KEY
    }

    # API returns up to 20 results per page, but allows token for next 2 pages.
    # Maximum total results that can be easily retrieved is around 60.
    page_limit = 3 
    page_count = 0
    
    while page_count < page_limit:
        res = requests.get(url, params=params).json()
        
        if res.get("status") not in ("OK", "ZERO_RESULTS"):
            print(f"❌ Places API Error: {res.get('status')} for location {lat}, {lon}")
            return []

        for place in res.get("results", []):
            # We only need the name here, but including location details can be useful for debugging
            parks.append({
                "name": place.get("name"),
            })
            
        if "next_page_token" in res:
            next_token = res["next_page_token"]
            # A pause is required when using the next_page_token
            time.sleep(2) 
            params = {"pagetoken": next_token, "key": API_KEY}
            page_count += 1
        else:
            break
            
    return parks

# -------------------------
# 4. Main loop to count parks for each café
# -------------------------
park_counts = []

for idx, row in gdf.iterrows():
    cafe_name = row["name"]
    cafe_lat, cafe_lon = row.geometry.y, row.geometry.x
    
    # Use the working Places API to find nearby parks
    parks_list = fetch_nearby_parks(cafe_lat, cafe_lon)
    
    park_count = len(parks_list)
    
    print(f"\n☕ Processing: {cafe_name} ({cafe_lat:.5f}, {cafe_lon:.5f})")
    print(f"   🌳 Found {park_count} parks within {RADIUS_M/1000} km radius.")

    park_counts.append({
        "name": cafe_name,
        "parks_count_1km": park_count,
    })

# -------------------------
# 5. Merge park counts into main GeoDataFrame and Save
# -------------------------
counts_df = pd.DataFrame(park_counts) 

# Merge using index to align the new count data correctly with the GeoDataFrame
gdf_final = gdf.copy()
# Ensure index is consistent for join
counts_df.set_index(gdf_final.index, inplace=True)

# Drop old columns (if they exist from a previous run) and join the new count column
columns_to_drop = ['park_name', 'park_lat', 'park_lon', 'distance_to_park_m', 'park_geometry']
for col in columns_to_drop:
    if col in gdf_final.columns:
        gdf_final = gdf_final.drop(columns=[col])

# Join the new parks count
gdf_final = gdf_final.join(counts_df[['parks_count_1km']])

# -------------------------
# 6. Save GeoPackage
# -------------------------
gdf_final.to_file(OUTPUT_GPKG, layer="lap_coffee", driver="GPKG")
print(f"\n✅ Saved GeoPackage with park counts ({RADIUS_M/1000}km radius): {OUTPUT_GPKG}")
