import os
import requests
import geopandas as gpd
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import sys
import time
import json 

# Define the radius for searching (in meters)
RADIUS_M = 500 

# -------------------------
# 1. Configuration and API Key
# -------------------------
load_dotenv()
API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

if not API_KEY:
    sys.exit("❌ Error: Google API key not found in .env. Please check the .env file.")

# -------------------------
# 2. File paths & Data Loading/Cleaning
# -------------------------
INPUT_GPKG = Path("data/processed/lap_locations.gpkg")
# New output file name reflecting the amenity focus
OUTPUT_GPKG = Path("data/processed/lap_locations_with_open_bars.gpkg")

try:
    gdf = gpd.read_file(INPUT_GPKG, layer="lap_coffee")
except Exception as e:
    sys.exit(f"❌ Error reading input file {INPUT_GPKG}: {e}. Ensure the file exists.")

print(f"Loaded {len(gdf)} initial cafe locations.")

# --- DEDUPLICATION STEP 1: By Coordinates (Standard cleaning) ---
initial_coord_count = len(gdf)
gdf['_wkt_temp'] = gdf.geometry.to_wkt()
gdf.drop_duplicates(subset=['_wkt_temp'], keep='first', inplace=True)
gdf.drop(columns=['_wkt_temp'], inplace=True)
mid_count = len(gdf)

if initial_coord_count != mid_count:
    print(f"⚠️ Removed {initial_coord_count - mid_count} duplicate cafe locations based on coordinates.")

# --- DEDUPLICATION STEP 2 (FIX): By Address (To ensure 1 row per unique physical location, as requested) ---
initial_address_count = len(gdf)
gdf.drop_duplicates(subset=['address'], keep='first', inplace=True)
final_count = len(gdf)

if initial_address_count != final_count:
    print(f"⚠️ Removed {initial_address_count - final_count} rows to ensure only one entry per unique cafe address.")
else:
    print("✅ Input cafes already have unique addresses.")

print(f"Processing {final_count} unique cafe locations.")


# -------------------------
# 3. Function to fetch nearby open bars
# -------------------------
def fetch_nearby_open_bars(lat, lon, radius=RADIUS_M):
    """
    Fetch all nearby bars/pubs that are currently open within the specified radius.
    NOTE: Uses 'opennow=true' which returns bars open at the time the script is executed.
    """
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": f"{lat},{lon}",
        "radius": radius,
        "type": "bar",
        "opennow": "true", # Filters results to only show those currently open
        "key": API_KEY
    }
    
    # Places API supports max ~60 results via pagination (3 pages)
    all_bars = []
    page_limit = 3
    page_count = 0

    while page_count < page_limit:
        res = requests.get(url, params=params).json()
        
        if res.get("status") not in ("OK", "ZERO_RESULTS"):
            print(f"❌ Places API Error: {res.get('status')} for location {lat}, {lon}")
            return []

        for place in res.get("results", []):
            all_bars.append({
                "name": place.get("name"),
            })
            
        if "next_page_token" in res and page_count < page_limit - 1:
            next_token = res["next_page_token"]
            time.sleep(2) # Pause required when using the next_page_token
            params = {"pagetoken": next_token, "key": API_KEY}
            page_count += 1
        else:
            break
            
    return all_bars

# -------------------------
# 4. Main loop to count open bars
# -------------------------
# Initialize a list to hold the calculated counts, indexed by the original unique GeoDataFrame index
bar_counts_list = [] 
indexes_processed = []

for i, row in gdf.iterrows():
    cafe_name = row["name"]
    cafe_lat, cafe_lon = row.geometry.y, row.geometry.x
    
    print(f"\n☕ Processing: {cafe_name} ({cafe_lat:.5f}, {cafe_lon:.5f})")

    # Fetch all open bars in the 500m radius
    open_bars_list = fetch_nearby_open_bars(cafe_lat, cafe_lon)
    
    # Total count for the density metric
    open_bars_count_500m = len(open_bars_list)
    
    bar_counts_list.append(open_bars_count_500m)
    indexes_processed.append(i)
    
    print(f"   🍺 Total Open Bars/Pubs in 500m: {open_bars_count_500m}.")

# -------------------------
# 5. Add bar counts to the deduplicated GeoDataFrame and Save
# -------------------------

# Create a Series from the counts, indexed by the rows that were processed
counts_series = pd.Series(bar_counts_list, index=indexes_processed, name="open_bars_count_500m")

# Clean up old amenity columns (if they existed from a previous run)
columns_to_drop = [
    'nearest_toilet_name', 'nearest_toilet_lat', 'nearest_toilet_lon', 
    'toilet_distance_m', 'toilets_count_500m', 
    'park_name', 'park_lat', 'park_lon', 'distance_to_park_m', 'parks_count_1km',
    'open_bars_count_500m' # drop existing column if present to ensure clean assignment
]
for col in columns_to_drop:
    if col in gdf.columns:
        gdf = gdf.drop(columns=[col])

# Add the new count column directly to the GeoDataFrame
gdf['open_bars_count_500m'] = counts_series

# -------------------------
# 6. Save updated GeoPackage
# -------------------------
gdf.to_file(OUTPUT_GPKG, layer="lap_coffee", driver="GPKG")
print(f"\n✅ Saved GeoPackage with open bar counts to: {OUTPUT_GPKG}")
