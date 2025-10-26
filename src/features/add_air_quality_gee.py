import ee
import geopandas as gpd
import pandas as pd
from pathlib import Path
from datetime import timedelta, datetime

# -------------------------
# 1. Initialize GEE
# -------------------------
try:
    ee.Initialize()
except Exception:
    # Assuming ee.Authenticate() and ee.Initialize() logic is already handled
    # ee.Authenticate()
    ee.Initialize()
    print("Earth Engine initialized successfully.")

# -------------------------
# 2. Input / Output (Adjusted output name)
# -------------------------
INPUT_GPKG = Path("data/processed/lap_locations.gpkg")
OUTPUT_GPKG = Path("data/processed/lap_locations_pm25_daily.gpkg")

# Assuming 'lap_coffee' layer exists and contains point geometries
gdf = gpd.read_file(INPUT_GPKG, layer="lap_coffee")

# Deduplicate by address before processing to prevent redundant API calls and data duplication
print(f"Loaded {gdf.shape[0]} cafe locations. Deduplicating by address...")
gdf.drop_duplicates(subset=['address'], keep='first', inplace=True)
print(f"Processing {gdf.shape[0]} unique cafe locations after deduplication.")


START_DATE = "2025-01-01"
END_DATE = "2025-10-24"

# ----------------------------------------------------
# 3. Helper: calculate PM2.5 for a point and day
#    Uses temporal smoothing (a window) to mitigate cloud cover gaps.
# ----------------------------------------------------
def get_daily_pm25(lat, lon, date_str, temporal_window_days=3):
    """
    Fetches MODIS AOD data as a proxy for PM2.5, using a temporal window
    to average data around the target date to fill cloud-related gaps.
    """
    AOD_BAND = 'Optical_Depth_055'
    SCALE_FACTOR = 0.001

    target_date = ee.Date(date_str)
    
    # Define the temporal window: [target_date - window, target_date + window + 1 day]
    start_date_window = target_date.advance(-temporal_window_days, 'day')
    end_date_window = target_date.advance(temporal_window_days + 1, 'day')

    # Filter the AOD collection over the time and spatial window
    collection = (ee.ImageCollection("MODIS/061/MCD19A2_GRANULES")
                  .filterDate(start_date_window, end_date_window)
                  .filterBounds(ee.Geometry.Point([lon, lat]))
                 )

    # Select the AOD band, apply the scale factor, and rename
    def process_aod(img):
        # FIX: Use updateMask() to correctly mask out invalid data (AOD value of -9999).
        # This creates a mask where the value is NOT -9999, applies it, and then scales.
        valid_mask = img.select(AOD_BAND).neq(-9999)
        aod = img.select(AOD_BAND).updateMask(valid_mask).multiply(SCALE_FACTOR).rename('AOD')
        return aod.copyProperties(img, ['system:time_start'])

    aod_collection = collection.map(process_aod)
    
    # Calculate the mean AOD over the temporal window to fill gaps
    mean_aod_img = aod_collection.mean()

    point = ee.Geometry.Point([lon, lat])
    
    # NOTE: MODIS AOD resolution is 10000m (10km), so we set the scale to 10000.
    try:
        val = mean_aod_img.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=point,
            scale=10000 # Use the sensor's native resolution for accuracy
        ).getInfo()
        
        return val.get('AOD')
    except Exception as e:
        # print(f"Error fetching AOD for {date_str}: {e}")
        return None

# -------------------------
# 4. Collect daily PM2.5 (AOD) for all cafés
# -------------------------
all_data = []
dates = pd.date_range(START_DATE, END_DATE)

for i, row in gdf.iterrows():
    lat, lon = row.geometry.y, row.geometry.x
    print(f"📍 {row['name']} ({lat:.5f}, {lon:.5f}) - Processing {len(dates)} days...")

    for d in dates:
        # Call the function with the 3-day window for gap-filling
        pm25_proxy_val = get_daily_pm25(lat, lon, d.strftime('%Y-%m-%d'), temporal_window_days=3)
        all_data.append({
            "name": row["name"],
            "address": row["address"],
            "lat": lat,
            "lon": lon,
            "date": d.strftime('%Y-%m-%d'),
            "pm25_aod_proxy": pm25_proxy_val
        })

# -------------------------
# 5. Convert to GeoDataFrame and save
# -------------------------
df = pd.DataFrame(all_data)
gdf_out = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df.lon, df.lat),
    crs="EPSG:4326"
)

# Save to new output file
gdf_out.to_file(OUTPUT_GPKG, layer="lap_coffee", driver="GPKG")
print(f"✅ Saved gap-filled daily PM2.5 proxy (AOD) GeoPackage to {OUTPUT_GPKG}")
