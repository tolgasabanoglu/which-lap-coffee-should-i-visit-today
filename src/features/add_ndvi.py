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
    ee.Authenticate()
    ee.Initialize()

# -------------------------
# 2. Input / Output
# -------------------------
INPUT_GPKG = Path("data/processed/lap_locations.gpkg")
OUTPUT_GPKG = Path("data/processed/lap_locations_ndvi_daily.gpkg")

gdf = gpd.read_file(INPUT_GPKG, layer="lap_coffee")

START_DATE = "2025-01-01"
END_DATE = "2025-10-24"

# -------------------------
# 3. Helper: calculate NDVI for a point and day
# -------------------------
def get_daily_ndvi(lat, lon, date):
    start = ee.Date(date)
    end = start.advance(1, 'day')

    collection = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                  .filterDate(start, end)
                  .filterBounds(ee.Geometry.Point([lon, lat]))
                  .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20)))

    def calc_ndvi(img):
        ndvi = img.normalizedDifference(['B8', 'B4']).rename('NDVI')
        return ndvi.copyProperties(img, img.propertyNames())

    ndvi_collection = collection.map(calc_ndvi)
    mean_ndvi_img = ndvi_collection.mean()

    point = ee.Geometry.Point([lon, lat])
    try:
        val = mean_ndvi_img.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=point,
            scale=10
        ).getInfo()
        return val.get('NDVI')
    except Exception:
        return None

# -------------------------
# 4. Collect daily NDVI for all cafés
# -------------------------
all_data = []
dates = pd.date_range(START_DATE, END_DATE)

for i, row in gdf.iterrows():
    lat, lon = row.geometry.y, row.geometry.x
    print(f"📍 {row['name']} ({lat:.5f}, {lon:.5f})")

    for d in dates:
        ndvi_val = get_daily_ndvi(lat, lon, d.strftime('%Y-%m-%d'))
        all_data.append({
            "name": row["name"],
            "address": row["address"],
            "lat": lat,
            "lon": lon,
            "date": d.strftime('%Y-%m-%d'),
            "ndvi": ndvi_val
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

gdf_out.to_file(OUTPUT_GPKG, layer="lap_coffee", driver="GPKG")
print(f"✅ Saved daily NDVI GeoPackage to {OUTPUT_GPKG}")
