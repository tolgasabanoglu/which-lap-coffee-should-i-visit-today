import ee
import geopandas as gpd
import pandas as pd
from pathlib import Path

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
OUTPUT_GPKG = Path("data/processed/lap_locations_nightlights_daily.gpkg")

gdf = gpd.read_file(INPUT_GPKG, layer="lap_coffee")

START_DATE = "2025-01-01"
END_DATE = "2025-10-24"

# -------------------------
# 3. Helper: get monthly nightlights
# -------------------------
def get_monthly_nightlights(lat, lon, date):
    dt = pd.to_datetime(date)
    month_start = dt.replace(day=1)
    month_end = (month_start + pd.offsets.MonthEnd(1))

    collection = (ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG")
                  .filterDate(month_start.strftime('%Y-%m-%d'), month_end.strftime('%Y-%m-%d'))
                  .filterBounds(ee.Geometry.Point([lon, lat])))

    mean_img = collection.mean()
    point = ee.Geometry.Point([lon, lat])

    try:
        val = mean_img.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=point,
            scale=500
        ).getInfo()
        return val.get('avg_rad')
    except Exception:
        return None

# -------------------------
# 4. Collect nightlights for all cafés
# -------------------------
all_data = []
dates = pd.date_range(START_DATE, END_DATE)

for i, row in gdf.iterrows():
    lat, lon = row.geometry.y, row.geometry.x
    print(f"📍 {row['name']} ({lat:.5f}, {lon:.5f})")

    for d in dates:
        nl_val = get_monthly_nightlights(lat, lon, d)
        all_data.append({
            "name": row["name"],
            "address": row["address"],
            "lat": lat,
            "lon": lon,
            "date": d.strftime('%Y-%m-%d'),
            "nightlight": nl_val
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
print(f"✅ Saved daily nightlight GeoPackage to {OUTPUT_GPKG}")
