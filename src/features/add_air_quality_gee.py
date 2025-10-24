# src/features/add_air_quality_gee.py
import ee
import geopandas as gpd
from pathlib import Path
from datetime import datetime

# Initialize Earth Engine
ee.Initialize()

# === Input / Output paths ===
INPUT_GPKG = Path("data/processed/lap_locations_env_ndvi.gpkg")
OUTPUT_GPKG = Path("data/processed/lap_locations_env_airquality.gpkg")

# === Load LAP Coffee points ===
gdf = gpd.read_file(INPUT_GPKG, layer="lap_coffee")

# === Bounding box around all points ===
minx, miny, maxx, maxy = gdf.total_bounds
bbox = ee.Geometry.Rectangle([minx, miny, maxx, maxy])

# === Date range ===
DATE_START = "2025-10-01"
DATE_END = "2025-10-24"

# ---------------------------------------------------------------------------
# Sentinel-5P NO₂ collection
# ---------------------------------------------------------------------------
collection = (
    ee.ImageCollection("COPERNICUS/S5P/NRTI/L3_NO2")
    .filterBounds(bbox)
    .filterDate(DATE_START, DATE_END)
    .select("NO2_column_number_density")
)

num_images = collection.size().getInfo()
if num_images == 0:
    raise ValueError("No Sentinel-5P NO₂ images found in this date range.")
print(f"🌍 Total NO₂ images: {num_images}")

# Convert points to EE FeatureCollection
points_fc = ee.FeatureCollection([
    ee.Feature(ee.Geometry.Point([row.geometry.x, row.geometry.y]), {"name": row["name"]})
    for _, row in gdf.iterrows()
])

# ---------------------------------------------------------------------------
# Extract NO₂ for each image at all points
# ---------------------------------------------------------------------------
records = []
image_list = collection.toList(num_images)

for i in range(num_images):
    img = ee.Image(image_list.get(i))
    date_str = datetime.utcfromtimestamp(img.date().millis().getInfo() / 1000).strftime("%Y-%m-%d")
    print(f"\n📅 Processing image {i+1}/{num_images} ({date_str})")

    # Extract NO₂ values for all points at once
    sample_fc = img.reduceRegions(
        collection=points_fc,
        reducer=ee.Reducer.first(),
        scale=1000  # Sentinel-5P ~1 km resolution
    )

    # Loop over features in the sample
    sample_list = sample_fc.getInfo()["features"]
    for feat in sample_list:
        no2_val = feat["properties"].get("NO2_column_number_density")  # returns None if missing
        records.append({
            "name": feat["properties"]["name"],
            "date": date_str,
            "no2": no2_val,
            "geometry": gdf.loc[gdf["name"] == feat["properties"]["name"], "geometry"].values[0]
        })
        print(f"  {feat['properties']['name']}: NO₂ = {no2_val}")

# ---------------------------------------------------------------------------
# Save results as GeoDataFrame
# ---------------------------------------------------------------------------
no2_gdf = gpd.GeoDataFrame(records, crs=gdf.crs)
no2_gdf.to_file(OUTPUT_GPKG, layer="lap_coffee_no2_timeseries", driver="GPKG")

print(f"\n✅ Saved daily NO₂ time series to {OUTPUT_GPKG}")
