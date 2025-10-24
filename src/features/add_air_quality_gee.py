# src/features/add_air_quality_gee.py

import ee
import geopandas as gpd
from pathlib import Path
from datetime import datetime

# Initialize Earth Engine
ee.Initialize()

# Input / output
INPUT_GPKG = Path("data/processed/lap_locations_env_ndvi.gpkg")
OUTPUT_GPKG = Path("data/processed/lap_locations_env_airquality.gpkg")

# Load LAP Coffee GeoPackage
gdf = gpd.read_file(INPUT_GPKG, layer="lap_coffee")

# Create bounding box around all points
minx, miny, maxx, maxy = gdf.total_bounds
bbox = ee.Geometry.Rectangle([minx, miny, maxx, maxy])

# Filter Sentinel-5P NO2 image collection
collection = (
    ee.ImageCollection("COPERNICUS/S5P/NRTI/L3_NO2")
    .filterBounds(bbox)
    .filterDate("2025-10-01", "2025-10-24")  # last 3 weeks
)

# Check collection size
num_images = collection.size().getInfo()
if num_images == 0:
    raise ValueError("No NO2 images found for this date range.")
print(f"Number of NO2 images in collection: {num_images}")

# Median image to reduce cloud/noise
median_no2 = collection.median().select("NO2_column_number_density")

# Function to get NO2 value at a point
def get_no2(lat, lon):
    point = ee.Geometry.Point([lon, lat])
    value = median_no2.sample(point, scale=1000).first()  # 1 km resolution
    if value is not None:
        return value.get("NO2_column_number_density").getInfo()
    return None

# Extract NO2 for all points
no2_values = []
for i, row in gdf.iterrows():
    lat, lon = row.geometry.y, row.geometry.x
    no2 = get_no2(lat, lon)
    no2_values.append(no2)
    print(f"{row['name']}: NO2 = {no2}")

# Add NO2 and retrieval date to GeoDataFrame
gdf["no2"] = no2_values
gdf["no2_date"] = datetime.today().strftime("%Y-%m-%d")

# Save updated GeoPackage
gdf.to_file(OUTPUT_GPKG, layer="lap_coffee", driver="GPKG")
print(f"Saved GeoPackage with NO2 to {OUTPUT_GPKG}")
