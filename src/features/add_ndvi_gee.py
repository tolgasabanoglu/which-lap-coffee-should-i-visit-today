# src/features/add_ndvi_gee.py

import ee
import geopandas as gpd
from pathlib import Path
from datetime import datetime

# Initialize Earth Engine
ee.Initialize()

# Input / output
INPUT_GPKG = Path("data/processed/lap_locations_env_weather.gpkg")
OUTPUT_GPKG = Path("data/processed/lap_locations_env_ndvi.gpkg")

# Load LAP Coffee GeoPackage
gdf = gpd.read_file(INPUT_GPKG, layer="lap_coffee")

# Create a bounding box around all points (slightly expanded)
minx, miny, maxx, maxy = gdf.total_bounds
bbox = ee.Geometry.Rectangle([minx, miny, maxx, maxy])

# Filter Sentinel-2 SR Harmonized collection
collection = (
    ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
    .filterBounds(bbox)
    .filterDate("2025-10-01", "2025-10-24")  # last 3 months
    .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 50))  # allow some clouds
)

# Check collection size
num_images = collection.size().getInfo()
if num_images == 0:
    raise ValueError("No images found in the specified date range and cloud filter.")
print(f"Number of images in collection: {num_images}")

# Take median image
median_image = collection.median()

# Get the latest image date (for reference)
dates = collection.aggregate_array('system:time_start').getInfo()
latest_date = datetime.utcfromtimestamp(dates[-1] / 1000).strftime('%Y-%m-%d')
print(f"NDVI date (latest image in collection): {latest_date}")

# Compute NDVI
ndvi_image = median_image.normalizedDifference(["B8", "B4"]).rename("NDVI")

# Function to extract NDVI at a point
def get_ndvi(lat, lon):
    point = ee.Geometry.Point([lon, lat])
    value = ndvi_image.sample(point, scale=10).first()
    if value is not None:
        return value.get("NDVI").getInfo()
    return None

# Extract NDVI for all LAP Coffee points
ndvi_values = []
for i, row in gdf.iterrows():
    lat, lon = row.geometry.y, row.geometry.x
    ndvi = get_ndvi(lat, lon)
    ndvi_values.append(ndvi)
    print(f"{row['name']}: NDVI = {ndvi}")

# Add NDVI and NDVI date to GeoDataFrame
gdf["ndvi"] = ndvi_values
gdf["ndvi_date"] = latest_date

# Save updated GeoPackage
gdf.to_file(OUTPUT_GPKG, layer="lap_coffee", driver="GPKG")
print(f"Saved GeoPackage with NDVI to {OUTPUT_GPKG}")
