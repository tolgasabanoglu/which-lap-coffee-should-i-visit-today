# src/features/add_gee_data.py

import ee
import geopandas as gpd
from pathlib import Path
from datetime import datetime, timedelta
from shapely.geometry import Point

ee.Initialize()

INPUT_GPKG = Path("data/processed/lap_locations_historical_weather.gpkg")
OUTPUT_GPKG = Path("data/processed/lap_locations_env_daily.gpkg")

gdf = gpd.read_file(INPUT_GPKG, layer="lap_coffee")

minx, miny, maxx, maxy = gdf.total_bounds
bbox = ee.Geometry.Rectangle([minx, miny, maxx, maxy])

DATE_START = datetime.fromisoformat("2025-06-01")
DATE_END = datetime.fromisoformat("2025-10-24")

points_fc = ee.FeatureCollection([
    ee.Feature(ee.Geometry.Point([row.geometry.x, row.geometry.y]), {"name": row["name"]})
    for _, row in gdf.iterrows()
])

# Collections
lst_coll = ee.ImageCollection("MODIS/061/MOD11A1").select("LST_Day_1km").filterBounds(bbox)
no2_coll = ee.ImageCollection("COPERNICUS/S5P/NRTI/L3_NO2").select("NO2_column_number_density").filterBounds(bbox)
ndvi_coll = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")\
    .filterBounds(bbox)\
    .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 50))\
    .map(lambda img: img.normalizedDifference(["B8","B4"]).rename("NDVI"))

# Store daily results
all_records = []

dt = DATE_START
while dt <= DATE_END:
    date_str = dt.strftime("%Y-%m-%d")
    print(f"📅 Processing date: {date_str}")

    # Daily images
    lst_img = lst_coll.filterDate(date_str, date_str + "T23:59:59").median().multiply(0.02)
    no2_img = no2_coll.filterDate(date_str, date_str + "T23:59:59").median()
    ndvi_img = ndvi_coll.filterDate(date_str, date_str + "T23:59:59").median()

    # Only keep images with bands
    imgs = [(lst_img, "LST_C"), (no2_img, "NO2"), (ndvi_img, "NDVI")]

    for img, name in imgs:
        if img.bandNames().size().getInfo() == 0:
            continue

        sample = img.reduceRegions(collection=points_fc, reducer=ee.Reducer.first(), scale=1000 if name != "NDVI" else 10)
        features = sample.getInfo()["features"]

        for f in features:
            geom = f["geometry"]["coordinates"]
            props = f["properties"]
            all_records.append({
                "name": props["name"],
                "date": date_str,
                name: list(props.values())[-1],
                "geometry": Point(geom)
            })

    dt += timedelta(days=1)

# Merge all daily records into GeoDataFrame
gdf_out = gpd.GeoDataFrame(all_records, crs="EPSG:4326")
gdf_out.to_file(OUTPUT_GPKG, layer="daily_env", driver="GPKG")
print(f"\n✅ Saved daily environmental data to {OUTPUT_GPKG}")
