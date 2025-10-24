# src/features/add_historical_weather.py

import geopandas as gpd
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta

# Input & output
INPUT_GPKG = Path("data/processed/lap_locations_env_open.gpkg")
OUTPUT_GPKG = Path("data/processed/lap_locations_historical_weather.gpkg")

# Load LAP Coffee GeoPackage
gdf = gpd.read_file(INPUT_GPKG, layer="lap_coffee")

# Function to determine season
def get_season(date):
    month = date.month
    if month in [3, 4, 5]:
        return "Spring"
    elif month in [6, 7, 8]:
        return "Summer"
    elif month in [9, 10, 11]:
        return "Autumn"
    else:
        return "Winter"

# Function to get historical weather from Open-Meteo
def get_historical_weather(lat, lon, start_date, end_date):
    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
        "&timezone=Europe/Berlin"
    )
    res = requests.get(url).json()
    daily = res.get("daily", {})
    if daily:
        records = []
        for i, date_str in enumerate(daily["time"]):
            records.append({
                "weather_date": date_str,
                "temp_max": daily["temperature_2m_max"][i],
                "temp_min": daily["temperature_2m_min"][i],
                "precip_mm": daily["precipitation_sum"][i]
            })
        return records
    return []

# Define start and end dates
START_DATE = "2025-06-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

# Collect all data
all_data = []

for i, row in gdf.iterrows():
    lat, lon = row.geometry.y, row.geometry.x
    weather_records = get_historical_weather(lat, lon, START_DATE, END_DATE)
    for record in weather_records:
        record.update({
            "name": row["name"],
            "address": row["address"],
            "lat": lat,
            "lon": lon,
            "rating": row.get("rating", None),
            "user_ratings_total": row.get("user_ratings_total", None),
            "elevation_m": row.get("elevation_m", None),
            "season": get_season(pd.to_datetime(record["weather_date"]))
        })
        all_data.append(record)
        print(f"{row['name']} {record['weather_date']} -> {record['temp_max']}°C")

# Convert to GeoDataFrame
df = pd.DataFrame(all_data)
gdf_final = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df.lon, df.lat),
    crs="EPSG:4326"
)

# Save to GeoPackage
gdf_final.to_file(OUTPUT_GPKG, layer="lap_coffee", driver="GPKG")
print(f"Saved historical weather GeoPackage to {OUTPUT_GPKG}")
