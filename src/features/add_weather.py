# src/features/add_weather.py

import geopandas as gpd
import requests
from pathlib import Path

# Input & output
INPUT_GPKG = Path("data/processed/lap_locations_env_open.gpkg")  # GeoPackage with elevation
OUTPUT_GPKG = Path("data/processed/lap_locations_env_weather.gpkg")  # Output GeoPackage

# Load LAP Coffee GeoPackage
gdf = gpd.read_file(INPUT_GPKG, layer="lap_coffee")

# Function to get daily weather from Open-Meteo
def get_weather(lat, lon):
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
        f"&timezone=Europe/Berlin"
    )
    res = requests.get(url).json()
    daily = res.get("daily", {})
    if daily:
        return {
            "weather_date": daily["time"][0],  # Date of the forecast (YYYY-MM-DD)
            "temp_max": daily["temperature_2m_max"][0],
            "temp_min": daily["temperature_2m_min"][0],
            "precip_mm": daily["precipitation_sum"][0]
        }
    return {"weather_date": None, "temp_max": None, "temp_min": None, "precip_mm": None}

# Fetch weather for all LAP Coffee locations
weather_data = []
for i, row in gdf.iterrows():
    lat, lon = row.geometry.y, row.geometry.x
    weather = get_weather(lat, lon)
    weather_data.append(weather)
    print(f"{row['name']}: {weather}")

# Add weather columns to GeoDataFrame
gdf["weather_date"] = [w["weather_date"] for w in weather_data]
gdf["temp_max"] = [w["temp_max"] for w in weather_data]
gdf["temp_min"] = [w["temp_min"] for w in weather_data]
gdf["precip_mm"] = [w["precip_mm"] for w in weather_data]

# Save updated GeoPackage
gdf.to_file(OUTPUT_GPKG, layer="lap_coffee", driver="GPKG")
print(f"Saved GeoPackage with weather data to {OUTPUT_GPKG}")
