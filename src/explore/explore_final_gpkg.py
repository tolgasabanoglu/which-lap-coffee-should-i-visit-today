# src/explore/explore_final_gpkg.py

import geopandas as gpd
from pathlib import Path

# Path to the final enriched GeoPackage
gpkg_path = Path("/Users/tolgasabanoglu/Desktop/github/which-lap-coffee-should-i-visit-today/data/processed/lap_locations_final_20251024_120840.gpkg")  # or your timestamped file

# Load the GeoPackage
gdf = gpd.read_file(gpkg_path, layer="lap_coffee")

print("=== HEAD ===")
print(gdf.head(), "\n")

print("=== DATA TYPES ===")
print(gdf.dtypes, "\n")

print("=== MISSING VALUES ===")
print(gdf.isnull().sum(), "\n")

print("=== NUMERIC SUMMARY ===")
print(gdf.describe(), "\n")
