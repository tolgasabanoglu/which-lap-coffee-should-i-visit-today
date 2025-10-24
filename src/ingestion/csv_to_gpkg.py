# src/ingestion/csv_to_gpkg.py

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path

# Input CSV
INPUT_CSV = Path("data/processed/lap_locations_google.csv")
# Output GeoPackage
OUTPUT_GPKG = Path("data/processed/lap_locations.gpkg")

# Step 1: Load CSV
df = pd.read_csv(INPUT_CSV)

# Step 2: Create geometry column
geometry = [Point(xy) for xy in zip(df['lon'], df['lat'])]
gdf = gpd.GeoDataFrame(df, geometry=geometry)

# Step 3: Set CRS (coordinate reference system) to WGS84
gdf.set_crs(epsg=4326, inplace=True)

# Step 4: Save as GeoPackage
OUTPUT_GPKG.parent.mkdir(parents=True, exist_ok=True)
gdf.to_file(OUTPUT_GPKG, layer='lap_coffee', driver="GPKG")

print(f"Saved GeoPackage with {len(gdf)} points to {OUTPUT_GPKG}")
