# src/run_full_pipeline.py

import subprocess
from pathlib import Path
from shutil import copyfile
from datetime import datetime

# List of scripts to run in order
scripts = [
    "src/ingestion/fetch_lap_locations_google.py",
    "src/features/add_elevation_google.py",
    "src/features/add_weather.py",
    "src/features/add_ndvi_gee.py",
    "src/features/add_air_quality_gee.py"
]

# Run each script
for s in scripts:
    print(f"Running {s} ...")
    subprocess.run(["python", s], check=True)

# After all scripts, create a unique copy
final_gpkg = Path("data/processed/lap_locations_env_airquality.gpkg")
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
unique_gpkg = Path(f"data/processed/lap_locations_final_{timestamp}.gpkg")

copyfile(final_gpkg, unique_gpkg)
print(f"Pipeline finished. Final GeoPackage saved as: {unique_gpkg}")
