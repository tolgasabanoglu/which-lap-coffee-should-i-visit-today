# src/run_full_pipeline.py
import subprocess
from pathlib import Path
from shutil import copyfile
from datetime import datetime
import sys  # add this

scripts = [
    "src/ingestion/fetch_lap_locations_google.py",
    "src/features/add_elevation_google.py",
    "src/features/add_weather.py",
    "src/features/add_gee_data.py"]

for s in scripts:
    print(f"Running {s} ...")
    subprocess.run([sys.executable, s], check=True)  # use sys.executable

# After all scripts, create a unique copy
final_gpkg = Path("data/processed/lap_locations_env_airquality.gpkg")
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
unique_gpkg = Path(f"data/processed/lap_locations_final_{timestamp}.gpkg")

copyfile(final_gpkg, unique_gpkg)
print(f"Pipeline finished. Final GeoPackage saved as: {unique_gpkg}")
