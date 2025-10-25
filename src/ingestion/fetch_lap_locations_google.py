# src/ingestion/fetch_lap_locations_google.py

import os
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import time

# Step 1: Load API key from .env
load_dotenv()
API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

# Debug: check if API key is loaded
if not API_KEY:
    print("ERROR: Google API key not found. Check your .env file!")
    exit(1)
else:
    print("Loaded API key successfully.")

# Output CSV path
OUTPUT_CSV = Path("data/processed/lap_locations_google.csv")

def fetch_lap_coffee():
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        "query": "LAP Coffee",
        "key": API_KEY
    }

    locations = []
    page = 1

    while True:
        print(f"Requesting Google Places API (page {page})...")
        res = requests.get(url, params=params).json()
        # Debug: print keys returned
        print("Response keys:", res.keys())

        if "error_message" in res:
            print("ERROR from Google API:", res["error_message"])
            break

        results = res.get("results", [])
        print(f"Number of results this page: {len(results)}")

        for place in results:
            locations.append({
                "name": place.get("name"),
                "address": place.get("formatted_address"),
                "lat": place["geometry"]["location"]["lat"],
                "lon": place["geometry"]["location"]["lng"],
                "rating": place.get("rating"),
                "user_ratings_total": place.get("user_ratings_total"),
                "place_id": place.get("place_id")
            })

        if "next_page_token" in res:
            # Wait a couple seconds before requesting the next page
            time.sleep(5)
            params["pagetoken"] = res["next_page_token"]
            page += 1
        else:
            break

    print(f"Total LAP Coffee locations collected: {len(locations)}")

    # Save to CSV
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(locations)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved data to {OUTPUT_CSV}")

if __name__ == "__main__":
    fetch_lap_coffee()
