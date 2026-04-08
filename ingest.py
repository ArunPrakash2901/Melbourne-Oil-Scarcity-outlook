import os
import json
import logging
import requests
import pandas as pd
from typing import List, Optional
from dotenv import load_dotenv
from datetime import datetime

# Setup logging for Github Actions Visibility
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables securely
load_dotenv()
VIC_GOV_API_KEY = os.getenv("VIC_GOV_API_KEY")

if not VIC_GOV_API_KEY:
    logger.warning("VIC_GOV_API_KEY not found in environment. API requests may be rate-limited or rejected.")

# Constants & Metadata
CKAN_BASE_URL = "https://discover.data.vic.gov.au/api/3/action"
TIRTL_RESOURCE_ID = "df26b31a-aee3-4d86-847b-5025e951be41"

# Greater Melbourne Boundary
MIN_LAT, MAX_LAT = -38.20, -37.50
MIN_LON, MAX_LON = 144.50, 145.50

# Vehicle classes
CLASS_PASSENGER = ["1"] # For actual policy effect
CLASS_FREIGHT = ["5", "6", "7"] # For Difference-in-Differences control group


# functions
def fetch_metro_sites(sites_csv_path:str="data/tirtl_sites.csv") -> List[str]:
    """
    Loads TIRTL site metadata and returns a list of site_ids that fall strictly within the boundary constraints
    """
    logger.info("Filtering TIRTL sites by boundary")
    try:
        df_sites = pd.read_csv(sites_csv_path)

        # The raw csv uses 'site'
        if "site" in df_sites.columns:
            df_sites = df_sites.rename(columns={"site": "site_id"})

        # Apply boundary filter
        metro_sites = df_sites[
            (df_sites["latitude"] >= MIN_LAT) & (df_sites["latitude"] <= MAX_LAT) &
            (df_sites["longitude"] >= MIN_LON) & (df_sites["longitude"] <= MAX_LON)
        ]
        valid_site_ids = metro_sites["site_id"].unique().tolist()
        logger.info(f"Retained {len(valid_site_ids)} active sites within metro bounding box.")
        return valid_site_ids
    
    except FileNotFoundError:
        logger.error((f"Sites metadata file not found at {sites_csv_path}. Download this first."))
        raise

def fetch_tirtl_traffic(start_date: str, end_date: str, site_ids: List[str]) -> pd.DataFrame:
    """
    Pulls Class 1 (Passenger) and Class 5-7 (Freight) traffic volumes using the standard datastore_search endpoint. Filters by site_id and vehicle_class at the API level, and filters dates locally.
    """
    logger.info(f"Fetching traffic data from {start_date} to {end_date}...")
    headers = {"Authorization": VIC_GOV_API_KEY} if VIC_GOV_API_KEY else {}
    endpoint = f"{CKAN_BASE_URL}/datastore_search"
   
    target_classes = CLASS_PASSENGER + CLASS_FREIGHT
    # Ensure site_ids are strings for the API filter
    site_ids = [str(s) for s in site_ids]
    
    # Standard endpoint uses JSON filters for exact matches
    filters = {
            "site": site_ids,
            "vehicle_class": target_classes
    }

    params = {
            "resource_id": TIRTL_RESOURCE_ID,
            "filters": json.dumps(filters),
            "limit": 32000, # Increase limit to ensure we get all the records for local filtering
            "sort": "date asc" # Sort by date to help find the requested range
    }
            

    try:
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        if not data.get("success"):
            logger.error(f"CKAN API returned an error: {data.get('error')}")
            return pd.DataFrame()

        records = data["result"]["records"]
        df = pd.DataFrame(records)

        if df.empty:
            logger.warning("Query returned zero records. Check date formats and site IDs.")
            return df

        # Rename the 'site' column to our standard 'site_id'
        if 'site' in df.columns:
            df = df.rename(columns={'site': 'site_id'})

        # Enforce data types for Parquet serialization
        df["date"] = pd.to_datetime(df["date"])
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
        df["vehicle_class"] = df["vehicle_class"].astype(str)
        
        # Convert the string inputs to datetime objects for safe comparison
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)

        # Apply Date filtering
        mask = (df["date"] >= start_dt) & (df["date"] <= end_dt)
        df_filtered = df.loc[mask].copy()

        logger.info(f"Successfully retrieved {len(df)} records. Filtered to {len(df_filtered)} within date range.")
        return df_filtered
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error during API calling: {e}")
        raise


if __name__ == "__main__":
    print("--- Testing Ingestion Layer ---")

    # 1. Test Bounding box logic
    try:
        metro_sites = fetch_metro_sites(sites_csv_path="data/tirtl_sites.csv")
        test_sites = metro_sites[:5]

        # 2. Test Phase 1 Baseline pull (March 1 to April 6, 2026)
        df_baseline = fetch_tirtl_traffic(
                start_date="2026-03-01",
                end_date="2026-04-06",
                site_ids=test_sites
        )

        print("\nSample Baseline Data:")
        if not df_baseline.empty:
            print(df_baseline.head())
        else:
            print("No data found for the specified range and sites.")

    except Exception as e:
        print(f"\nTest failed. Ensure data/tirtl_sites.csv exists and API is reachable. Error: {e}")



    
