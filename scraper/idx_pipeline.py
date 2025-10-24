import os
import json
import random
import time
from typing import Dict, List, Optional, Union
from datetime import datetime, timezone

import cloudscraper
from dotenv import load_dotenv
from supabase import create_client, Client

# --- Configuration ---
load_dotenv()  # Load variables from .env file

# API and Scraper Configuration
BASE_URL = "https://sustainability.idx.co.id/api"
TICKER_LIST_URL = f"{BASE_URL}/esg-score-title"
ESG_SCORE_URL_TEMPLATE = f"{BASE_URL}/esg-scores/{{ticker}}?lang=en"
MIN_DELAY = 1.0  # Min random delay between requests in seconds
MAX_DELAY = 3.0  # Max random delay

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = "idx_esg_score_temp"

# --- Helper Functions for Data Transformation ---


def _to_float(value: Optional[str]) -> Optional[float]:
    """Safely convert a string to a float, returning None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _to_int(value: Optional[str]) -> Optional[int]:
    """Safely convert a string to an integer, returning None on failure."""
    if value is None:
        return None
    try:
        # Some values might be floats in string form, e.g., "1.0"
        return int(float(value))
    except (ValueError, TypeError):
        return None


# --- Scraper Functions ---


def get_all_tickers(scraper: cloudscraper.CloudScraper) -> Optional[List[Dict]]:
    """Fetches the list of all ticker codes using the cloudscraper instance."""
    try:
        print("Fetching the list of all company tickers...")
        response = scraper.get(TICKER_LIST_URL)
        response.raise_for_status()
        data = response.json()
        if data.get("status") == "success" and "data" in data:
            print(f"Successfully fetched {len(data['data'])} tickers.")
            return data["data"]
        else:
            print("Error: Could not find 'data' in the ticker list API response.")
            return None
    except cloudscraper.requests.exceptions.RequestException as e:
        print(f"Error fetching ticker list: {e}")
        return None
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON from ticker list response.")
        return None


def get_esg_data_for_ticker(
    scraper: cloudscraper.CloudScraper, ticker_code: str
) -> Optional[Dict]:
    """Fetches the ESG score data for a single ticker code."""
    url = ESG_SCORE_URL_TEMPLATE.format(ticker=ticker_code)
    try:
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        print(
            f"Waiting for {delay:.2f} seconds before fetching data for {ticker_code}..."
        )
        time.sleep(delay)
        response = scraper.get(url)
        response.raise_for_status()
        data = response.json()
        if data.get("status") == "success" and "data" in data:
            print(f"Successfully fetched ESG data for {ticker_code}.")
            return data["data"]
        else:
            print(
                f"Warning: No data found for ticker {ticker_code} in the API response."
            )
            return None
    except cloudscraper.requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"Warning: ESG data not found for ticker {ticker_code} (404 Error).")
        else:
            print(
                f"Error fetching ESG data for {ticker_code}: HTTP {e.response.status_code}"
            )
    except cloudscraper.requests.exceptions.RequestException as e:
        print(f"Error fetching ESG data for {ticker_code}: {e}")
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON for ticker {ticker_code}.")
    return None


def transform_data(api_data: Dict) -> Optional[Dict]:
    """
    Transforms the raw API data to match the target Supabase table schema.
    Performs data type casting and validation.
    Returns None if essential NOT NULL fields are missing.
    """
    sustainalytics_data = api_data.get("sustainalytics")
    if not sustainalytics_data:
        return None

    # Cast values and handle potential errors
    esg_score = _to_float(sustainalytics_data.get("esg_score"))
    controversy_risk = _to_int(sustainalytics_data.get("controversy_risk_level"))

    # --- Validation against NOT NULL constraints ---
    # If these core values are missing after conversion, skip the entire record.
    if esg_score is None:
        ticker = api_data.get("ticker_code", "Unknown")
        print(f"Skipping ticker {ticker} due to missing required data (esg_score).")
        return None

    # The current UTC time when this script runs
    updated_on_timestamp = datetime.now(timezone.utc).isoformat()

    # Map API fields to the final DB schema with correct types
    transformed_record = {
        "symbol": f"{api_data.get('ticker_code', '').upper()}.JK",
        "esg_score": esg_score,
        "controversy_risk": controversy_risk,
        "environment_risk": _to_float(
            sustainalytics_data.get("environment_risk_score")
        ),
        "social_risk": _to_float(sustainalytics_data.get("social_risk_score")),
        "governance_risk": _to_float(sustainalytics_data.get("governance_risk_score")),
        "management": sustainalytics_data.get("esg_risk_category"),
        # "highest_controversy_level": sustainalytics_data.get(
        #     "highest_controversy_level_category"
        # ),
        "industry_group": sustainalytics_data.get("sector"),
        "last_esg_update_date": sustainalytics_data.get("updated_at"),
        "updated_on": updated_on_timestamp,
    }
    return transformed_record


# --- Main Execution ---

if __name__ == "__main__":
    print("Starting ESG data pipeline for IDX.")

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: SUPABASE_URL and SUPABASE_KEY must be set in the .env file.")
        exit(1)

    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
    except Exception as e:
        print(f"Error connecting to Supabase: {e}")
        exit(1)

    scraper = cloudscraper.create_scraper()

    tickers = get_all_tickers(scraper)
    if tickers:
        records_to_upsert = []
        for ticker_info in tickers:
            ticker_code = ticker_info.get("ticker_code")
            if not ticker_code:
                continue

            esg_data = get_esg_data_for_ticker(scraper, ticker_code)

            if esg_data:
                esg_data["ticker_code"] = ticker_code
                transformed_record = transform_data(esg_data)
                if transformed_record:
                    records_to_upsert.append(transformed_record)

        if records_to_upsert:
            print(
                f"\nAttempting to upsert {len(records_to_upsert)} valid records to Supabase table '{TABLE_NAME}'..."
            )
            try:
                # Upsert based on the 'symbol' primary key
                data, count = (
                    supabase.table(TABLE_NAME)
                    .upsert(records_to_upsert, on_conflict="symbol")
                    .execute()
                )
                print(f"Successfully upserted data.")
            except Exception as e:
                print(f"An error occurred during the Supabase upsert operation: {e}")
        else:
            print("\nNo valid data was collected to upsert.")

    print("\nPipeline process finished.")
