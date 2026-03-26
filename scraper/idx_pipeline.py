import os
import json
import random
import time
from typing import Dict, List, Optional
from datetime import datetime, timezone

# We use curl_cffi instead of cloudscraper for advanced Cloudflare/WAF bypass.
from curl_cffi import requests
from dotenv import load_dotenv
from supabase import create_client, Client

# --- Configuration ---
load_dotenv()  # Load variables from .env file

# API Configuration
BASE_URL = "https://sustainability.idx.co.id/api"
TICKER_LIST_URL = f"{BASE_URL}/esg-score-title"
ESG_SCORE_URL_TEMPLATE = f"{BASE_URL}/esg-scores/{{ticker}}?lang=en"
MIN_DELAY = 1.5  # Slightly increased min delay to prevent rate-limiting
MAX_DELAY = 4.0  # Max random delay

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = "idx_esg_score"

# We omit 'User-Agent' here because curl_cffi automatically injects the perfect
# User-Agent that matches the Chrome 120 TLS fingerprint.
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
    "Referer": "https://sustainability.idx.co.id/",
    "Origin": "https://sustainability.idx.co.id",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

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
        return int(float(value))
    except (ValueError, TypeError):
        return None


# --- Scraper Functions ---


def create_scraper_session() -> requests.Session:
    """
    Initializes a session that impersonates a modern Chrome browser to bypass WAF.
    Also fetches the homepage first to acquire any necessary session cookies.
    """
    # impersonate="chrome120" accurately mimics a real browser's TLS fingerprint
    session = requests.Session(impersonate="chrome120")
    session.headers.update(HEADERS)

    try:
        print("Initializing session and fetching initial cookies from homepage...")
        # Visiting the root domain first often solves token/cookie validation issues
        session.get("https://sustainability.idx.co.id/", timeout=30)
        time.sleep(random.uniform(1.0, 2.0))
    except Exception as e:
        print(f"Warning: Failed to fetch initial page: {e}")

    return session


def get_all_tickers(session: requests.Session) -> Optional[List[Dict]]:
    """Fetches the list of all ticker codes using the active session."""
    try:
        print("Fetching the list of all company tickers...")
        response = session.get(TICKER_LIST_URL, timeout=30)

        if response.status_code == 403:
            print("Error: 403 Forbidden. The WAF is still blocking the request.")
            return None
        elif response.status_code != 200:
            print(f"Error fetching ticker list: HTTP {response.status_code}")
            return None

        data = response.json()
        if data.get("status") == "success" and "data" in data:
            print(f"Successfully fetched {len(data['data'])} tickers.")
            return data["data"]
        else:
            print("Error: Could not find 'data' in the ticker list API response.")
            return None

    except Exception as e:
        print(f"Exception fetching ticker list: {e}")
        return None


def get_esg_data_for_ticker(
    session: requests.Session, ticker_code: str
) -> Optional[Dict]:
    """Fetches the ESG score data for a single ticker code."""
    url = ESG_SCORE_URL_TEMPLATE.format(ticker=ticker_code)
    try:
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        print(
            f"Waiting for {delay:.2f} seconds before fetching data for {ticker_code}..."
        )
        time.sleep(delay)

        response = session.get(url, timeout=30)

        if response.status_code == 404:
            print(f"Warning: ESG data not found for ticker {ticker_code} (404 Error).")
            return None
        elif response.status_code != 200:
            print(
                f"Error fetching ESG data for {ticker_code}: HTTP {response.status_code}"
            )
            return None

        data = response.json()
        if data.get("status") == "success" and "data" in data:
            print(f"Successfully fetched ESG data for {ticker_code}.")
            return data["data"]
        else:
            print(
                f"Warning: No data found for ticker {ticker_code} in the API response."
            )
            return None

    except Exception as e:
        print(f"Error fetching ESG data for {ticker_code}: {e}")

    return None


def transform_data(api_data: Dict) -> Optional[Dict]:
    """
    Transforms the raw API data to match the target Supabase table schema.
    Returns None if essential NOT NULL fields are missing.
    """
    sustainalytics_data = api_data.get("sustainalytics")
    if not sustainalytics_data:
        return None

    esg_score = _to_float(sustainalytics_data.get("esg_score"))
    controversy_risk = _to_int(sustainalytics_data.get("controversy_risk_level"))

    if esg_score is None:
        ticker = api_data.get("ticker_code", "Unknown")
        print(f"Skipping ticker {ticker} due to missing required data (esg_score).")
        return None

    updated_on_timestamp = datetime.now(timezone.utc).isoformat()

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

    # Initialize WAF-bypassing session
    session = create_scraper_session()

    tickers = get_all_tickers(session)
    if tickers:
        records_to_upsert = []
        for ticker_info in tickers:
            ticker_code = ticker_info.get("ticker_code")
            if not ticker_code:
                continue

            esg_data = get_esg_data_for_ticker(session, ticker_code)

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
