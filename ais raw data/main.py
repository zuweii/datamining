import os
import time
import math
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# --- Config ---
BASE_URL = "https://gateway.api.globalfishingwatch.org/v3/events"
DATASET_ID = "public-global-encounters-events:latest"   # change to other :latest datasets if needed
DAYS_BACK = 180                                          # "recent" window; increase if you need more rows
TARGET_ROWS = 8000                                      # aim between 5000 and 10000
PAGE_LIMIT = 1000                                       # server max is commonly 1000
OUTFILE = "gfw_encounters_recent.csv"

# Load API token
load_dotenv()
API_TOKEN = os.getenv("GFW_API_TOKEN")
if not API_TOKEN:
    raise RuntimeError("Missing GFW_API_TOKEN in your environment/.env")

headers = {"Authorization": f"Bearer {API_TOKEN}"}

def iso_z(dt: datetime) -> str:
    """Return ISO string with trailing 'Z'."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def fetch_events_recent(dataset_id=DATASET_ID,
                        days_back=DAYS_BACK,
                        target_rows=TARGET_ROWS,
                        page_limit=PAGE_LIMIT,
                        outfile=OUTFILE):
    start_dt = datetime.now(timezone.utc) - timedelta(days=days_back)
    end_dt = datetime.now(timezone.utc)

    params = {
        "datasets[0]": dataset_id,
        "start-date": iso_z(start_dt),
        "end-date": iso_z(end_dt),
        "limit": page_limit,
        "offset": 0,
    }

    all_entries = []
    page = 0
    session = requests.Session()
    backoff = 1.0

    while True:
        resp = session.get(BASE_URL, headers=headers, params=params, timeout=60)
        if resp.status_code == 429:  # rate limited
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        if resp.status_code >= 400:
            raise RuntimeError(f"Error {resp.status_code}: {resp.text}")

        data = resp.json()
        entries = data.get("entries", [])
        if not entries:
            break

        all_entries.extend(entries)
        page += 1

        # Stop if we have what we need
        if len(all_entries) >= target_rows:
            break

        # Advance offset: prefer server-provided nextOffset if present
        next_offset = data.get("nextOffset")
        if next_offset is None:
            # fallback: increment by limit
            params["offset"] = params.get("offset", 0) + params["limit"]
        else:
            params["offset"] = next_offset

        # small, polite pause to avoid rate limits
        time.sleep(0.25)

    # Flatten JSON to table
    if not all_entries:
        print("No events returned for the chosen window. Consider increasing DAYS_BACK or checking dataset.")
        return pd.DataFrame()

    df = pd.json_normalize(all_entries, sep="__")
    df.to_csv(outfile, index=False)
    print(f"Saved {len(df):,} events to {outfile}")
    print(f"Date window: {params['start-date']} -> {params['end-date']}")
    return df

if __name__ == "__main__":
    df = fetch_events_recent()
