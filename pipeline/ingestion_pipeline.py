import requests
import json
import time
import pandas as pd
from sqlalchemy import create_engine
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import BALLDONTLIE_API_KEY, DATABASE_URL

BASE_URL = "https://api.balldontlie.io/ucl/v1"

# SQLAlchemy engine
engine = create_engine(DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://"))

session = requests.Session()
session.headers.update({"Authorization": BALLDONTLIE_API_KEY})
session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=5,
    backoff_factor=2,     # waits 2s, 4s, 8s, 16s, 32s between retries
    status_forcelist=[429, 500, 502, 503]  # retry on these status codes
)))

def fetch_all(endpoint: str, per_page: int = 100) -> list[dict]:
    results, params = [], {"per_page": per_page}
    while True:
        payload = session.get(f"{BASE_URL}/{endpoint}", params=params).json()
        results.extend(payload.get("data", []))
        next_cursor = payload.get("meta", {}).get("next_cursor")
        if not next_cursor:
            break
        params["cursor"] = next_cursor
        time.sleep(0.5)
    return results

def sanitize(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            # Flatten nested dict into dot-separated columns: team.id, team.name etc.
            normalized = pd.json_normalize(df[col].tolist())
            normalized.columns = [f"{col}_{c}" for c in normalized.columns]
            normalized.index = df.index
            df = pd.concat([df.drop(columns=[col]), normalized], axis=1)

        elif df[col].apply(lambda x: isinstance(x, list)).any():
            # Serialize lists to JSON strings so Postgres can store them as text
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)

    return df

def ingest(endpoint: str, table_name: str):
    print(f"⏳ Fetching {table_name}...")
    raw = fetch_all(endpoint)
    if not raw:
        print(f"⚠️  No data returned for {table_name}")
        return

    df = pd.DataFrame(raw)  # columns come directly from the JSON keys
    df.columns = df.columns.str.lower()  # normalize to lowercase
    df = sanitize(df)

    # to_sql handles CREATE TABLE and INSERT automatically
    # 'replace' drops and recreates the table — swap to 'append' once stable
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"✅ {len(df)} rows → '{table_name}'")


def main():
    if not BALLDONTLIE_API_KEY:
        print("❌ Missing BALLDONTLIE_API_KEY")
        return

    # Add every endpoint/table pair you need here — that's it
    tables = [
        ("players",   "ucl_players"),
        ("standings", "ucl_standings"),
    ]

    for endpoint, table_name in tables:
        ingest(endpoint, table_name)


if __name__ == "__main__":
    main()
