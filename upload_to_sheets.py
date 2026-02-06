"""
One-time script to upload CSV data from farm_app_synth_data/ into Google Sheets.

Prerequisites:
  1. Place your service_account.json in this folder (farm_app_cloud/).
  2. Create a Google Spreadsheet and copy its ID from the URL.
  3. Share the spreadsheet with the service account email (Editor).

Usage:
  python upload_to_sheets.py
"""

import os
import time
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CSV_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "farm_app_synth_data")
SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "service_account.json")

# Paste your Google Spreadsheet ID here (from the URL)
SPREADSHEET_ID = "1JPhIKXqMMJJU8x-UotNjSRfVLblkhwHJ7MuYIR_77nI"

SHEETS = {
    "workers": "workers.csv",
    "work_types": "work_types.csv",
    "work_logs": "work_logs.csv",
    "tools": "tools.csv",
    "tool_moves": "tool_moves.csv",
    "storage_places": "storage_places.csv",
}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def main():
    # Authenticate
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    for sheet_name, csv_file in SHEETS.items():
        csv_path = os.path.join(CSV_DIR, csv_file)
        print(f"Reading {csv_path} ...")
        df = pd.read_csv(csv_path, encoding="utf-8-sig")

        # Clean up phone numbers: float → string (e.g. 9.87654e+09 → "9876543210")
        if "phone" in df.columns:
            df["phone"] = (
                df["phone"]
                .apply(lambda v: str(int(v)) if pd.notna(v) and v != "" else "")
            )

        # Convert everything to string for Sheets upload
        df = df.fillna("")
        data = [df.columns.tolist()] + df.astype(str).values.tolist()

        # Create or get worksheet
        try:
            ws = spreadsheet.worksheet(sheet_name)
            ws.clear()
            print(f"  Cleared existing worksheet '{sheet_name}'")
        except gspread.exceptions.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=sheet_name, rows=len(data) + 10, cols=len(data[0]) + 2)
            print(f"  Created new worksheet '{sheet_name}'")

        ws.update(range_name="A1", values=data)
        print(f"  Uploaded {len(df)} rows to '{sheet_name}'")

        # Rate limit: avoid hitting Google API quota
        time.sleep(2)

    print("\nDone! All sheets uploaded. Verify in Google Sheets.")


if __name__ == "__main__":
    main()
