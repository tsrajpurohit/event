from nsepython import *
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import requests
import csv
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
import datetime as dt

# --- 7. Upload to Google Sheets ---
# Fetch credentials and Sheet ID from environment variables
credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"

if not credentials_json:
    raise ValueError("GOOGLE_SHEETS_CREDENTIALS environment variable is not set.")

# Authenticate using the JSON string from environment
credentials_info = json.loads(credentials_json)
credentials = Credentials.from_service_account_info(
    credentials_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
client = gspread.authorize(credentials)

# Open the Google Sheet by ID
sheet = client.open_by_key(SHEET_ID)

# Function to update data in a Google Sheet tab
def upload_to_sheets(df, tab_name):
    try:
        # Replace problematic values with empty strings or a placeholder
        df_clean = df.replace([float('inf'), float('-inf')], None)
        df_clean = df_clean.fillna('')  # or use a placeholder like 'NA'

        try:
            worksheet = sheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=tab_name, rows="100", cols="20")

        worksheet.clear()
        worksheet.update([df_clean.columns.values.tolist()] + df_clean.values.tolist())
        print(f"✅ Data uploaded to '{tab_name}' tab.")
    except Exception as e:
        print(f"❌ Google Sheet error for {tab_name}: {e}")

# --- 1. Calculate Dates ---
to_date = dt.datetime.today()
from_date = to_date - timedelta(days=30)

to_date_str = to_date.strftime('%d-%m-%Y')
from_date_str = from_date.strftime('%d-%m-%Y')

# --- 2. Get NSE Session ---
def get_nse_session():
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-event-calendar',
        'Connection': 'keep-alive'
    }

    try:
        url = "https://www.nseindia.com/companies-listing/corporate-filings-event-calendar"
        response = session.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            print("✅ Accessed the NSE calendar page successfully.")
            return session
        else:
            print(f"❌ Failed to access NSE page. Status Code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Error occurred: {e}")
        return None

# --- 3. Fetch NSE Event Calendar ---
def fetch_nse_events(session, from_date_str, to_date_str):
    url = f'https://www.nseindia.com/api/event-calendar?index=equities&from_date={from_date_str}&to_date={to_date_str}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-event-calendar',
    }

    try:
        response = session.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Request failed with status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Error occurred: {e}")
        return None

# --- 4. Fetch FO Holidays ---
def fetch_fo_holidays(session):
    url = f'https://www.nseindia.com/api/holiday-master?type=trading'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Referer': 'https://www.nseindia.com/resources/exchange-communication-holidays',
    }

    try:
        response = session.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Request failed with status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Error occurred: {e}")
        return None

# --- 5. Save Events to CSV ---
def save_events_to_csv(events, filename="nse_events.csv"):
    if not events:
        print("⚠️ No event data to save.")
        return

    if isinstance(events, dict):
        events = [events]
    
    keys = ['company', 'date', 'purpose', 'symbol','bm_desc']
    with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=keys)
        writer.writeheader()
        for event in events:
            writer.writerow({
                'company': event.get('company', ''),
                'date': event.get('date', ''),
                'purpose': event.get('purpose', ''),
                'symbol': event.get('symbol', ''),
                'bm_desc': event.get('bm_desc', '')
            })
    print(f"✅ Events saved to '{filename}'")

# --- 6. Save Holidays to CSV ---
def save_Holidays_to_csv(FO_Holidays, filename="fo_holidays.csv"):
    if not FO_Holidays:
        print("⚠️ No FO Holiday data to save.")
        return

    if isinstance(FO_Holidays, dict) and 'FO' in FO_Holidays:
        FO_Holidays = FO_Holidays['FO']
    elif isinstance(FO_Holidays, dict):
        FO_Holidays = [FO_Holidays]

    keys = ['tradingDate', 'weekDay', 'description', 'morning_session', 'evening_session', 'Sr_no']
    with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=keys)
        writer.writeheader()
        for holiday in FO_Holidays:
            writer.writerow({
                'tradingDate': holiday.get('tradingDate', ''),
                'weekDay': holiday.get('weekDay', ''),
                'description': holiday.get('description', ''),
                'morning_session': holiday.get('morning_session', ''),
                'evening_session': holiday.get('evening_session', ''),
                'Sr_no': holiday.get('Sr_no', '')
            })
    print(f"✅ FO Holidays saved to '{filename}'")




# --- 9. Fetch and Save NSE Bulk/Block Deals ---
def fetch_and_save_csv(url, local_filename, sheet_tab_name):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            with open(local_filename, 'wb') as f:
                f.write(response.content)
            print(f"✅ Downloaded and saved {local_filename}")

            df = pd.read_csv(local_filename)
            upload_to_sheets(df, tab_name=sheet_tab_name)
        else:
            print(f"❌ Failed to download {local_filename}. Status: {response.status_code}")
    except Exception as e:
        print(f"❌ Error fetching {local_filename}: {e}")


# --- 8. Main Execution ---
if __name__ == "__main__":
    session = get_nse_session()

    if session:
        # Fetch NSE Events
        events = fetch_nse_events(session, from_date_str, to_date_str)
        if events:
            print("✅ NSE Events fetched successfully.")
            save_events_to_csv(events)
            df_events = pd.DataFrame(events)
            upload_to_sheets(df_events, tab_name="NSE_Events")
        else:
            print("⚠️ No events found or fetch failed.")

        # Fetch FO Holidays
        holidays = fetch_fo_holidays(session)
        if holidays:
            print("✅ FO Holidays fetched successfully.")

            # Extract holiday list if inside 'FO' key
            if isinstance(holidays, dict) and 'FO' in holidays:
                holidays = holidays['FO']

            # Ensure consistent keys before DataFrame creation
            for h in holidays:
                for key in ['tradingDate', 'weekDay', 'description', 'morning_session', 'evening_session', 'Sr_no']:
                    h.setdefault(key, '')

            save_Holidays_to_csv(holidays, filename="fo_holidays.csv")
            df_holidays = pd.DataFrame(holidays)
            upload_to_sheets(df_holidays, tab_name="FO_Holidays")
        else:
            print("⚠️ No FO Holidays found or fetch failed.")
        
                # --- Fetch and Upload Bulk Deals ---
                # --- Fetch and Upload Bulk Deals ---
        fetch_and_save_csv(
            "https://archives.nseindia.com/content/equities/bulk.csv",
            "bulk_deals.csv",
            "Bulk_Deals"
        )

        # --- Fetch and Upload Block Deals ---
        fetch_and_save_csv(
            "https://archives.nseindia.com/content/equities/block.csv",
            "block_deals.csv",
            "Block_Deals"
        )
