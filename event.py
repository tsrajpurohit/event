import requests
import time
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json

# Google Sheets setup (from your code)
credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"

if not credentials_json:
    raise ValueError("GOOGLE_SHEETS_CREDENTIALS environment variable is not set.")

credentials_info = json.loads(credentials_json)
credentials = Credentials.from_service_account_info(
    credentials_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
client = gspread.authorize(credentials)
sheet = client.open_by_key(SHEET_ID)

def upload_to_sheets(df, tab_name):
    try:
        df_clean = df.replace([float('inf'), float('-inf')], None).fillna('')
        try:
            worksheet = sheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=tab_name, rows="100", cols="20")
        worksheet.clear()
        worksheet.update([df_clean.columns.values.tolist()] + df_clean.values.tolist())
        print(f"✅ Data uploaded to '{tab_name}' tab.")
    except Exception as e:
        print(f"❌ Google Sheet error for {tab_name}: {e}")

def get_nse_session():
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Referer': 'https://www.nseindia.com/',
    }

    try:
        url = "https://www.nseindia.com/"
        response = session.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            print("✅ Accessed NSE homepage and obtained cookies.")
            return session
        else:
            print(f"❌ Failed to access NSE homepage. Status Code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Error occurred: {e}")
        return None

def fetch_pre_open_data(session):
    url = "https://www.nseindia.com/api/market-data-pre-open?key=FO"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nseindia.com/market-data/pre-open-market-cm-and-fo-segment',
        'Connection': 'keep-alive',
    }

    try:
        time.sleep(1)  # Avoid rate limiting
        response = session.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            print("✅ Pre-open data fetched successfully.")
            return response.json()
        else:
            print(f"❌ Request failed with status code: {response.status_code}")
            print(f"Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Error occurred: {e}")
        return None

# Main execution
if __name__ == "__main__":
    session = get_nse_session()
    if session:
        pre_open_data = fetch_pre_open_data(session)
        if pre_open_data:
            # Convert to DataFrame (adjust based on API response structure)
            df_pre_open = pd.DataFrame(pre_open_data.get('data', []))
            print(df_pre_open)
            # Upload to Google Sheets
            upload_to_sheets(df_pre_open, tab_name="Pre_Open_Data")
        else:
            print("⚠️ No pre-open data fetched.")
