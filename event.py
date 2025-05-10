import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import csv
from datetime import datetime, timedelta
import datetime as dt
from tenacity import retry, stop_after_attempt, wait_fixed
from pandas_market_calendars import get_calendar

# Try importing nsepythonserver
try:
    from nsepythonserver import nse_largedeals
    NSEPYTHON_AVAILABLE = True
    print("✅ nsepythonserver module imported successfully.")
except ImportError as e:
    NSEPYTHON_AVAILABLE = False
    print(f"❌ Failed to import nsepythonserver: {e}")

# --- Upload to Google Sheets ---
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
        df_clean = df.replace([float('inf'), float('-inf')], None)
        df_clean = df_clean.fillna('')
        try:
            worksheet = sheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=tab_name, rows="100", cols="20")
        worksheet.clear()
        worksheet.update([df_clean.columns.values.tolist()] + df_clean.values.tolist())
        print(f"✅ Data uploaded to '{tab_name}' tab.")
    except Exception as e:
        print(f"❌ Google Sheet error for {tab_name}: {e}")

# --- Calculate Dates ---
to_date = dt.datetime.today()
from_date = to_date - timedelta(days=30)
to_date_str = to_date.strftime('%d-%m-%Y')
from_date_str = from_date.strftime('%d-%m-%Y')

# --- Fetch NSE Holidays ---
def fetch_nse_holidays(year=2025):
    try:
        nse_cal = get_calendar('XNSE')  # NSE calendar
        holidays = nse_cal.holidays().holidays
        # Filter holidays for the specified year
        holidays_year = [h for h in holidays if h.year == year]
        # Convert to a list of dictionaries for consistency
        holiday_data = [
            {
                'tradingDate': h.strftime('%d-%m-%Y'),
                'weekDay': h.strftime('%A'),
                'description': 'Holiday',  # Placeholder, as exact descriptions may vary
                'morning_session': 'Closed',
                'evening_session': 'Closed',
                'Sr_no': idx + 1
            }
            for idx, h in enumerate(holidays_year)
        ]
        return holiday_data
    except Exception as e:
        print(f"❌ Error fetching NSE holidays: {e}")
        return None

# --- Save Holidays to CSV ---
def save_holidays_to_csv(fo_holidays, filename="fo_holidays.csv"):
    if not fo_holidays:
        print("⚠️ No FO holiday data to save.")
        return
    keys = ['tradingDate', 'weekDay', 'description', 'morning_session', 'evening_session', 'Sr_no']
    with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=keys)
        writer.writeheader()
        for holiday in fo_holidays:
            writer.writerow({
                'tradingDate': holiday.get('tradingDate', ''),
                'weekDay': holiday.get('weekDay', ''),
                'description': holiday.get('description', ''),
                'morning_session': holiday.get('morning_session', ''),
                'evening_session': holiday.get('evening_session', ''),
                'Sr_no': holiday.get('Sr_no', '')
            })
    print(f"✅ FO Holidays saved to '{filename}'")

# --- Fetch Bulk/Block Deals Using nsepythonserver ---
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_and_save_deals(deal_type, local_filename, sheet_tab_name):
    if not NSEPYTHON_AVAILABLE:
        print(f"❌ Cannot fetch {deal_type} deals: nsepythonserver module not available.")
        return
    try:
        if deal_type not in ["bulk_deals", "block_deals"]:
            raise ValueError("Invalid deal type. Use 'bulk_deals' or 'block_deals'.")
        data = nse_largedeals(deal_type)
        if data:
            df = pd.DataFrame(data)
            df.to_csv(local_filename, index=False)
            print(f"✅ Saved {deal_type} deals to {local_filename}")
            upload_to_sheets(df, tab_name=sheet_tab_name)
        else:
            print(f"⚠️ No {deal_type} deal data fetched.")
    except Exception as e:
        print(f"❌ Error fetching {deal_type} deals: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    # Log nsepythonserver status
    print(f"nsepythonserver available: {NSEPYTHON_AVAILABLE}")

    # Skip Events (no reliable API available)
    print("⚠️ Corporate events not supported due to lack of API. Skipping.")

    # Fetch and Save Holidays
    holidays = fetch_nse_holidays(year=2025)
    if holidays:
        print("✅ NSE Holidays fetched successfully.")
        save_holidays_to_csv(holidays, filename="fo_holidays.csv")
        df_holidays = pd.DataFrame(holidays)
        upload_to_sheets(df_holidays, tab_name="FO_Holidays")
    else:
        print("⚠️ No holidays found or fetch failed.")

    # Fetch and Upload Bulk Deals
    fetch_and_save_deals("bulk_deals", "bulk_deals.csv", "Bulk_Deals")

    # Fetch and Upload Block Deals
    fetch_and_save_deals("block_deals", "block_deals.csv", "Block_Deals")
