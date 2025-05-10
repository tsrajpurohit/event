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
import requests
from bs4 import BeautifulSoup

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
        # Convert numpy.datetime64 to datetime and filter for the specified year
        holidays_year = [pd.Timestamp(h).to_pydatetime() for h in holidays if pd.Timestamp(h).year == year]
        # Convert to a list of dictionaries for consistency
        holiday_data = [
            {
                'tradingDate': h.strftime('%d-%m-%Y'),
                'weekDay': h.strftime('%A'),
                'description': 'Holiday',  # Placeholder
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

# --- Fallback: Fetch Bulk/Block Deals from Moneycontrol ---
def fetch_moneycontrol_deals(deal_type, date_range=30):
    try:
        proxies = {
            'http': os.getenv('HTTP_PROXY'),
            'https': os.getenv('HTTPS_PROXY'),
        } if os.getenv('HTTP_PROXY') or os.getenv('HTTPS_PROXY') else {}
        url = f"https://www.moneycontrol.com/markets/indian-indices/large-deals"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
        }
        response = requests.get(url, headers=headers, proxies=proxies, timeout=15)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            # Placeholder: Parse table for bulk/block deals
            # Note: Actual parsing depends on Moneycontrol's HTML structure
            deals = []
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')[1:]  # Skip header
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 4:
                        deals.append({
                            'symbol': cols[0].text.strip(),
                            'date': cols[1].text.strip(),
                            'quantity': cols[2].text.strip(),
                            'value': cols[3].text.strip()
                        })
            return deals
        else:
            print(f"❌ Moneycontrol request failed for {deal_type}. Status Code: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Error fetching {deal_type} from Moneycontrol: {e}")
        return None

# --- Fetch Bulk/Block Deals Using nsepythonserver ---
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_and_save_deals(deal_type, local_filename, sheet_tab_name):
    if not NSEPYTHON_AVAILABLE:
        print(f"❌ Cannot fetch {deal_type} deals: nsepythonserver module not available.")
        print(f"🔄 Falling back to Moneycontrol for {deal_type}...")
        data = fetch_moneycontrol_deals(deal_type)
        if data:
            df = pd.DataFrame(data)
            df.to_csv(local_filename, index=False)
            print(f"✅ Saved {deal_type} deals to {local_filename} (from Moneycontrol)")
            upload_to_sheets(df, tab_name=sheet_tab_name)
        else:
            print(f"⚠️ No {deal_type} deal data fetched from Moneycontrol.")
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
            print(f"⚠️ No {deal_type} deal data fetched from nsepythonserver. Falling back to Moneycontrol...")
            data = fetch_moneycontrol_deals(deal_type)
            if data:
                df = pd.DataFrame(data)
                df.to_csv(local_filename, index=False)
                print(f"✅ Saved {deal_type} deals to {local_filename} (from Moneycontrol)")
                upload_to_sheets(df, tab_name=sheet_tab_name)
            else:
                print(f"⚠️ No {deal_type} deal data fetched from Moneycontrol.")
    except Exception as e:
        print(f"❌ Error fetching {deal_type} deals from nsepythonserver: {e}")
        print(f"🔄 Falling back to Moneycontrol for {deal_type}...")
        data = fetch_moneycontrol_deals(deal_type)
        if data:
            df = pd.DataFrame(data)
            df.to_csv(local_filename, index=False)
            print(f"✅ Saved {deal_type} deals to {local_filename} (from Moneycontrol)")
            upload_to_sheets(df, tab_name=sheet_tab_name)
        else:
            print(f"⚠️ No {deal_type} deal data fetched from Moneycontrol.")

# --- Main Execution ---
if __name__ == "__main__":
    # Log nsepythonserver status
    print(f"nsepythonserver available: {NSEPYTHON_AVAILABLE}")

    # Skip Events
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
