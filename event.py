import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import requests
import csv
from datetime import datetime, timedelta
import datetime as dt
from tenacity import retry, stop_after_attempt, wait_fixed

# Try importing nsepython explicitly
try:
    from nsepython import nse_bulk_deals, nse_block_deals  # Import specific functions
    NSEPYTHON_AVAILABLE = True
    print("✅ nsepython module imported successfully.")
except ImportError as e:
    NSEPYTHON_AVAILABLE = False
    print(f"❌ Failed to import nsepython: {e}")

# --- 7. Upload to Google Sheets ---
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

# --- 1. Calculate Dates ---
to_date = dt.datetime.today()
from_date = to_date - timedelta(days=30)
to_date_str = to_date.strftime('%d-%m-%Y')
from_date_str = from_date.strftime('%d-%m-%Y')

# --- 2. Get NSE Session ---
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_nse_session():
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.google.com/',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
        'TE': 'trailers',
    }
    session.headers.update(headers)

    proxies = {
        'http': os.getenv('HTTP_PROXY'),
        'https': os.getenv('HTTPS_PROXY'),
    } if os.getenv('HTTP_PROXY') or os.getenv('HTTPS_PROXY') else {}

    try:
        init_url = "https://www.nseindia.com/resources/exchange-communication-holidays"
        init_response = session.get(init_url, proxies=proxies, timeout=15)
        if init_response.status_code == 200:
            print("✅ Fetched initial cookies from holidays page.")
        else:
            print(f"⚠️ Initial cookie fetch failed. Status Code: {init_response.status_code}")

        url = "https://www.nseindia.com/"
        response = session.get(url, proxies=proxies, timeout=15)
        if response.status_code == 200:
            print("✅ Accessed NSE homepage and set cookies.")
            print(f"Cookies: {session.cookies.get_dict()}")
            return session
        else:
            print(f"❌ Failed to access NSE homepage. Status Code: {response.status_code}")
            print(f"Response Content: {response.text[:1000]}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Error accessing NSE homepage: {e}")
        return None

# --- 3. Fetch NSE Event Calendar ---
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_nse_events(session, from_date_str, to_date_str):
    url = f'https://www.nseindia.com/api/event-calendar?index=equities&from_date={from_date_str}&to_date={to_date_str}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/377.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-event-calendar',
        'X-Requested-With': 'XMLHttpRequest',
        'Accept-Encoding': 'gzip, deflate, br',
    }

    try:
        response = session.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Event calendar request failed. Status Code: {response.status_code}")
            print(f"Response Content: {response.text[:1000]}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching event calendar: {e}")
        return None

# --- 4. Fetch FO Holidays ---
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_fo_holidays(session):
    url = 'https://www.nseindia.com/api/holiday-master?type=trading'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nseindia.com/resources/exchange-communication-holidays',
        'X-Requested-With': 'XMLHttpRequest',
        'Accept-Encoding': 'gzip, deflate, br',
    }

    try:
        response = session.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ FO holidays request failed. Status Code: {response.status_code}")
            print(f"Response Content: {response.text[:1000]}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching FO holidays: {e}")
        return None

# --- 5. Save Events to CSV ---
def save_events_to_csv(events, filename="nse_events.csv"):
    if not events:
        print("⚠️ No event data to save.")
        return
    if isinstance(events, dict):
        events = [events]
    keys = ['company', 'date', 'purpose', 'symbol', 'bm_desc']
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
def save_holidays_to_csv(fo_holidays, filename="fo_holidays.csv"):
    if not fo_holidays:
        print("⚠️ No FO holiday data to save.")
        return
    if isinstance(fo_holidays, dict) and 'FO' in fo_holidays:
        fo_holidays = fo_holidays['FO']
    elif isinstance(fo_holidays, dict):
        fo_holidays = [fo_holidays]
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

# --- 7. Fetch Bulk/Block Deals Using nsepython ---
def fetch_and_save_deals(deal_type, local_filename, sheet_tab_name):
    if not NSEPYTHON_AVAILABLE:
        print(f"❌ Cannot fetch {deal_type} deals: nsepython module not available.")
        return
    try:
        if deal_type == "bulk":
            data = nse_bulk_deals()
        elif deal_type == "block":
            data = nse_block_deals()
        else:
            raise ValueError("Invalid deal type. Use 'bulk' or 'block'.")

        if data:
            df = pd.DataFrame(data)
            df.to_csv(local_filename, index=False)
            print(f"✅ Saved {deal_type} deals to {local_filename}")
            upload_to_sheets(df, tab_name=sheet_tab_name)
        else:
            print(f"⚠️ No {deal_type} deal data fetched.")
    except Exception as e:
        print(f"❌ Error fetching {deal_type} deals: {e}")

# --- 8. Main Execution ---
if __name__ == "__main__":
    # Log nsepython status
    print(f"nsepython available: {NSEPYTHON_AVAILABLE}")

    # Fetch NSE Events (custom requests as nse_events is not available)
    session = get_nse_session()
    if session:
        events = fetch_nse_events(session, from_date_str, to_date_str)
        if events:
            print("✅ NSE Events fetched successfully.")
            save_events_to_csv(events)
            df_events = pd.DataFrame(events)
            upload_to_sheets(df_events, tab_name="NSE_Events")
        else:
            print("⚠️ No events found or fetch failed.")
    else:
        print("❌ Session creation failed. Skipping events.")

    # Fetch FO Holidays (custom requests as nse_holidays is not available)
    if session:
        holidays = fetch_fo_holidays(session)
        if holidays:
            print("✅ FO Holidays fetched successfully.")
            if isinstance(holidays, dict) and 'FO' in holidays:
                holidays = holidays['FO']
            for h in holidays:
                for key in ['tradingDate', 'weekDay', 'description', 'morning_session', 'evening_session', 'Sr_no']:
                    h.setdefault(key, '')
            save_holidays_to_csv(holidays, filename="fo_holidays.csv")
            df_holidays = pd.DataFrame(holidays)
            upload_to_sheets(df_holidays, tab_name="FO_Holidays")
        else:
            print("⚠️ No FO Holidays found or fetch failed.")
    else:
        print("❌ Session creation failed. Skipping holidays.")

    # Fetch and Upload Bulk Deals
    fetch_and_save_deals("bulk", "bulk_deals.csv", "Bulk_Deals")

    # Fetch and Upload Block Deals
    fetch_and_save_deals("block", "block_deals.csv", "Block_Deals")
