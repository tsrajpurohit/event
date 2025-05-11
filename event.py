import requests
import time
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    print("⚠️ Selenium not installed. Skipping Selenium-based session.")
try:
    from nsepython import nsefetch
except ImportError:
    print("⚠️ nsepython not installed. Skipping nsepython-based fetch.")

# --- Google Sheets Setup ---
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

# --- NSE Session with Selenium ---
def get_nse_session_selenium():
    try:
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36")
        
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.get("https://www.nseindia.com/")
        time.sleep(5)
        cookies = driver.get_cookies()
        print("✅ Cookies obtained via Selenium:", cookies)
        
        session = requests.Session()
        for cookie in cookies:
            session.cookies.set(cookie['name'], cookie['value'])
        return session
    except Exception as e:
        print(f"❌ Selenium error: {e}")
        return None
    finally:
        driver.quit()

# --- NSE Session with Requests ---
def get_nse_session_requests():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[403, 429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
STONE, 'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
        'Referer': 'https://www.google.com/',
    }

    try:
        time.sleep(2)
        url = "https://www.nseindia.com/"
        response = session.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            print("✅ Accessed NSE homepage and obtained cookies.")
            print(f"Cookies: {session.cookies.get_dict()}")
            return session
        else:
            print(f"❌ Failed to access NSE homepage. Status Code: {response.status_code}")
            print(f"Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Error occurred: {e}")
        return None

# --- NSE Session with nsepython ---
def get_nse_session_nsepython():
    try:
        homepage_data = nsefetch("https://www.nseindia.com/")
        if homepage_data:
            print("✅ Accessed NSE homepage via nsepython.")
            session = requests.Session()
            return session
        else:
            print("❌ Failed to access NSE homepage via nsepython.")
            return None
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        return None

# --- Fetch Pre-Open Data ---
def fetch_pre_open_data(session):
    url = "https://www.nseindia.com/api/market-data-pre-open?key=FO"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nseindia.com/market-data/pre-open-market-cm-and-fo-segment',
        'Connection': 'keep-alive',
    }

    try:
        time.sleep(2)
        response = session.get(url, headers=headers, timeout=15)
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

# --- Fetch Pre-Open Data with nsepython ---
def fetch_pre_open_data_nsepython():
    try:
        url = "https://www.nseindia.com/api/market-data-pre-open?key=FO"
        data = nsefetch(url)
        if data:
            print("✅ Pre-open data fetched successfully via nsepython.")
            return data
        else:
            print("❌ Failed to fetch pre-open data.")
            return None
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        return None

# --- Main Execution ---
if __name__ == "__main__":
    session = None
    # Try nsepython first
    try:
        session = get_nse_session_nsepython()
        if session:
            pre_open_data = fetch_pre_open_data_nsepython()
            if pre_open_data:
                df_pre_open = pd.DataFrame(pre_open_data.get('data', []))
                if not df_pre_open.empty:
                    print("Pre-open data sample:")
                    print(df_pre_open.head())
                    upload_to_sheets(df_pre_open, tab_name="Pre_Open_Data")
                else:
                    print("⚠️ Pre-open data is empty.")
                exit(0)
    except NameError:
        print("⚠️ nsepython not available. Falling back to other methods.")

    # Try Selenium
    if not session:
        print("⚠️ Trying Selenium-based session.")
        session = get_nse_session_selenium()

    # Try requests as fallback
    if not session:
        print("⚠️ Falling back to requests-based session.")
        session = get_nse_session_requests()

    if session:
        pre_open_data = fetch_pre_open_data(session)
        if pre_open_data:
            df_pre_open = pd.DataFrame(pre_open_data.get('data', []))
            if not df_pre_open.empty:
                print("Pre-open data sample:")
                print(df_pre_open.head())
                upload_to_sheets(df_pre_open, tab_name="Pre_Open_Data")
            else:
                print("⚠️ Pre-open data is empty.")
        else:
            print("⚠️ Failed to fetch pre-open data.")
    else:
        print("⚠️ Failed to initialize NSE session.")
