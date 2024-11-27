from nsepython import *
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json

# Fetch credentials and Sheet ID from environment variables
credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')  # JSON string
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
def update_sheet(sheet, tab_name, data_frame):
    try:
        # Select or create the tab
        try:
            worksheet = sheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=tab_name, rows="100", cols="20")
        
        # Clear and update the worksheet
        worksheet.clear()
        worksheet.update([data_frame.columns.values.tolist()] + data_frame.values.tolist())
        print(f"Data uploaded to '{tab_name}' successfully.")
    except Exception as e:
        print(f"Error updating '{tab_name}': {e}")

# Fetch data
fo_holidays = pd.json_normalize(nse_holidays()['FO'])
block_deals = pd.DataFrame(get_blockdeals())
bulk_deals = pd.DataFrame(get_bulkdeals())
events = pd.DataFrame(nse_events())

# Save DataFrames locally
fo_holidays.to_csv('fo_holidays.csv', index=False)
block_deals.to_csv('block_deals.csv', index=False)
bulk_deals.to_csv('bulk_deals.csv', index=False)
events.to_csv('nse_events.csv', index=False)

print("Data saved to CSV files.")

# Upload DataFrames to Google Sheets
update_sheet(sheet, "FO_Holidays", fo_holidays)
update_sheet(sheet, "Block_Deals", block_deals)
update_sheet(sheet, "Bulk_Deals", bulk_deals)
update_sheet(sheet, "NSE_Events", events)
