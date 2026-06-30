import requests
from datetime import datetime, timedelta

# ==========================
# Dynamic Date (Last 6 Months)
# ==========================

today = datetime.today()
six_months_ago = today - timedelta(days=182)   # Approx. 6 months

from_date = six_months_ago.strftime("%d-%m-%Y")
to_date = today.strftime("%d-%m-%Y")

# Dynamic URL
url = (
    f"https://www.nseindia.com/api/historicalOR/"
    f"bulk-block-short-deals?"
    f"optionType=bulk_deals&from={from_date}&to={to_date}&csv=true"
)

print("URL:", url)

# ==========================
# Headers
# ==========================

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "Accept": "text/csv,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}

output_filename = "nse_bulk_deals_6M.csv"


def fetch_nse_csv(target_url, output_path):
    session = requests.Session()
    session.headers.update(headers)

    try:
        # Get cookies first
        print("Initializing NSE session...")
        session.get("https://www.nseindia.com", timeout=10)

        print(f"Fetching data from:\n{target_url}")

        response = session.get(target_url, timeout=20)

        if response.status_code == 200:
            with open(output_path, "wb") as f:
                f.write(response.content)

            print(f"✅ Saved to {output_path}")
        else:
            print("Status:", response.status_code)
            print(response.text[:500])

    except Exception as e:
        print(e)


if __name__ == "__main__":
    fetch_nse_csv(url, output_filename)
