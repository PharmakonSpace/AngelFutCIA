import requests
import pandas as pd
from datetime import datetime
import os

# Step 1: Check fetch log
log_file = "fetch_log.txt"
today_str = datetime.now().strftime('%Y-%m-%d')

if os.path.exists(log_file):
    with open(log_file, "r") as f:
        last_fetch_date = f.read().strip()
    if last_fetch_date == today_str:
        print("Data already fetched today. Skipping fetch.")
        exit()

# Step 2: Fetch the data from the URL
url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
response = requests.get(url)

# Ensure the request was successful
if response.status_code != 200:
    print("Failed to fetch data.")
    exit()

data = response.json()

# Step 3: Prepare date string for expiry filtering
current_date = datetime.now()
current_month = current_date.strftime('%b').upper()
current_year = current_date.year
expiry_date = f"29{current_month}{current_year}"

allowed_digit_names = {
    'NIFTY 50',
    
}
filtered_data = []
# Step 1: Collect all names available in NFO FUTSTK
nfo_names = set()
for item in data:
    if item.get('instrumenttype') == 'FUTSTK' and item.get('exch_seg') == 'NFO':
        name = item.get('name', '').strip().upper()
        if name:
            nfo_names.add(name)

# Step 2: Filter based on conditions
for item in data:
    instrument_type = item.get('instrumenttype', '')
    expiry = item.get('expiry', '')
    exch_seg = item.get('exch_seg', '')
    symbol = item.get('symbol', '')
    name = item.get('name', '').strip().upper()
    
    # Skip test symbols
    if 'NSETEST' in symbol.upper():
        continue
    # Skip names with digits unless explicitly allowed
    if any(char.isdigit() for char in name) and name not in allowed_digit_names:
        continue

    if (instrument_type == 'FUTSTKx' and expiry and expiry_date in expiry and exch_seg == 'NFOx') or \
       (instrument_type == '' and expiry == '' and exch_seg == 'NSE' and symbol.endswith('-EQ') and name in nfo_names) or \
       (instrument_type == 'AMXIDX' and expiry == "" and exch_seg in ['NSE', 'NFO']):
        filtered_data.append(item)


# Step 5: Save DataFrame to CSV
df = pd.DataFrame(filtered_data)
if not df.empty:
    csv_filename = f"Angel_MasterList.csv"
    df.to_csv(csv_filename, index=False)
    print(f"Filtered data saved to {csv_filename}")
    
    # Step 6: Update log file
    with open(log_file, "w") as f:
        f.write(today_str)
else:
    print(f"No matching data found for {expiry_date}.")
