import requests
from SmartApi import SmartConnect
import pandas as pd
import pandas_ta as ta
import ta as ta_lib
from datetime import datetime, timedelta
import numpy as np
from time import sleep
import pyotp
import warnings
import os
import pytz
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor
import logging
import subprocess
from google.oauth2.service_account import Credentials
import json
import hashlib
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Debug - Check if .env is loaded properly
print(f"Current working directory: {os.getcwd()}")
print(f".env file exists: {os.path.exists('.env')}")

# Get all credentials directly from environment variables
USER_NAME = os.getenv("USER_NAME")
API_KEY = os.getenv("API_KEY")
PWD = os.getenv("PWD")
TOTP_SECRET = os.getenv("TOTP_SECRET")
SHEET_ID = os.getenv("SHEET_ID", "17y8FzzvHnc5jgMoS40H1WXxj133PgAcjfxYcXtoGwh4")
CREDENTIALS_FILE = os.getenv('GOOGLE_SHEETS_CREDENTIALS')

# Debug - Print credential status
print("Environment variables loaded:")
print(f"USER_NAME: {'Set' if USER_NAME else 'Not Set'}")
print(f"API_KEY: {'Set' if API_KEY else 'Not Set'}")
print(f"PWD: {'Set' if PWD else 'Not Set'}")
print(f"TOTP_SECRET: {'Set' if TOTP_SECRET else 'Not Set'}")
print(f"GOOGLE_SHEETS_CREDENTIALS: {'Set' if CREDENTIALS_FILE else 'Not Set'}")

# Global variables
SMART_API_OBJ = None
TOKEN_MAP = None

# Run Angelmasterlist.py
script_dir = os.path.dirname(os.path.abspath(__file__))
angel_script = os.path.join(script_dir, "Angelmasterlist.py")

try:
    subprocess.run(["python", angel_script], check=True)
except subprocess.CalledProcessError as e:
    print(f"❌ Failed to run Angelmasterlist.py: {e}")

# API Rate Limits
MAX_REQUESTS_PER_SEC = 3
BACKOFF_MULTIPLIER = 2
MAX_BACKOFF = 30  # Maximum wait time

symbol_queue = queue.Queue()
all_data = []

pd.set_option('future.no_silent_downcasting', True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Google Sheets Integration
SHEET_NAME = "1hrST"

# Log file for sent alerts
LOG_FILE = "sent_alerts.log"

# ✅ Define IST timezone
IST_TZ = pytz.timezone("Asia/Kolkata")

warnings.filterwarnings('ignore')

OUTPUT_FILE = "AngelFutCIA.csv"

def authenticate_google_sheets():
    """Authenticate with Google Sheets API using service account credentials"""
    if not CREDENTIALS_FILE:
        logging.error("GOOGLE_SHEETS_CREDENTIALS environment variable is not set.")
        return None
    
    try:
        # Parse the JSON credentials string from environment variable
        credentials_info = json.loads(CREDENTIALS_FILE)
        credentials = Credentials.from_service_account_info(
            credentials_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        logging.error(f"Failed to authenticate with Google Sheets: {e}")
        return None

def load_sent_alerts():
    """Load previously sent alerts to avoid duplicates"""
    if not os.path.exists(LOG_FILE):
        return set()
    
    with open(LOG_FILE, 'r') as f:
        return set(line.strip() for line in f)

def send_telegram_alert(message):
    """Send alert message via Telegram"""
    # This function would need to be implemented if you want Telegram alerts
    logging.info(f"Would send Telegram alert: {message}")
    # Implement actual Telegram API call here if needed

def row_hash(row):
    """Generate a unique hash for a DataFrame row by concatenating all its values."""
    row_string = ''.join(map(str, row.values))
    return hashlib.sha256(row_string.encode()).hexdigest()

def filter_valid_rows(df, today_date, now):
    """Filter rows based on timestamp validity"""
    if 'timestamp' not in df.columns:
        return pd.DataFrame()
    
    try:
        # Convert timestamp to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Get entries from today
        today_mask = df['timestamp'].dt.strftime('%Y-%m-%d') == today_date
        return df[today_mask].copy()
    except Exception as e:
        logging.error(f"Error filtering rows by date: {e}")
        return pd.DataFrame()

def upload_to_gsheet(sheet, data_to_append):
    """Upload data to Google Sheet"""
    if not data_to_append:
        logging.info("No data to append to Google Sheets")
        return
    
    try:
        # Clear existing data
        sheet.clear()
        logging.info("Existing data cleared from the sheet.")
        
        # Append new data
        sheet.append_rows(data_to_append)
        logging.info(f"Successfully uploaded {len(data_to_append)} rows to Google Sheets")
    except Exception as e:
        logging.error(f"Failed to upload data to Google Sheets: {e}")
        send_telegram_alert(f"Error uploading data to Google Sheets: {e}")

def send_alerts(filtered_df, sent_alerts):
    """Send alerts for new signals"""
    for _, row in filtered_df.iterrows():
        alert_id = f"{row['symbol']}_{row['timestamp']}_{row['Signal']}"
        
        if alert_id not in sent_alerts:
            message = f"🔔 {row['Signal']} signal for {row['symbol']} at {row['timestamp']}"
            send_telegram_alert(message)
            
            # Log the sent alert
            with open(LOG_FILE, 'a') as f:
                f.write(f"{alert_id}\n")
            
            sent_alerts.add(alert_id)

def save_to_google_sheets(filtered_data):
    """Save filtered data to Google Sheets"""
    # Authenticate and connect to Google Sheets
    client = authenticate_google_sheets()
    if not client:
        logging.error("Failed to authenticate with Google Sheets")
        return
    
    try:
        sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    except Exception as e:
        logging.error(f"Failed to access Google Sheets worksheet: {e}")
        send_telegram_alert(f"Error accessing Google Sheets: {e}")
        return

    # Prepare data to append
    data_to_append = []
    headers_written = False
    sent_alerts = load_sent_alerts()

    # Get today's date and the current time with timezone
    kolkata_tz = pytz.timezone('Asia/Kolkata')
    today_date = datetime.today().strftime('%Y-%m-%d')
    now = pd.to_datetime(datetime.now())

    for item in filtered_data:
        df = item['data']
        filtered_df = df[df['Bull'] | df['Bear'] | df['BullContinue'] | df['BearContinue'] | df['Dem_60/55'] | df['Cam_CIA'] | df['Cam_15/30i']]
        
        # Safely convert timestamp and filter rows                                                 
        filtered_df = filter_valid_rows(filtered_df, today_date, now)

        if not filtered_df.empty:
            # Replace NaN values with empty strings
            filtered_df = filtered_df.fillna("")

            # Prepare data for Google Sheets
            filtered_df['timestamp'] = filtered_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

            if not headers_written:
                data_to_append.append(filtered_df.columns.tolist())
                headers_written = True

            data_to_append.extend(filtered_df.values.tolist())

            # Send alerts after data is successfully uploaded
            send_alerts(filtered_df, sent_alerts)

    # Upload data to Google Sheets
    upload_to_gsheet(sheet, data_to_append)

def fetch_symbols_from_csv(file_path="Angel_MasterList.csv", column_name="symbol"):
    """Fetch symbol list from CSV file"""
    try:
        df = pd.read_csv(file_path)
        symbols = df[column_name].dropna().astype(str).str.strip().tolist()
        print(f"✅ Fetched {len(symbols)} symbols from {file_path}")
        return symbols
    except Exception as e:
        print(f"❌ Error reading symbols: {e}")
        return []

def initializeSymbolTokenMap():
    """Initialize symbol to token mapping from Angel Broking"""
    global TOKEN_MAP
    url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
    d = requests.get(url).json()
    token_df = pd.DataFrame.from_dict(d)
    token_df['expiry'] = pd.to_datetime(token_df['expiry'])
    token_df = token_df.astype({'strike': float})
    TOKEN_MAP = token_df
    print("✅ Symbol token map initialized")

def getTokenInfo(symbol):
    """Get token information for a symbol"""
    global TOKEN_MAP
    if TOKEN_MAP is None:
        print("❌ Token map not initialized")
        return None

    result = TOKEN_MAP[TOKEN_MAP['symbol'] == symbol]

    if result.empty:
        print(f"⚠️ Token not found for {symbol}")
        return None

    return result.iloc[0]['token']

def calculate_indicators(df, symbol):
    """Calculate basic technical indicators"""
    if df.empty:
        print(f"❌ No data available for {symbol}")
        return None

    # ✅ Convert timestamp to IST
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df['timestamp'] = df['timestamp'].dt.tz_convert(IST_TZ)

    df.sort_values('timestamp', inplace=True)
    df.fillna(method='ffill', inplace=True)

    if len(df) < 50:  # Ensure enough data for calculations
        print(f"⚠️ Not enough data for indicators for {symbol}")
        return None

    # ✅ Calculate Technical Indicators
    df['RSI'] = ta_lib.momentum.RSIIndicator(df['C'], window=14).rsi()
    df['ATR'] = ta_lib.volatility.AverageTrueRange(df['H'], df['L'], df['C'], window=20).average_true_range()
    df['ADX'] = ta_lib.trend.ADXIndicator(df['H'], df['L'], df['C'], window=14).adx()
    
    df['EMA_20'] = ta_lib.trend.EMAIndicator(df['C'], window=20).ema_indicator()
    df['EMA_50'] = ta_lib.trend.EMAIndicator(df['C'], window=50).ema_indicator()
    df['EMA_200'] = ta_lib.trend.EMAIndicator(df['C'], window=200).ema_indicator()
    
    df['VWAP'] = ta_lib.volume.VolumeWeightedAveragePrice(df['H'], df['L'], df['C'], df['V']).volume_weighted_average_price()

    df['symbol'] = symbol  # Add symbol column for merging

    df.fillna(0, inplace=True)  # Replace NaNs with 0

    return df

def calculate_rvi(df, period=10, len_smooth=14):
    """Calculate Relative Vigor Index"""
    src = df['close']
    stddev = src.rolling(window=period).std()

    up_vol = np.where(src.diff() > 0, stddev, 0)
    down_vol = np.where(src.diff() <= 0, stddev, 0)

    up_ema = pd.Series(up_vol, index=df.index).ewm(span=len_smooth, adjust=False).mean()
    down_ema = pd.Series(down_vol, index=df.index).ewm(span=len_smooth, adjust=False).mean()

    df['RVI'] = 100 * (up_ema / (up_ema + down_ema))
    df['RVI'] = df['RVI'].ffill()
    return df

def calculate_chaikin_volatility(df, ema_period=10, change_period=10):
    """Calculate Chaikin Volatility"""
    # Step 1: EMA of the high-low range
    hl_range = df['high'] - df['low']
    ema_hl = hl_range.ewm(span=ema_period, adjust=False).mean()

    # Step 2: % change over change_period days
    chaikin_volatility = ema_hl.pct_change(periods=change_period) * 100

    df['ChaikinVolatility'] = chaikin_volatility.fillna(0)
    return df

def calculate_supertrend(df, period=10, multiplier=3):
    """Calculate Supertrend Indicator"""
    # Rename columns to lowercase for pandas_ta compatibility
    df = df.rename(columns={'H': 'high', 'L': 'low', 'C': 'close', 'O': 'open'})

    df.ta.supertrend(length=period, multiplier=multiplier, append=True)

    # pandas_ta creates 'SUPERT_10_3.0' and 'SUPERTd_10_3.0' columns
    supertrend_col = f"SUPERT_{period}_{multiplier}.0"
    
    if supertrend_col in df.columns:
        df.rename(columns={supertrend_col: 'Supertrend'}, inplace=True)
    else:
        print(f"⚠️ Supertrend column {supertrend_col} not found in DataFrame.")

    return df

def calculate_camarilla_pivots(df):
    """Calculate Camarilla Pivot Points"""
    if df.empty:
        print("⚠️ DataFrame is empty, skipping Camarilla pivot calculation.")
        return df

    # Convert timestamp to datetime and extract date
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date'] = df['timestamp'].dt.date  # Extract date only

    # Rename columns to match expected names
    rename_dict = {'H': 'high', 'L': 'low', 'C': 'close', 'O': 'open'}
    df.rename(columns=rename_dict, inplace=True)

    # Check if required columns are present after renaming
    if not all(col in df.columns for col in ['high', 'low', 'close']):
        print("⚠️ Missing required columns for Camarilla calculation: 'high', 'low', 'close'")
        return df

    # Aggregating to get previous day's high, low, and close
    daily_df = df.groupby('date').agg({'high': 'max', 'low': 'min', 'close': 'last'}).dropna()
    daily_df = daily_df.shift(1)  # Shift by 1 day to use yesterday's values

    if daily_df.empty:
        print("⚠️ No valid daily data after shifting, skipping Camarilla calculation.")
        return df

    # Camarilla Pivot Calculations
    daily_df['PP'] = (daily_df['high'] + daily_df['low'] + daily_df['close']) / 3
    daily_df['R1'] = daily_df['close'] + (1.1 / 12) * (daily_df['high'] - daily_df['low'])
    daily_df['R2'] = daily_df['close'] + (1.1 / 6) * (daily_df['high'] - daily_df['low'])
    daily_df['R3'] = daily_df['close'] + (1.1 / 4) * (daily_df['high'] - daily_df['low'])
    daily_df['R4'] = daily_df['close'] + (1.1 / 2) * (daily_df['high'] - daily_df['low'])
    daily_df['S1'] = daily_df['close'] - (1.1 / 12) * (daily_df['high'] - daily_df['low'])
    daily_df['S2'] = daily_df['close'] - (1.1 / 6) * (daily_df['high'] - daily_df['low'])
    daily_df['S3'] = daily_df['close'] - (1.1 / 4) * (daily_df['high'] - daily_df['low'])
    daily_df['S4'] = daily_df['close'] - (1.1 / 2) * (daily_df['high'] - daily_df['low'])

    # Reset index for merging
    daily_df.reset_index(inplace=True)

    # Merge with 15-minute data on date to apply previous day's pivots
    df = df.merge(daily_df[['date', 'PP', 'R1', 'R2', 'R3', 'R4', 'S1', 'S2', 'S3', 'S4']], on="date", how="left")

    return df

def calculate_weekly_camarilla_pivots(df):
    """Calculate Weekly Camarilla Pivot Points"""
    if df.empty:
        print("⚠️ DataFrame is empty, skipping Camarilla pivot calculation.")
        return df

    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Rename columns to match expected names
    rename_dict = {'H': 'high', 'L': 'low', 'C': 'close', 'O': 'open'}
    df.rename(columns=rename_dict, inplace=True)

    # Extract week start date
    df['week'] = df['timestamp'].dt.to_period('W').apply(lambda r: r.start_time)  # Get the start of the week

    # Aggregating high, low, and close prices by week
    weekly_df = df.groupby('week').agg({'high': 'max', 'low': 'min', 'close': 'last'}).dropna()

    # Camarilla Pivot Calculations
    weekly_df['W_PP'] = (weekly_df['high'] + weekly_df['low'] + weekly_df['close']) / 3
    weekly_df['W_R1'] = weekly_df['close'] + (1.1 / 12) * (weekly_df['high'] - weekly_df['low'])
    weekly_df['W_R2'] = weekly_df['close'] + (1.1 / 6) * (weekly_df['high'] - weekly_df['low'])
    weekly_df['W_R3'] = weekly_df['close'] + (1.1 / 4) * (weekly_df['high'] - weekly_df['low'])
    weekly_df['W_R4'] = weekly_df['close'] + (1.1 / 2) * (weekly_df['high'] - weekly_df['low'])
    weekly_df['W_S1'] = weekly_df['close'] - (1.1 / 12) * (weekly_df['high'] - weekly_df['low'])
    weekly_df['W_S2'] = weekly_df['close'] - (1.1 / 6) * (weekly_df['high'] - weekly_df['low'])
    weekly_df['W_S3'] = weekly_df['close'] - (1.1 / 4) * (weekly_df['high'] - weekly_df['low'])
    weekly_df['W_S4'] = weekly_df['close'] - (1.1 / 2) * (weekly_df['high'] - weekly_df['low'])

    # Reset index for merging with the original DataFrame
    weekly_df.reset_index(inplace=True)

    # Merging the Camarilla data back into the original DataFrame
    df = df.merge(weekly_df[['week', 'W_PP', 'W_R1', 'W_R2', 'W_R3', 'W_R4', 'W_S1', 'W_S2', 'W_S3', 'W_S4']], on="week", how="left")
    
    return df

def calculate_weekly_demark_pivots(df):
    """Calculate Weekly DeMark Pivot Points"""
    if df.empty:
        print("⚠️ DataFrame is empty, skipping DeMark pivot calculation.")
        return df

    # Ensure timestamp is in datetime format and normalize to weekly buckets
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['week'] = df['timestamp'].dt.to_period('W').apply(lambda r: r.start_time)

    # Compute previous week's OHLC data
    weekly_summary = (
        df.groupby('week')
        .agg(
            prev_high=('high', 'max'),
            prev_low=('low', 'min'),
            prev_close=('close', 'last'),
            prev_open=('open', 'first')
        )
        .shift(1)  # Use previous week's data
        .bfill()   # In case first week has no prior, backfill
        .reset_index()
    )

    # Merge into main DataFrame
    df = df.merge(weekly_summary, on='week', how='left')
    df[['prev_high', 'prev_low', 'prev_close', 'prev_open']] = df[['prev_high', 'prev_low', 'prev_close', 'prev_open']].ffill()

    # Apply DeMark conditional logic
    conditions = [
        df['prev_close'] < df['prev_open'],
        df['prev_close'] > df['prev_open'],
        df['prev_close'] == df['prev_open']
    ]
    values = [
        df['prev_high'] + (2 * df['prev_low']) + df['prev_close'],
        (2 * df['prev_high']) + df['prev_low'] + df['prev_close'],
        df['prev_high'] + df['prev_low'] + (2 * df['prev_close'])
    ]
    df['X'] = np.select(conditions, values)

    # DeMark Pivot Points
    df['PP_Demark'] = df['X'] / 4
    df['R1_Demark'] = (df['X'] / 2) - df['prev_low']
    df['S1_Demark'] = (df['X'] / 2) - df['prev_high']

    # Optional: Format week for display
    df['week'] = df['week'].dt.strftime('%Y-%m-%d')

    return df

def getExchangeSegment(symbol):
    """Get exchange segment for a symbol"""
    global TOKEN_MAP
    if TOKEN_MAP is None:
        return "NSE"  # default fallback
        
    result = TOKEN_MAP[TOKEN_MAP['symbol'] == symbol]
    if result.empty:
        return "NSE"  # default fallback
    return result.iloc[0]['exch_seg']  # Should return 'NSE', 'NFO', etc.

def apply_bull_bear_conditions(df):
    """Apply bull/bear pattern recognition conditions"""
    required_cols = ['close', 'Supertrend', 'R1_Demark', 'S1_Demark', 'ChaikinVolatility', 'RVI']
    for col in required_cols:
        if col not in df.columns:
            print(f"❌ Missing column '{col}' for bull/bear condition check.")
            return df

    # Fresh Bullish breakout
    bull_condition = (
        (df['close'] > df['Supertrend']) &
        (df['close'] > df['R1_Demark']) &
        (df['close'].shift(1) < df['R1_Demark']) &
        (df['ChaikinVolatility'] > 0) &
        (df['RVI'] > 50)
    )

    # Fresh Bearish breakdown
    bear_condition = (
        (df['close'] < df['Supertrend']) &
        (df['close'] < df['S1_Demark']) &
        (df['close'].shift(1) > df['S1_Demark']) &
        (df['RVI'] < 50)
    )

    # Continuation signals
    bull_continue = (
        (df['close'] > df['Supertrend']) &
        (df['close'] > df['R1_Demark'])
    )

    bear_continue = (
        (df['close'] < df['Supertrend']) &
        (df['close'] < df['S1_Demark'])
    )

    # Initialize boolean columns for filtering
    df['Bull'] = bull_condition
    df['Bear'] = bear_condition
    df['BullContinue'] = bull_continue & ~bull_condition
    df['BearContinue'] = bear_continue & ~bear_condition
    
    # For compatibility with existing filters, also need these columns
    df['Dem_60/55'] = False  # Placeholder
    df['Cam_CIA'] = False    # Placeholder
    df['Cam_15/30i'] = False # Placeholder

    # Initialize signal column
    df['Signal'] = 'Neutral'
    df.loc[bull_condition, 'Signal'] = 'Bull'
    df.loc[bear_condition, 'Signal'] = 'Bear'
    df.loc[(bull_continue) & (df['Signal'] == 'Neutral'), 'Signal'] = 'BullContinue'
    df.loc[(bear_continue) & (df['Signal'] == 'Neutral'), 'Signal'] = 'BearContinue'

    return df

def getHistoricalAPI(symbol, token, interval='ONE_HOUR'):
    """Fetch historical price data from Angel Broking API"""
    global SMART_API_OBJ
    if SMART_API_OBJ is None:
        print("❌ Smart API object not initialized")
        return None
        
    # ✅ Ensure correct market hours: 9:15 AM - 3:30 PM IST
    today_ist = datetime.now(IST_TZ)
    to_date = today_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    from_date = to_date - timedelta(days=15)

    from_date_format = from_date.strftime("%Y-%m-%d 09:15")
    to_date_format = to_date.strftime("%Y-%m-%d 15:30")

    print(f"📅 Fetching data for {symbol} from {from_date_format} to {to_date_format}")

    if not token or pd.isna(token):
        print(f"❌ Error: Invalid token ({token}) for {symbol}")
        return None
        
    exchange = getExchangeSegment(symbol)
    for attempt in range(3):
        try:
            historicParam = {
                "exchange": exchange,
                "symboltoken": str(token),
                "interval": interval,
                "fromdate": from_date_format,
                "todate": to_date_format
            }

            response = SMART_API_OBJ.getCandleData(historicParam)

            if not response or 'data' not in response or not response['data']:
                print(f"⚠️ API returned empty data for {symbol}. Retrying... (Attempt {attempt + 1}/3)")
                sleep(2)
                continue

            df = pd.DataFrame(response['data'], columns=['timestamp', 'O', 'H', 'L', 'C', 'V'])
            df = calculate_indicators(df, symbol)  # Basic indicators
            
            if df is not None:
                df = calculate_supertrend(df) 
                df = calculate_camarilla_pivots(df) 
                df = calculate_weekly_camarilla_pivots(df) 
                df = calculate_weekly_demark_pivots(df)
                df = calculate_chaikin_volatility(df)
                df = calculate_rvi(df)
                df = apply_bull_bear_conditions(df)
                return df

        except Exception as e:
            print(f"❌ Error fetching data for {symbol}: {str(e)}")
            sleep(2)
            continue

    print(f"❌ API failed for {symbol} after 3 attempts.")
    return None

def fetch_data(symbol):
    """Fetch historical data with retry handling and token validation."""
    token = getTokenInfo(symbol)

    if not token or pd.isna(token):
        print(f"⚠️ No token found for {symbol}, skipping.")
        return

    attempt = 0
    backoff = 1  # Initial backoff delay

    while attempt < 3:
        df = getHistoricalAPI(symbol, token)

        if df is not None:
            all_data.append({'symbol': symbol, 'data': df})
            print(f"✅ Successfully fetched data for {symbol}")
            return

        attempt += 1
        print(f"🔄 Retrying {symbol} in {backoff}s (Attempt {attempt}/3)")
        time.sleep(backoff)
        backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF)

    print(f"❌ Failed to fetch data for {symbol} after 3 attempts")

def worker():
    """Worker function for the thread pool"""
    while not symbol_queue.empty():
        symbol = symbol_queue.get()
        try:
            fetch_data(symbol)
        except Exception as e:
            print(f"❌ Error processing {symbol}: {str(e)}")
        finally:
            symbol_queue.task_done()

if __name__ == '__main__':
    initializeSymbolTokenMap()
    
    # Try to load symbols
    SYMBOL_LIST = fetch_symbols_from_csv()
    if not SYMBOL_LIST:
        print("❌ Failed to load symbols from CSV file")
        exit(1)

    # Get credentials directly from

    # Get credentials directly from environment variables
    username = os.getenv("USER_NAME")
    api_key = os.getenv("API_KEY")
    password = os.getenv("PWD")
    totp_secret = os.getenv("TOTP_SECRET")
    
    # Validate that all credentials are present
    if not all([username, api_key, password, totp_secret]):
        print("❌ Missing credential(s) in .env file. Please check your configuration.")
        print(f"USER_NAME: {'✓' if username else '✗'}")
        print(f"API_KEY: {'✓' if api_key else '✗'}")
        print(f"PWD: {'✓' if password else '✗'}")
        print(f"TOTP_SECRET: {'✓' if totp_secret else '✗'}")
        exit()
    
    try:
        totp = pyotp.TOTP(totp_secret).now()
        obj = SmartConnect(api_key=api_key)
        data = obj.generateSession(username, password, totp)
        credentials.SMART_API_OBJ = obj  # Store for use in other functions
    except Exception as e:
        print(f"❌ Login failed: {str(e)}")
        exit()
        
   # Add symbols to queue
    for symbol in SYMBOL_LIST:
        symbol_queue.put(symbol)

    worker()  # Start fetching data

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df['timestamp'] = pd.to_datetime(final_df['timestamp'], utc=True).dt.tz_convert(IST_TZ)
        final_df.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Data saved to {OUTPUT_FILE}")

    print("✅ Data collection completed.")
