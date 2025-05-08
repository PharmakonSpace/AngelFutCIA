from SmartApi import SmartConnect
import pandas as pd
import pandas_ta as ta
import ta
from datetime import datetime, timedelta
import requests
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
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Run Angelmasterlist.py
script_dir = os.path.dirname(os.path.abspath(__file__))
angel_script = os.path.join(script_dir, "Angelmasterlist.py")

try:
    subprocess.run(["python", angel_script], check=True)
except subprocess.CalledProcessError as e:
    print(f"❌ Failed to run Angelmasterlist.py: {e}")
    exit()

# Access credentials from environment variables
API_KEY = os.getenv('API_KEY')
USER_NAME = os.getenv('USER_NAME')
PWD = os.getenv('PWD')
TOTP_SECRET = os.getenv('TOTP_SECRET')

# Global variables
SMART_API_OBJ = None
TOKEN_MAP = None

# Google Sheets Credentials Setup
def fetch_symbols_from_csv(file_path="Angel_MasterList.csv", column_name="symbol"):
    try:
        df = pd.read_csv(file_path)
        symbols = df[column_name].dropna().astype(str).str.strip().tolist()
        print(f"✅ Fetched {len(symbols)} symbols from {file_path}")
        return symbols
    except Exception as e:
        print(f"❌ Error reading symbols: {e}")
        return []

# API Rate Limits
MAX_REQUESTS_PER_SEC = 3
BACKOFF_MULTIPLIER = 2
MAX_BACKOFF = 30  # Maximum wait time

symbol_queue = queue.Queue()
all_data = []

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
            all_data.append(df)
            print(f"✅ Successfully fetched data for {symbol}")
            return

        attempt += 1
        print(f"🔄 Retrying {symbol} in {backoff}s (Attempt {attempt}/3)")
        time.sleep(backoff)
        backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF)

    print(f"❌ Failed to fetch data for {symbol} after 3 attempts")


def worker():
    while not symbol_queue.empty():
        symbol = symbol_queue.get()
        fetch_data(symbol)
        symbol_queue.task_done()

# Using threads
with ThreadPoolExecutor(max_workers=MAX_REQUESTS_PER_SEC) as executor:
    for _ in range(MAX_REQUESTS_PER_SEC):
        executor.submit(worker)

symbol_queue.join()


# Fetch SYMBOL_LIST from Google Sheets
SYMBOL_LIST = fetch_symbols_from_csv()
print(f"✅ SYMBOL_LIST fetched: {SYMBOL_LIST}")

# ✅ Define IST timezone
IST_TZ = pytz.timezone("Asia/Kolkata")

warnings.filterwarnings('ignore')

#SYMBOL_LIST = ['CDSL', 'IEX']
OUTPUT_FILE = "AngelFutCIA.csv"

def initializeSymbolTokenMap():
    url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
    d = requests.get(url).json()
    global TOKEN_MAP
    TOKEN_MAP = pd.DataFrame.from_dict(d)
    TOKEN_MAP['expiry'] = pd.to_datetime(TOKEN_MAP['expiry'])
    TOKEN_MAP = TOKEN_MAP.astype({'strike': float})

def getTokenInfo(symbol):
    global TOKEN_MAP
    if TOKEN_MAP is None:
        print("⚠️ Token map not initialized")
        return None
        
    result = TOKEN_MAP[TOKEN_MAP['symbol'] == symbol]

    if result.empty:
        print(f"⚠️ Token not found for {symbol}")
        return None

    return result.iloc[0]['token']


def calculate_indicators(df, symbol):
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
    df['RSI'] = ta.momentum.RSIIndicator(df['C'], window=14).rsi()
    df['ATR'] = ta.volatility.AverageTrueRange(df['H'], df['L'], df['C'], window=20).average_true_range()
    df['ADX'] = ta.trend.ADXIndicator(df['H'], df['L'], df['C'], window=14).adx()
    
    df['EMA_20'] = ta.trend.EMAIndicator(df['C'], window=20).ema_indicator()
    df['EMA_50'] = ta.trend.EMAIndicator(df['C'], window=50).ema_indicator()
    df['EMA_200'] = ta.trend.EMAIndicator(df['C'], window=200).ema_indicator()
    
    df['VWAP'] = ta.volume.VolumeWeightedAveragePrice(df['H'], df['L'], df['C'], df['V']).volume_weighted_average_price()

   
    df['symbol'] = symbol  # Add symbol column for merging

    df.fillna(0, inplace=True)  # Replace NaNs with 0

    return df

def calculate_rvi(df, period=10, len_smooth=14):
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
    # Step 1: EMA of the high-low range
    hl_range = df['high'] - df['low']
    ema_hl = hl_range.ewm(span=ema_period, adjust=False).mean()

    # Step 2: % change over `change_period` days
    chaikin_volatility = ema_hl.pct_change(periods=change_period) * 100

    df['ChaikinVolatility'] = chaikin_volatility.fillna(0)
    return df


# Calculate Supertrend Indicator
def calculate_supertrend(df, period=10, multiplier=3):
    # Rename columns to lowercase for pandas_ta compatibility
    df = df.rename(columns={'H': 'high', 'L': 'low', 'C': 'close'})

    df.ta.supertrend(length=period, multiplier=multiplier, append=True)

    # pandas_ta creates 'SUPERT_10_3.0' and 'SUPERTd_10_3.0' columns
    supertrend_col = f"SUPERT_{period}_{multiplier}.0"
    
    if supertrend_col in df.columns:
        df.rename(columns={supertrend_col: 'Supertrend'}, inplace=True)
    else:
        print(f"⚠️ Supertrend column {supertrend_col} not found in DataFrame.")

    return df

def calculate_camarilla_pivots(df):
    if df.empty:
        print("⚠️ DataFrame is empty, skipping Camarilla pivot calculation.")
        return df

    # Print column names before renaming to ensure they match
    print("Columns before renaming:", df.columns)

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
    if df.empty:
        print("⚠️ DataFrame is empty, skipping Camarilla pivot calculation.")
        return df

    # Print column names before renaming to ensure they match
    print("Columns before renaming:", df.columns)

    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Rename columns to match expected names
    rename_dict = {'H': 'high', 'L': 'low', 'C': 'close', 'O': 'open'}
    df.rename(columns=rename_dict, inplace=True)

    # Check if required columns are present after renaming
    if not all(col in df.columns for col in ['high', 'low', 'close']):
        print("⚠️ Missing required columns for Camarilla calculation: 'high', 'low', 'close'")
        return df

    # Extract week start date
    df['week'] = df['timestamp'].dt.to_period('W').apply(lambda r: r.start_time)  # Get the start of the week

    # Print the week column values to ensure correct extraction
    print("Week column values:", df['week'].head())

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

    # Debug: Print weekly data to verify it's correctly calculated
    print(f"✅ Weekly Camarilla Data:\n{weekly_df.head()}")
    
    return df

def calculate_weekly_demark_pivots(df):
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

    print("✅ DeMark Pivots calculated based on previous week's OHLC.")
    return df



def getExchangeSegment(symbol):
    global TOKEN_MAP
    if TOKEN_MAP is None:
        return "NSE"  # default fallback
        
    result = TOKEN_MAP[TOKEN_MAP['symbol'] == symbol]
    if result.empty:
        return "NSE"  # default fallback
    return result.iloc[0]['exch_seg']  # Should return 'NSE', 'NFO', etc.



def apply_bull_bear_conditions(df):
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
        #(df['ChaikinVolatility'] < 0) &
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

    # Initialize signal column
    df['Signal'] = 'Neutral'

    df.loc[bull_condition, 'Signal'] = 'Bull'
    df.loc[bear_condition, 'Signal'] = 'Bear'
    df.loc[(bull_continue) & (df['Signal'] == 'Neutral'), 'Signal'] = 'BullContinue'
    df.loc[(bear_continue) & (df['Signal'] == 'Neutral'), 'Signal'] = 'BearContinue'

    # Remove Neutral rows
    df = df[df['Signal'] != 'Neutral']

    return df



def getHistoricalAPI(symbol, token, interval='ONE_HOUR'):
    # ✅ Ensure correct market hours: 9:15 AM - 3:30 PM IST
    today_ist = datetime.now(IST_TZ)
    to_date = today_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    
    # Reduce the data range to 7 days to avoid timeouts/limits
    from_date = to_date - timedelta(days=7)

    # Format date strings according to Angel API requirements
    # Format should be YYYY-MM-DD HH:MM with leading zeros
    from_date_format = from_date.strftime("%Y-%m-%d 09:15")
    to_date_format = to_date.strftime("%Y-%m-%d 15:30")

    print(f"📅 Fetching data for {symbol} from {from_date_format} to {to_date_format}")

    if not token or pd.isna(token):
        print(f"❌ Error: Invalid token ({token}) for {symbol}")
        return None
        
    exchange = getExchangeSegment(symbol)
    
    # Verify SMART_API_OBJ is authenticated before proceeding
    global SMART_API_OBJ
    if SMART_API_OBJ is None:
        print("❌ API client not initialized")
        return None
        
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

            # Check for authentication errors
            if response and isinstance(response, dict) and response.get('message') == 'Invalid Token':
                print(f"🔑 Session expired. Attempting to reconnect...")
                if reconnect_api():
                    print(f"🔄 Retrying request after reconnection for {symbol}")
                    response = SMART_API_OBJ.getCandleData(historicParam)
                else:
                    print(f"❌ Could not reconnect. Aborting request for {symbol}")
                    return None

            if not response or 'data' not in response or not response['data']:
                print(f"⚠️ API returned empty data for {symbol}. Retrying... (Attempt {attempt + 1}/3)")
                sleep(5)  # Increase sleep time to avoid rate limits
                continue

            df = pd.DataFrame(response['data'], columns=['timestamp', 'O', 'H', 'L', 'C', 'V'])
            df = calculate_indicators(df, symbol)  # Ensure this modifies df
            df = calculate_supertrend(df) 
            df = calculate_camarilla_pivots(df) 
            df = calculate_weekly_camarilla_pivots(df) 
            df = calculate_weekly_demark_pivots(df)
            df = calculate_chaikin_volatility(df)
            df=  calculate_rvi(df)
            df = apply_bull_bear_conditions(df)
            return df

        except Exception as e:
            print(f"❌ Error fetching data for {symbol}: {str(e)}")
            return None

    print(f"❌ API failed for {symbol} after 3 attempts.")
    return None


def reconnect_api():
    """Reconnect to the Angel One API when the session expires."""
    logger.info("🔄 Attempting to reconnect to Angel One API...")
    
    try:
        totp = pyotp.TOTP(TOTP_SECRET).now()
        obj = SmartConnect(api_key=API_KEY)
        
        # Use MPIN instead of password for login
        data = obj.generateSessionByMPIN(USER_NAME, MPIN, totp)
        
        if data and isinstance(data, dict) and data.get('status'):
            global SMART_API_OBJ
            SMART_API_OBJ = obj
            logger.info("✅ Successfully reconnected to API")
            return True
        else:
            error_msg = data.get('message') if data and isinstance(data, dict) else "Unknown error"
            logger.error(f"❌ Reconnection failed: {error_msg}")
            return False
    except Exception as e:
        logger.error(f"❌ Reconnection failed with exception: {str(e)}")
        return False


if __name__ == '__main__':
    # Check if environment variables are properly loaded
    required_env_vars = ['API_KEY', 'USER_NAME', 'MPIN', 'TOTP_SECRET']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please create a .env file with the required credentials")
        exit(1)
    
    initializeSymbolTokenMap()

    try:
        totp = pyotp.TOTP(TOTP_SECRET).now()
        logger.info(f"✅ Generated TOTP: {totp}")
    except Exception as e:
        logger.error(f"❌ TOTP generation failed: {str(e)}")
        exit(1)

    obj = SmartConnect(api_key=API_KEY)
    try:
        # Use MPIN instead of password
        data = obj.generateSessionByMPIN(USER_NAME, MPIN, totp)
        
        if not data or not isinstance(data, dict):
            logger.error("❌ Authentication failed: Invalid response format")
            exit(1)
            
        if not data.get('status'):
            logger.error(f"❌ Authentication failed: {data.get('message', 'Unknown error')}")
            exit(1)
        
        refresh_token = data.get('data', {}).get('refreshToken')
        if not refresh_token:
            logger.error("❌ No refresh token in response")
            exit(1)
            
        SMART_API_OBJ = obj
        logger.info(f"✅ Login successful. Session token generated.")
        
        # Fetch user profile to verify login
        try:
            user_profile = obj.getProfile(refresh_token)
            if user_profile and user_profile.get('data', {}).get('name'):
                logger.info(f"✅ Authenticated as: {user_profile['data']['name']}")
            else:
                logger.warning("⚠️ Authenticated but couldn't retrieve profile name")
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch profile: {e}")
            
    except Exception as e:
        logger.error(f"❌ Login failed: {str(e)}")
        exit(1)

   
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
