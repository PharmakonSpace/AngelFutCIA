import requests
from SmartApi import SmartConnect
import pandas as pd
import pandas_ta as ta
import numpy as np
from datetime import datetime, timedelta
import pyotp
import warnings
import os
import pytz
import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor
import subprocess
import json
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("angel_api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Load environment variables from .env file
load_dotenv()

# Run Angelmasterlist.py
script_dir = os.path.dirname(os.path.abspath(__file__))
angel_script = os.path.join(script_dir, "Angelmasterlist.py")

try:
    subprocess.run(["python", angel_script], check=True)
    logger.info("✅ Successfully ran Angelmasterlist.py")
except subprocess.CalledProcessError as e:
    logger.error(f"❌ Failed to run Angelmasterlist.py: {e}")
    exit()

# Global variables
TOKEN_MAP = None  # Will be initialized with token mapping data
SMART_API_OBJ = None
IST_TZ = pytz.timezone("Asia/Kolkata")
OUTPUT_FILE = "AngelFutCIA.csv"
symbol_queue = queue.Queue()
all_data = []

# API Rate Limits
MAX_REQUESTS_PER_SEC = 2  # Reduced to avoid rate limiting
BACKOFF_MULTIPLIER = 2
MAX_BACKOFF = 30  # Maximum wait time

def fetch_symbols_from_csv(file_path="Angel_MasterList.csv", column_name="symbol"):
    """Load symbols from CSV file"""
    try:
        df = pd.read_csv(file_path)
        symbols = df[column_name].dropna().astype(str).str.strip().tolist()
        logger.info(f"✅ Fetched {len(symbols)} symbols from {file_path}")
        return symbols
    except Exception as e:
        logger.error(f"❌ Error reading symbols: {e}")
        return []

def authenticate_api():
    """Authenticate with Angel API and handle token generation"""
    global SMART_API_OBJ
    
    # Fetch credentials from environment variables
    API_KEY = os.getenv('API_KEY')
    USER_NAME = os.getenv('USER_NAME')
    PWD = os.getenv('PWD')
    TOTP_SECRET = os.getenv('TOTP_SECRET')
    
    # Check if all required environment variables are set
    if not all([API_KEY, USER_NAME, PWD, TOTP_SECRET]):
        logger.error("❌ Missing required environment variables. Please check your .env file.")
        logger.error("Required variables: API_KEY, USER_NAME, PWD, TOTP_SECRET")
        return False
    
    try:
        totp = pyotp.TOTP(TOTP_SECRET).now()
        logger.info(f"Generated TOTP: {totp}")
    except Exception as e:
        logger.error(f"❌ TOTP generation failed: {str(e)}")
        return False

    obj = SmartConnect(api_key=API_KEY)
    try:
        data = obj.generateSession(USER_NAME, PWD, totp)
        
        # Verify if login was successful
        if data.get('status'):
            SMART_API_OBJ = obj
            logger.info(f"✅ Successfully logged in. User: {data.get('data', {}).get('name')}")
            # Get feed token for historical data
            feed_token = obj.getfeedToken()
            if feed_token:
                logger.info("✅ Successfully obtained feed token")
            else:
                logger.warning("⚠️ Failed to get feed token, some functionality may be limited")
            return True
        else:
            logger.error(f"❌ Login failed: {data.get('message')}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Login error: {str(e)}")
        return False

def initializeSymbolTokenMap():
    """Initialize token mapping for symbols"""
    global TOKEN_MAP
    try:
        url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
        response = requests.get(url)
        
        if response.status_code == 200:
            d = response.json()
            TOKEN_MAP = pd.DataFrame.from_dict(d)
            TOKEN_MAP['expiry'] = pd.to_datetime(TOKEN_MAP['expiry'])
            TOKEN_MAP = TOKEN_MAP.astype({'strike': float})
            logger.info(f"✅ Successfully loaded token mapping with {len(TOKEN_MAP)} entries")
            return True
        else:
            logger.error(f"❌ Failed to fetch token mapping. Status code: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"❌ Error initializing token map: {str(e)}")
        return False

def getTokenInfo(symbol):
    """Get token for a symbol"""
    global TOKEN_MAP
    result = TOKEN_MAP[TOKEN_MAP['symbol'] == symbol]

    if result.empty:
        logger.warning(f"⚠️ Token not found for {symbol}")
        return None

    token = result.iloc[0]['token']
    logger.info(f"✅ Found token {token} for {symbol}")
    return token

def getExchangeSegment(symbol):
    """Get exchange segment for a symbol"""
    global TOKEN_MAP
    result = TOKEN_MAP[TOKEN_MAP['symbol'] == symbol]
    if result.empty:
        return "NSE"  # default fallback
    exchange = result.iloc[0]['exch_seg']
    logger.info(f"Exchange for {symbol}: {exchange}")
    return exchange

def getHistoricalAPI(symbol, token, interval='ONE_HOUR'):
    """Fetch historical data with improved error handling"""
    global SMART_API_OBJ
    
    # Ensure we have a valid API object
    if SMART_API_OBJ is None:
        logger.error("❌ API object is not initialized. Re-authenticating...")
        if not authenticate_api():
            logger.error("❌ Failed to re-authenticate")
            return None
    
    # ✅ Ensure correct market hours: 9:15 AM - 3:30 PM IST
    today_ist = datetime.now(IST_TZ)
    to_date = today_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    from_date = to_date - timedelta(days=15)

    from_date_format = from_date.strftime("%Y-%m-%d 09:15")
    to_date_format = to_date.strftime("%Y-%m-%d 15:30")

    logger.info(f"📅 Fetching data for {symbol} from {from_date_format} to {to_date_format}")

    if not token or pd.isna(token):
        logger.error(f"❌ Error: Invalid token ({token}) for {symbol}")
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

            logger.info(f"Attempt {attempt+1}/3: Fetching data for {symbol}")
            response = SMART_API_OBJ.getCandleData(historicParam)

            # Check if session expired (common cause of Invalid Token errors)
            if not response or ('message' in response and 'Invalid Token' in response['message']):
                logger.warning(f"⚠️ Invalid token for {symbol}. Re-authenticating...")
                # Re-authenticate before next attempt
                authenticate_api()
                time.sleep(2)
                continue
                
            if not response or 'data' not in response or not response['data']:
                logger.warning(f"⚠️ API returned empty data for {symbol}. Retrying... (Attempt {attempt + 1}/3)")
                time.sleep(2)
                continue

            logger.info(f"✅ Successfully fetched data for {symbol}")
            df = pd.DataFrame(response['data'], columns=['timestamp', 'O', 'H', 'L', 'C', 'V'])
            
            # Process the data
            df = process_data(df, symbol)
            
            return df

        except Exception as e:
            logger.error(f"❌ Error fetching data for {symbol} (Attempt {attempt+1}/3): {str(e)}")
            time.sleep(2 * (attempt + 1))  # Progressive backoff
            
            # Re-authenticate on certain errors
            if "Invalid Token" in str(e) or "Session Expired" in str(e):
                authenticate_api()

    logger.error(f"❌ API failed for {symbol} after 3 attempts.")
    return None

def process_data(df, symbol):
    """Process and calculate indicators for the data"""
    if df.empty:
        logger.warning(f"❌ No data available for {symbol}")
        return None

    try:
        # ✅ Convert timestamp to IST
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df['timestamp'] = df['timestamp'].dt.tz_convert(IST_TZ)

        df.sort_values('timestamp', inplace=True)
        df.fillna(method='ffill', inplace=True)

        if len(df) < 50:  # Ensure enough data for calculations
            logger.warning(f"⚠️ Not enough data for indicators for {symbol} - only {len(df)} rows")
            return None

        # Rename columns for uniform processing
        df = df.rename(columns={'O': 'open', 'H': 'high', 'L': 'low', 'C': 'close', 'V': 'volume'})

        # ✅ Calculate Technical Indicators
        logger.info(f"Calculating indicators for {symbol}")
        
        # Basic indicators
        df['RSI'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi()
        df['ATR'] = ta.volatility.AverageTrueRange(df['high'], df['low'], df['close'], window=20).average_true_range()
        df['ADX'] = ta.trend.ADXIndicator(df['high'], df['low'], df['close'], window=14).adx()
        
        df['EMA_20'] = ta.trend.EMAIndicator(df['close'], window=20).ema_indicator()
        df['EMA_50'] = ta.trend.EMAIndicator(df['close'], window=50).ema_indicator()
        df['EMA_200'] = ta.trend.EMAIndicator(df['close'], window=200).ema_indicator()
        
        df['VWAP'] = ta.volume.VolumeWeightedAveragePrice(df['high'], df['low'], df['close'], df['volume']).volume_weighted_average_price()

        # Advanced indicators
        df = calculate_supertrend(df)
        df = calculate_rvi(df)
        df = calculate_chaikin_volatility(df)
        
        # Pivot points
        df = calculate_camarilla_pivots(df)
        df = calculate_weekly_camarilla_pivots(df)
        df = calculate_weekly_demark_pivots(df)
        
        # Analysis
        df = apply_bull_bear_conditions(df)
        
        # Add symbol column
        df['symbol'] = symbol
        
        # Fill any remaining NaN values
        df.fillna(0, inplace=True)
        
        logger.info(f"✅ Successfully processed indicators for {symbol}")
        return df
        
    except Exception as e:
        logger.error(f"❌ Error processing data for {symbol}: {str(e)}")
        return None

def calculate_rvi(df, period=10, len_smooth=14):
    """Calculate Relative Vigor Index"""
    try:
        src = df['close']
        stddev = src.rolling(window=period).std()

        up_vol = np.where(src.diff() > 0, stddev, 0)
        down_vol = np.where(src.diff() <= 0, stddev, 0)

        up_ema = pd.Series(up_vol, index=df.index).ewm(span=len_smooth, adjust=False).mean()
        down_ema = pd.Series(down_vol, index=df.index).ewm(span=len_smooth, adjust=False).mean()

        df['RVI'] = 100 * (up_ema / (up_ema + down_ema))
        df['RVI'] = df['RVI'].ffill()
        return df
    except Exception as e:
        logger.error(f"❌ Error calculating RVI: {str(e)}")
        df['RVI'] = 50  # Default neutral value
        return df

def calculate_chaikin_volatility(df, ema_period=10, change_period=10):
    """Calculate Chaikin Volatility"""
    try:
        # Step 1: EMA of the high-low range
        hl_range = df['high'] - df['low']
        ema_hl = hl_range.ewm(span=ema_period, adjust=False).mean()

        # Step 2: % change over change_period days
        chaikin_volatility = ema_hl.pct_change(periods=change_period) * 100

        df['ChaikinVolatility'] = chaikin_volatility.fillna(0)
        return df
    except Exception as e:
        logger.error(f"❌ Error calculating Chaikin Volatility: {str(e)}")
        df['ChaikinVolatility'] = 0
        return df

def calculate_supertrend(df, period=10, multiplier=3):
    """Calculate Supertrend Indicator"""
    try:
        # Use pandas_ta for Supertrend calculation
        df.ta.supertrend(length=period, multiplier=multiplier, append=True)
        
        # pandas_ta creates 'SUPERT_10_3.0' and 'SUPERTd_10_3.0' columns
        supertrend_col = f"SUPERT_{period}_{multiplier}.0"
        
        if supertrend_col in df.columns:
            df.rename(columns={supertrend_col: 'Supertrend'}, inplace=True)
        else:
            logger.warning(f"⚠️ Supertrend column {supertrend_col} not found in DataFrame.")
            # Add a placeholder if calculation failed
            df['Supertrend'] = df['close']  # Neutral default

        return df
    except Exception as e:
        logger.error(f"❌ Error calculating Supertrend: {str(e)}")
        # Add placeholder if calculation failed
        df['Supertrend'] = df['close']  # Neutral default
        return df

def calculate_camarilla_pivots(df):
    """Calculate Camarilla Pivot Points"""
    try:
        # Convert timestamp to datetime and extract date
        if 'timestamp' in df.columns:
            df['date'] = pd.to_datetime(df['timestamp']).dt.date  # Extract date only
        else:
            logger.warning("⚠️ No timestamp column found for Camarilla calculation")
            return df

        # Aggregating to get previous day's high, low, and close
        daily_df = df.groupby('date').agg({'high': 'max', 'low': 'min', 'close': 'last'}).dropna()
        daily_df = daily_df.shift(1)  # Shift by 1 day to use yesterday's values

        if daily_df.empty:
            logger.warning("⚠️ No valid daily data after shifting, skipping Camarilla calculation.")
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

        # Merge with data on date to apply previous day's pivots
        df = df.merge(daily_df[['date', 'PP', 'R1', 'R2', 'R3', 'R4', 'S1', 'S2', 'S3', 'S4']], on="date", how="left")

        return df
    except Exception as e:
        logger.error(f"❌ Error calculating Camarilla pivots: {str(e)}")
        return df

def calculate_weekly_camarilla_pivots(df):
    """Calculate Weekly Camarilla Pivot Points"""
    try:
        # Convert timestamp to datetime
        if 'timestamp' not in df.columns:
            logger.warning("⚠️ No timestamp column found for weekly Camarilla calculation")
            return df

        # Extract week start date
        df['week'] = pd.to_datetime(df['timestamp']).dt.to_period('W').apply(lambda r: r.start_time)

        # Aggregating high, low, and close prices by week
        weekly_df = df.groupby('week').agg({'high': 'max', 'low': 'min', 'close': 'last'}).dropna()
        weekly_df = weekly_df.shift(1)  # Use previous week's data

        if weekly_df.empty:
            logger.warning("⚠️ No valid weekly data after shifting")
            return df

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
        df = df.merge(weekly_df[['week', 'W_PP', 'W_R1', 'W_R2', 'W_R3', 'W_R4', 'W_S1', 'W_S2', 'W_S3', 'W_S4']], 
                      on="week", how="left")
        
        return df
    except Exception as e:
        logger.error(f"❌ Error calculating weekly Camarilla pivots: {str(e)}")
        return df

def calculate_weekly_demark_pivots(df):
    """Calculate Weekly DeMark Pivot Points"""
    try:
        # Ensure timestamp is in datetime format and normalize to weekly buckets
        if 'timestamp' not in df.columns:
            logger.warning("⚠️ No timestamp column found for DeMark pivot calculation")
            return df
            
        df['week'] = pd.to_datetime(df['timestamp']).dt.to_period('W').apply(lambda r: r.start_time)

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

        # Convert week to string format for display if needed
        df['week_str'] = df['week'].astype(str)

        return df
    except Exception as e:
        logger.error(f"❌ Error calculating weekly DeMark pivots: {str(e)}")
        return df

def apply_bull_bear_conditions(df):
    """Apply bull/bear conditions for signal generation"""
    try:
        required_cols = ['close', 'Supertrend', 'R1_Demark', 'S1_Demark', 'ChaikinVolatility', 'RVI']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            logger.warning(f"❌ Missing columns for bull/bear condition check: {missing_cols}")
            # Add placeholder columns so we can continue
            for col in missing_cols:
                if col == 'Supertrend':
                    df[col] = df['close']
                elif col in ['R1_Demark', 'S1_Demark']:
                    df[col] = df['close']
                elif col == 'ChaikinVolatility':
                    df[col] = 0
                elif col == 'RVI':
                    df[col] = 50

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

        # Initialize signal column
        df['Signal'] = 'Neutral'

        # Apply conditions
        df.loc[bull_condition, 'Signal'] = 'Bull'
        df.loc[bear_condition, 'Signal'] = 'Bear'
        df.loc[(bull_continue) & (df['Signal'] == 'Neutral'), 'Signal'] = 'BullContinue'
        df.loc[(bear_continue) & (df['Signal'] == 'Neutral'), 'Signal'] = 'BearContinue'

        # Keep all rows for now, filtering can be done later if needed
        # df = df[df['Signal'] != 'Neutral']

        return df
    except Exception as e:
        logger.error(f"❌ Error applying bull/bear conditions: {str(e)}")
        df['Signal'] = 'Error'
        return df

def fetch_data(symbol):
    """Fetch historical data with retry handling and token validation."""
    token = getTokenInfo(symbol)

    if not token or pd.isna(token):
        logger.warning(f"⚠️ No token found for {symbol}, skipping.")
        return

    attempt = 0
    backoff = 1  # Initial backoff delay

    while attempt < 3:
        df = getHistoricalAPI(symbol, token)

        if df is not None and not df.empty:
            all_data.append(df)
            logger.info(f"✅ Successfully fetched and processed data for {symbol}")
            return

        attempt += 1
        logger.warning(f"🔄 Retrying {symbol} in {backoff}s (Attempt {attempt}/3)")
        time.sleep(backoff)
        backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF)

    logger.error(f"❌ Failed to fetch data for {symbol} after 3 attempts")

def worker():
    """Worker function to process symbols from the queue"""
    while not symbol_queue.empty():
        try:
            symbol = symbol_queue.get()
            logger.info(f"Processing symbol: {symbol}")
            fetch_data(symbol)
            symbol_queue.task_done()
            # Add delay to avoid rate limiting
            time.sleep(1/MAX_REQUESTS_PER_SEC)
        except Exception as e:
            logger.error(f"❌ Error in worker processing {symbol}: {str(e)}")
            symbol_queue.task_done()

def main():
    """Main function to orchestrate the data collection process"""
    logger.info("Starting data collection process")
    
    # Initialize token map
    if not initializeSymbolTokenMap():
        logger.error("❌ Failed to initialize symbol token map. Exiting.")
        return
    
    # Authenticate with API
    if not authenticate_api():
        logger.error("❌ Failed to authenticate with Angel API. Exiting.")
        return
    
    # Fetch symbol list
    SYMBOL_LIST = fetch_symbols_from_csv()
    if not SYMBOL_LIST:
        logger.error("❌ No symbols found. Exiting.")
        return
        
    logger.info(f"✅ Processing {len(SYMBOL_LIST)} symbols")
    
    # Add symbols to queue
    for symbol in SYMBOL_LIST:
        symbol_queue.put(symbol)
    
    # Process with thread pool
    with ThreadPoolExecutor(max_workers=MAX_REQUESTS_PER_SEC) as executor:
        for _ in range(min(MAX_REQUESTS_PER_SEC, len(SYMBOL_LIST))):
            executor.submit(worker)
    
    # Wait for queue to be processed
    symbol_queue.join()
    
    # Combine and save results
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df['timestamp'] = pd.to_datetime(final_df['timestamp'])
        final_df.to_csv(OUTPUT_FILE, index=False)
        logger.info(f"✅ Data saved to {OUTPUT_FILE} with {len(final_df)} rows")
    else:
        logger.warning("⚠️ No data collected. Check for errors.")
    
    logger.info("✅ Data collection completed.")

if __name__ == '__main__':
    main()
