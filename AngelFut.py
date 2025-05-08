import requests
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
import logging
import subprocess
from SmartApi import SmartConnect
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Run Angelmasterlist.py (optional step - can be commented out if not needed)
script_dir = os.path.dirname(os.path.abspath(__file__))
angel_script = os.path.join(script_dir, "Angelmasterlist.py")

try:
    subprocess.run(["python", angel_script], check=True)
    logger.info("✅ Successfully ran Angelmasterlist.py")
except subprocess.CalledProcessError as e:
    logger.error(f"❌ Failed to run Angelmasterlist.py: {e}")
    # Don't exit - continue with the existing master list if available

# Define constants
MAX_REQUESTS_PER_SEC = 3
BACKOFF_MULTIPLIER = 2
MAX_BACKOFF = 30
OUTPUT_FILE = "AngelFutCIA.csv"
IST_TZ = pytz.timezone("Asia/Kolkata")

# Global variables
TOKEN_MAP = None
symbol_queue = queue.Queue()
all_data = []
SMART_API_OBJ = None


def fetch_symbols_from_csv(file_path="Angel_MasterList.csv", column_name="symbol"):
    """Load symbols from CSV file."""
    try:
        if not os.path.exists(file_path):
            logger.error(f"❌ CSV file not found: {file_path}")
            return []
            
        df = pd.read_csv(file_path)
        if column_name not in df.columns:
            logger.error(f"❌ Column '{column_name}' not found in {file_path}")
            return []
            
        symbols = df[column_name].dropna().astype(str).str.strip().tolist()
        logger.info(f"✅ Fetched {len(symbols)} symbols from {file_path}")
        return symbols
    except Exception as e:
        logger.error(f"❌ Error reading symbols: {e}")
        return []


def initialize_symbol_token_map():
    """Initialize token mapping from Angel Broking API."""
    try:
        url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            logger.error(f"❌ Failed to fetch token map: Status code {response.status_code}")
            return False
            
        d = response.json()
        global TOKEN_MAP
        TOKEN_MAP = pd.DataFrame.from_dict(d)
        TOKEN_MAP['expiry'] = pd.to_datetime(TOKEN_MAP['expiry'], errors='coerce')
        TOKEN_MAP = TOKEN_MAP.astype({'strike': float}, errors='ignore')
        logger.info(f"✅ Token map initialized with {len(TOKEN_MAP)} symbols")
        return True
    except Exception as e:
        logger.error(f"❌ Error initializing token map: {e}")
        return False


def get_token_info(symbol):
    """Get token information for a symbol."""
    global TOKEN_MAP
    if TOKEN_MAP is None or TOKEN_MAP.empty:
        logger.error("❌ Token map not initialized")
        return None
        
    result = TOKEN_MAP[TOKEN_MAP['symbol'] == symbol]
    
    if result.empty:
        logger.warning(f"⚠️ Token not found for {symbol}")
        return None
        
    return result.iloc[0]['token']


def get_exchange_segment(symbol):
    """Get exchange segment for a symbol."""
    global TOKEN_MAP
    if TOKEN_MAP is None or TOKEN_MAP.empty:
        return "NSE"  # default fallback
        
    result = TOKEN_MAP[TOKEN_MAP['symbol'] == symbol]
    if result.empty:
        return "NSE"  # default fallback
        
    return result.iloc[0]['exch_seg']  # Should return 'NSE', 'NFO', etc.


def calculate_indicators(df, symbol):
    """Calculate technical indicators for DataFrame."""
    if df.empty:
        logger.error(f"❌ No data available for {symbol}")
        return None

    try:
        # Convert timestamp to IST
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df['timestamp'] = df['timestamp'].dt.tz_convert(IST_TZ)

        df.sort_values('timestamp', inplace=True)
        df.fillna(method='ffill', inplace=True)

        if len(df) < 50:  # Ensure enough data for calculations
            logger.warning(f"⚠️ Not enough data for indicators for {symbol}, only {len(df)} rows")
            return None

        # Calculate Technical Indicators
        df['RSI'] = ta.momentum.RSIIndicator(df['C'], window=14).rsi()
        df['ATR'] = ta.volatility.AverageTrueRange(df['H'], df['L'], df['C'], window=20).average_true_range()
        df['ADX'] = ta.trend.ADXIndicator(df['H'], df['L'], df['C'], window=14).adx()
        
        df['EMA_20'] = ta.trend.EMAIndicator(df['C'], window=20).ema_indicator()
        df['EMA_50'] = ta.trend.EMAIndicator(df['C'], window=50).ema_indicator()
        df['EMA_200'] = ta.trend.EMAIndicator(df['C'], window=200).ema_indicator()
        
        df['VWAP'] = ta.volume.VolumeWeightedAveragePrice(df['H'], df['L'], df['C'], df['V']).volume_weighted_average_price()

        df['symbol'] = symbol  # Add symbol column for merging
        df.fillna(0, inplace=True)  # Replace NaNs with 0
        
        logger.info(f"✅ Calculated indicators for {symbol}")
        return df
        
    except Exception as e:
        logger.error(f"❌ Error calculating indicators for {symbol}: {e}")
        return None


def calculate_rvi(df, period=10, len_smooth=14):
    """Calculate RVI (Relative Volatility Index)."""
    try:
        # Ensure column names are consistent
        df = df.rename(columns={'C': 'close'}) if 'C' in df.columns and 'close' not in df.columns else df
        
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
        logger.error(f"❌ Error calculating RVI: {e}")
        return df


def calculate_chaikin_volatility(df, ema_period=10, change_period=10):
    """Calculate Chaikin Volatility indicator."""
    try:
        # Ensure column names are consistent
        df = df.rename(columns={'H': 'high'}) if 'H' in df.columns and 'high' not in df.columns else df
        df = df.rename(columns={'L': 'low'}) if 'L' in df.columns and 'low' not in df.columns else df
        
        # Calculate high-low range
        hl_range = df['high'] - df['low']
        ema_hl = hl_range.ewm(span=ema_period, adjust=False).mean()

        # Calculate % change over change_period days
        chaikin_volatility = ema_hl.pct_change(periods=change_period) * 100

        df['ChaikinVolatility'] = chaikin_volatility.fillna(0)
        return df
    except Exception as e:
        logger.error(f"❌ Error calculating Chaikin Volatility: {e}")
        return df


def calculate_supertrend(df, period=10, multiplier=3):
    """Calculate Supertrend indicator."""
    try:
        # Rename columns to lowercase for pandas_ta compatibility
        df = df.rename(columns={'H': 'high', 'L': 'low', 'C': 'close'})

        # Calculate Supertrend
        df.ta.supertrend(length=period, multiplier=multiplier, append=True)

        # pandas_ta creates 'SUPERT_10_3.0' and 'SUPERTd_10_3.0' columns
        supertrend_col = f"SUPERT_{period}_{multiplier}.0"
        
        if supertrend_col in df.columns:
            df.rename(columns={supertrend_col: 'Supertrend'}, inplace=True)
        else:
            logger.warning(f"⚠️ Supertrend column {supertrend_col} not found in DataFrame.")

        return df
    except Exception as e:
        logger.error(f"❌ Error calculating Supertrend: {e}")
        return df


def calculate_camarilla_pivots(df):
    """Calculate Camarilla pivot points."""
    try:
        if df.empty:
            logger.warning("⚠️ DataFrame is empty, skipping Camarilla pivot calculation.")
            return df

        # Convert timestamp to datetime and extract date
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['date'] = df['timestamp'].dt.date  # Extract date only

        # Rename columns to match expected names
        rename_dict = {'H': 'high', 'L': 'low', 'C': 'close', 'O': 'open'}
        df.rename(columns=rename_dict, inplace=True)

        # Check if required columns are present after renaming
        if not all(col in df.columns for col in ['high', 'low', 'close']):
            logger.warning("⚠️ Missing required columns for Camarilla calculation")
            return df

        # Aggregating to get previous day's high, low, and close
        daily_df = df.groupby('date').agg({'high': 'max', 'low': 'min', 'close': 'last'}).dropna()
        daily_df = daily_df.shift(1)  # Shift by 1 day to use yesterday's values

        if daily_df.empty:
            logger.warning("⚠️ No valid daily data after shifting")
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
        logger.error(f"❌ Error calculating Camarilla pivots: {e}")
        return df


def calculate_weekly_camarilla_pivots(df):
    """Calculate weekly Camarilla pivot points."""
    try:
        if df.empty:
            logger.warning("⚠️ DataFrame is empty, skipping weekly Camarilla pivot calculation.")
            return df

        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Rename columns to match expected names
        rename_dict = {'H': 'high', 'L': 'low', 'C': 'close', 'O': 'open'}
        df.rename(columns=rename_dict, inplace=True)

        # Extract week start date
        df['week'] = df['timestamp'].dt.to_period('W').apply(lambda r: r.start_time)

        # Aggregating high, low, and close prices by week
        weekly_df = df.groupby('week').agg({'high': 'max', 'low': 'min', 'close': 'last'}).dropna()

        # Camarilla Pivot Calculations for weekly data
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
    except Exception as e:
        logger.error(f"❌ Error calculating weekly Camarilla pivots: {e}")
        return df


def calculate_weekly_demark_pivots(df):
    """Calculate weekly DeMark pivot points."""
    try:
        if df.empty:
            logger.warning("⚠️ DataFrame is empty, skipping DeMark pivot calculation.")
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

        return df
    except Exception as e:
        logger.error(f"❌ Error calculating weekly DeMark pivots: {e}")
        return df


def apply_bull_bear_conditions(df):
    """Apply bull and bear signal conditions to DataFrame."""
    try:
        required_cols = ['close', 'Supertrend', 'R1_Demark', 'S1_Demark', 'ChaikinVolatility', 'RVI']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            logger.warning(f"❌ Missing columns for bull/bear condition check: {missing_cols}")
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

        # Initialize signal column
        df['Signal'] = 'Neutral'

        df.loc[bull_condition, 'Signal'] = 'Bull'
        df.loc[bear_condition, 'Signal'] = 'Bear'
        df.loc[(bull_continue) & (df['Signal'] == 'Neutral'), 'Signal'] = 'BullContinue'
        df.loc[(bear_continue) & (df['Signal'] == 'Neutral'), 'Signal'] = 'BearContinue'

        # Remove Neutral rows
        df = df[df['Signal'] != 'Neutral']

        return df
    except Exception as e:
        logger.error(f"❌ Error applying bull/bear conditions: {e}")
        return df


def get_historical_data(symbol, token, interval='ONE_HOUR'):
    """Fetch historical data from Angel Broking API."""
    global SMART_API_OBJ
    
    if SMART_API_OBJ is None:
        logger.error("❌ SmartAPI object not initialized")
        return None
        
    if not token or pd.isna(token):
        logger.error(f"❌ Invalid token ({token}) for {symbol}")
        return None
        
    # Ensure correct market hours: 9:15 AM - 3:30 PM IST
    today_ist = datetime.now(IST_TZ)
    to_date = today_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    from_date = to_date - timedelta(days=15)

    from_date_format = from_date.strftime("%Y-%m-%d 09:15")
    to_date_format = to_date.strftime("%Y-%m-%d 15:30")

    logger.info(f"📅 Fetching data for {symbol} from {from_date_format} to {to_date_format}")

    exchange = get_exchange_segment(symbol)
    
    for attempt in range(3):
        try:
            historic_param = {
                "exchange": exchange,
                "symboltoken": str(token),
                "interval": interval,
                "fromdate": from_date_format,
                "todate": to_date_format
            }

            response = SMART_API_OBJ.getCandleData(historic_param)

            if not response or 'data' not in response or not response['data']:
                logger.warning(f"⚠️ API returned empty data for {symbol}. Retrying... (Attempt {attempt + 1}/3)")
                time.sleep(2)
                continue

            df = pd.DataFrame(response['data'], columns=['timestamp', 'O', 'H', 'L', 'C', 'V'])
            
            # Apply all technical indicators
            df = calculate_indicators(df, symbol)
            if df is None:
                logger.warning(f"⚠️ Failed to calculate indicators for {symbol}")
                return None
                
            df = calculate_supertrend(df)
            df = calculate_camarilla_pivots(df)
            df = calculate_weekly_camarilla_pivots(df)
            df = calculate_weekly_demark_pivots(df)
            df = calculate_chaikin_volatility(df)
            df = calculate_rvi(df)
            df = apply_bull_bear_conditions(df)
            
            logger.info(f"✅ Successfully processed data for {symbol}")
            return df

        except Exception as e:
            logger.error(f"❌ Error fetching data for {symbol} (Attempt {attempt+1}/3): {str(e)}")
            time.sleep(2 * (attempt + 1))  # Exponential backoff

    logger.error(f"❌ API failed for {symbol} after 3 attempts.")
    return None


def fetch_data_worker():
    """Worker function to process symbols from the queue."""
    while not symbol_queue.empty():
        try:
            symbol = symbol_queue.get()
            logger.info(f"🔄 Processing {symbol}")
            
            token = get_token_info(symbol)
            if not token or pd.isna(token):
                logger.warning(f"⚠️ No token found for {symbol}, skipping.")
                symbol_queue.task_done()
                continue

            df = get_historical_data(symbol, token)
            if df is not None and not df.empty:
                all_data.append(df)
                logger.info(f"✅ Successfully fetched data for {symbol}")
            else:
                logger.warning(f"⚠️ No valid data returned for {symbol}")
                
            symbol_queue.task_done()
            
            # Rate limiting
            time.sleep(1.0 / MAX_REQUESTS_PER_SEC)
            
        except Exception as e:
            logger.error(f"❌ Error in worker processing {symbol}: {e}")
            symbol_queue.task_done()


def authenticate_angel_api():
    """Authenticate with Angel Broking API."""
    # Fetch credentials from environment variables
    API_KEY = os.getenv('API_KEY')
    USER_NAME = os.getenv('USER_NAME')
    PWD = os.getenv('PWD')
    TOTP_SECRET = os.getenv('TOTP_SECRET')
    
    # Check if all required environment variables are set
    if not all([API_KEY, USER_NAME, PWD, TOTP_SECRET]):
        logger.error("❌ Missing required environment variables. Please check your .env file.")
        logger.error("Required variables: API_KEY, USER_NAME, PWD, TOTP_SECRET")
        return None
    
    try:
        # Generate TOTP
        totp = pyotp.TOTP(TOTP_SECRET).now()
        logger.info(f"✅ Generated TOTP")
        
        # Initialize SmartConnect
        obj = SmartConnect(API_KEY=API_KEY)
        
        # Generate session
        data = obj.generateSession(USER_NAME, PWD, totp)
        
        if data.get('status'):
            logger.info("✅ Successfully authenticated with Angel API")
            return obj
        else:
            logger.error(f"❌ Authentication failed: {data.get('message')}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Authentication error: {str(e)}")
        return None


def main():
    """Main function to execute the script."""
    global SMART_API_OBJ, all_data
    
    # Initialize token map
    if not initialize_symbol_token_map():
        logger.error("❌ Failed to initialize token map. Exiting.")
        return
    
    # Authenticate with Angel API
    SMART_API_OBJ = authenticate_angel_api()
    if SMART_API_OBJ is None:
        logger.error("❌ Failed to authenticate with Angel API. Exiting.")
        return
    
    # Fetch symbols
    symbols = fetch_symbols_from_csv()
    if not symbols:
        logger.error("❌ No symbols found. Exiting.")
        return
    
    logger.info(f"✅ Processing {len(symbols)} symbols")
    
    # Add symbols to queue
    for symbol in symbols:
        symbol_queue.put(symbol)
    
    # Start worker threads
    threads = []
    for _ in range(min(MAX_REQUESTS_PER_SEC, len(symbols))):
        t = threading.Thread(target=fetch_data_worker)
        t.daemon = True
        t.start()
        threads.append(t)
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    # Process collected data
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df['timestamp'] = pd.to_datetime(final_df['timestamp'], utc=True).dt.tz_convert(IST_TZ)
        final_df.to_csv(OUTPUT_FILE, index=False)
        logger.info(f"✅ Data saved to {OUTPUT_FILE}")
        logger.info(f"✅ Processed {len(final_df)} data points across {len(symbols)} symbols")
    else:
        logger.warning("⚠️ No data collected. Check for errors above.")
    
    logger.info("✅ Data collection completed.")


if __name__ == '__main__':
    main()
