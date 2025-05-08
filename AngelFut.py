import requests
import pandas as pd
import pandas_ta as ta
import numpy as np
from datetime import datetime, timedelta
import pyotp
import warnings
import os
import pytz
import time
import logging
import subprocess
from dotenv import load_dotenv
from SmartApi import SmartConnect
from concurrent.futures import ThreadPoolExecutor
import queue

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Constants
MAX_RETRIES = 3
RETRY_DELAY = 2
MAX_WORKERS = 3
IST_TZ = pytz.timezone("Asia/Kolkata")
OUTPUT_FILE = "AngelHistoricalData.csv"

# Suppress warnings
warnings.filterwarnings('ignore')

# Global variables
TOKEN_MAP = None
SMART_API_OBJ = None
symbol_queue = queue.Queue()
all_data = []

def run_pre_requisites():
    """Run any prerequisite scripts"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        angel_script = os.path.join(script_dir, "Angelmasterlist.py")
        
        if os.path.exists(angel_script):
            logger.info(f"Running prerequisite script: {angel_script}")
            subprocess.run(["python", angel_script], check=True)
        else:
            logger.warning(f"Prerequisite script not found: {angel_script}")
            
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to run prerequisite script: {e}")

def fetch_symbols_from_csv(file_path="Angel_MasterList.csv", column_name="symbol"):
    """Fetch symbols from CSV file"""
    try:
        df = pd.read_csv(file_path)
        symbols = df[column_name].dropna().astype(str).str.strip().tolist()
        logger.info(f"Fetched {len(symbols)} symbols from {file_path}")
        return symbols
    except Exception as e:
        logger.error(f"Error reading symbols from {file_path}: {e}")
        return []

def initialize_token_map():
    """Initialize the token map from Angel Broking API"""
    global TOKEN_MAP
    try:
        url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
        response = requests.get(url)
        if response.status_code != 200:
            logger.error(f"Failed to fetch token map: Status code {response.status_code}")
            return False
            
        d = response.json()
        TOKEN_MAP = pd.DataFrame.from_dict(d)
        TOKEN_MAP['expiry'] = pd.to_datetime(TOKEN_MAP['expiry'], errors='coerce')
        TOKEN_MAP = TOKEN_MAP.astype({'strike': float})
        logger.info("Token map initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Error initializing token map: {e}")
        return False

def get_token_info(symbol):
    """Get token info for a symbol"""
    global TOKEN_MAP
    if TOKEN_MAP is None:
        logger.error("Token map not initialized")
        return None
        
    result = TOKEN_MAP[TOKEN_MAP['symbol'] == symbol]
    if result.empty:
        logger.warning(f"Token not found for {symbol}")
        return None
    
    return result.iloc[0]['token']

def get_exchange_segment(symbol):
    """Get exchange segment for a symbol"""
    global TOKEN_MAP
    if TOKEN_MAP is None:
        return "NSE"  # Default fallback
        
    result = TOKEN_MAP[TOKEN_MAP['symbol'] == symbol]
    if result.empty:
        return "NSE"  # Default fallback
    
    return result.iloc[0]['exch_seg']

def authenticate_api():
    """Authenticate with Angel Broking API"""
    global SMART_API_OBJ
    
    # Fetch credentials
    api_key = os.getenv('API_KEY')
    user_name = os.getenv('USER_NAME')
    password = os.getenv('PWD')
    totp_secret = os.getenv('TOTP_SECRET')
    
    # Validate credentials
    if not all([api_key, user_name, password, totp_secret]):
        logger.error("Missing required environment variables. Check your .env file.")
        return False
    
    try:
        # Generate TOTP
        totp = pyotp.TOTP(totp_secret).now()
        logger.info(f"Generated TOTP for authentication")
        
        # Create SmartConnect object
        obj = SmartConnect(api_key=api_key)
        
        # Generate session
        data = obj.generateSession(user_name, password, totp)
        
        if data.get('status'):
            logger.info("Authentication successful")
            SMART_API_OBJ = obj
            return True
        else:
            logger.error(f"Authentication failed: {data.get('message')}")
            return False
            
    except Exception as e:
        logger.error(f"Authentication error: {str(e)}")
        return False

def get_historical_data(symbol, token, interval='ONE_HOUR'):
    """Get historical data for a symbol"""
    if SMART_API_OBJ is None:
        logger.error("API not authenticated")
        return None
        
    if not token or pd.isna(token):
        logger.error(f"Invalid token for {symbol}")
        return None
    
    # Set date range
    today_ist = datetime.now(IST_TZ)
    to_date = today_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    from_date = to_date - timedelta(days=15)
    
    from_date_format = from_date.strftime("%Y-%m-%d 09:15")
    to_date_format = to_date.strftime("%Y-%m-%d 15:30")
    
    logger.info(f"Fetching data for {symbol} from {from_date_format} to {to_date_format}")
    
    exchange = get_exchange_segment(symbol)
    
    # Try to get data with retries
    for attempt in range(MAX_RETRIES):
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
                logger.warning(f"Empty response for {symbol}. Attempt {attempt + 1}/{MAX_RETRIES}")
                time.sleep(RETRY_DELAY)
                continue
                
            # Process data
            df = pd.DataFrame(response['data'], columns=['timestamp', 'O', 'H', 'L', 'C', 'V'])
            df = calculate_indicators(df, symbol)
            
            logger.info(f"Successfully retrieved data for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            time.sleep(RETRY_DELAY)
    
    logger.error(f"Failed to fetch data for {symbol} after {MAX_RETRIES} attempts")
    return None

def calculate_indicators(df, symbol):
    """Calculate technical indicators"""
    if df.empty:
        logger.warning(f"No data available for {symbol}")
        return None
    
    # Convert timestamp to IST
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df['timestamp'] = df['timestamp'].dt.tz_convert(IST_TZ)
    
    df.sort_values('timestamp', inplace=True)
    df.fillna(method='ffill', inplace=True)
    
    if len(df) < 50:
        logger.warning(f"Not enough data for indicators for {symbol}")
        return None
    
    # Rename columns for consistency
    df = df.rename(columns={'O': 'open', 'H': 'high', 'L': 'low', 'C': 'close', 'V': 'volume'})
    
    # Calculate indicators
    try:
        # RSI
        df['RSI'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi()
        
        # ATR
        df['ATR'] = ta.volatility.AverageTrueRange(df['high'], df['low'], df['close'], window=20).average_true_range()
        
        # ADX
        df['ADX'] = ta.trend.ADXIndicator(df['high'], df['low'], df['close'], window=14).adx()
        
        # EMAs
        df['EMA_20'] = ta.trend.EMAIndicator(df['close'], window=20).ema_indicator()
        df['EMA_50'] = ta.trend.EMAIndicator(df['close'], window=50).ema_indicator()
        df['EMA_200'] = ta.trend.EMAIndicator(df['close'], window=200).ema_indicator()
        
        # VWAP
        df['VWAP'] = ta.volume.VolumeWeightedAveragePrice(df['high'], df['low'], df['close'], df['volume']).volume_weighted_average_price()
        
        # SuperTrend
        df.ta.supertrend(length=10, multiplier=3, append=True)
        supertrend_col = "SUPERT_10_3.0"
        if supertrend_col in df.columns:
            df.rename(columns={supertrend_col: 'Supertrend'}, inplace=True)
        
        # RVI
        calculate_rvi(df)
        
        # Chaikin Volatility
        calculate_chaikin_volatility(df)
        
        # Add symbol column
        df['symbol'] = symbol
        
        # Fill any NaN values
        df.fillna(0, inplace=True)
        
        logger.info(f"Indicators calculated for {symbol}")
        return df
        
    except Exception as e:
        logger.error(f"Error calculating indicators for {symbol}: {str(e)}")
        return None

def calculate_rvi(df, period=10, len_smooth=14):
    """Calculate Relative Volatility Index"""
    try:
        src = df['close']
        stddev = src.rolling(window=period).std()
        
        up_vol = np.where(src.diff() > 0, stddev, 0)
        down_vol = np.where(src.diff() <= 0, stddev, 0)
        
        up_ema = pd.Series(up_vol, index=df.index).ewm(span=len_smooth, adjust=False).mean()
        down_ema = pd.Series(down_vol, index=df.index).ewm(span=len_smooth, adjust=False).mean()
        
        df['RVI'] = 100 * (up_ema / (up_ema + down_ema))
        df['RVI'] = df['RVI'].fillna(50)  # Default to neutral
        
        return df
    except Exception as e:
        logger.error(f"Error calculating RVI: {str(e)}")
        df['RVI'] = 50  # Default to neutral
        return df

def calculate_chaikin_volatility(df, ema_period=10, change_period=10):
    """Calculate Chaikin Volatility"""
    try:
        hl_range = df['high'] - df['low']
        ema_hl = hl_range.ewm(span=ema_period, adjust=False).mean()
        
        chaikin_volatility = ema_hl.pct_change(periods=change_period) * 100
        
        df['ChaikinVolatility'] = chaikin_volatility.fillna(0)
        return df
    except Exception as e:
        logger.error(f"Error calculating Chaikin Volatility: {str(e)}")
        df['ChaikinVolatility'] = 0
        return df

def worker():
    """Worker function for processing symbols"""
    while not symbol_queue.empty():
        try:
            symbol = symbol_queue.get()
            
            # Get token for symbol
            token = get_token_info(symbol)
            if not token:
                logger.warning(f"Skipping {symbol} - no token found")
                symbol_queue.task_done()
                continue
                
            # Get historical data
            df = get_historical_data(symbol, token)
            if df is not None:
                all_data.append(df)
                logger.info(f"Data for {symbol} processed successfully")
            
            symbol_queue.task_done()
            
        except Exception as e:
            logger.error(f"Error processing {symbol}: {str(e)}")
            symbol_queue.task_done()

def main():
    """Main function"""
    logger.info("Starting Angel Historical Data Retrieval")
    
    # Run pre-requisites
    run_pre_requisites()
    
    # Initialize token map
    if not initialize_token_map():
        logger.error("Failed to initialize token map, exiting")
        return
    
    # Authenticate API
    if not authenticate_api():
        logger.error("Failed to authenticate API, exiting")
        return
    
    # Get symbol list
    symbols = fetch_symbols_from_csv()
    if not symbols:
        logger.error("No symbols found, exiting")
        return
    
    # Add symbols to queue
    for symbol in symbols:
        symbol_queue.put(symbol)
    
    # Process symbols
    logger.info(f"Processing {symbol_queue.qsize()} symbols")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for _ in range(MAX_WORKERS):
            executor.submit(worker)
    
    # Wait for queue to be empty
    symbol_queue.join()
    
    # Save results
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(OUTPUT_FILE, index=False)
        logger.info(f"Data saved to {OUTPUT_FILE}")
    else:
        logger.warning("No data collected")
    
    logger.info("Data collection completed")

if __name__ == "__main__":
    main()
