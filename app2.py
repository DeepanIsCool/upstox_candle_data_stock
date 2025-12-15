import os
import time
import pandas as pd
import upstox_client
from upstox_client.rest import ApiException
from datetime import datetime, timedelta
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# =================CONFIGURATION=================
INPUT_FILE = "Stock.csv"
OUTPUT_FILENAME = "Stock_Candle_Data_Final.csv"  # Changed to CSV
EXCHANGE_PREFIX = "NSE_EQ|"

INTERVAL_UNIT = "days"
INTERVAL_VALUE = "1"

# THREADING SETTINGS
MAX_WORKERS = 4  # Keep this low to avoid Rate Limits (429)

# TIME SETTINGS - Start from Jan 1, 2010
END_DATE = datetime.now()
START_DATE = datetime(2010, 1, 1)  # Changed to Jan 1, 2010
CHUNK_SIZE_DAYS = 365 * 3  # Fetch 3 years per chunk

# =================HELPER FUNCTIONS=================

def get_instrument_key(isin):
    return f"{EXCHANGE_PREFIX}{str(isin).strip()}"

def fetch_chunk(api, instrument_key, start_date, end_date):
    """
    Fetches a single chunk of data.
    """
    try:
        response = api.get_historical_candle_data1(
            instrument_key, 
            INTERVAL_UNIT, 
            INTERVAL_VALUE, 
            end_date.strftime("%Y-%m-%d"), 
            start_date.strftime("%Y-%m-%d")
        )
        
        candles = getattr(response.data, "candles", [])
        return candles if candles else []

    except Exception as e:
        return []

def process_single_stock(symbol, isin):
    """
    Fetches all history for a single stock from Jan 1, 2010 to now.
    """
    instrument_key = get_instrument_key(isin)
    
    # Create a fresh API instance for this thread
    configuration = upstox_client.Configuration()
    api_instance = upstox_client.HistoryV3Api(upstox_client.ApiClient(configuration))
    
    all_candles = []
    current_end = END_DATE
    
    # Loop backwards from Now until Jan 1, 2010
    while current_end >= START_DATE:
        current_start = max(current_end - timedelta(days=CHUNK_SIZE_DAYS), START_DATE)
        
        # Fetch Data
        chunk_candles = fetch_chunk(api_instance, instrument_key, current_start, current_end)
        
        if chunk_candles:
            all_candles.extend(chunk_candles)
        
        # Move backwards
        current_end = current_start - timedelta(days=1)
        
        # Stop if we've reached the start date
        if current_start <= START_DATE:
            break
            
        # Sleep to avoid Rate Limits
        time.sleep(0.5)

    # Process Data
    if all_candles:
        df = pd.DataFrame(
            all_candles, 
            columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open_Interest']
        )
        
        # Add Symbol column
        df['Symbol'] = symbol
        
        # Formatting
        df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
        df[['Open', 'High', 'Low', 'Close']] = df[['Open', 'High', 'Low', 'Close']].astype(float)
        df[['Volume', 'Open_Interest']] = df[['Volume', 'Open_Interest']].astype(int)
        
        # Sort by Date (Ascending) and remove duplicates
        df = df.sort_values('Date').drop_duplicates(subset=['Date'])
        
        # Reorder columns: Symbol, Date, OHLCV
        df = df[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open_Interest']]
        
        return symbol, df
    else:
        return symbol, pd.DataFrame()

# =================MAIN EXECUTION=================
if __name__ == "__main__":
    print(f"🚀 Starting CSV Downloader (Data from {START_DATE.date()} onwards)...")
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ '{INPUT_FILE}' not found.")
        exit()

    # Load Data
    stock_df = pd.read_csv(INPUT_FILE)
    stock_df.columns = [c.lower() for c in stock_df.columns]
    
    # Map Symbol -> ISIN
    stock_map = pd.Series(stock_df.isin_number.values, index=stock_df.symbol).to_dict()
    
    all_data = []  # List to collect all dataframes
    
    print(f"⚡ Fetching data for {len(stock_map)} stocks using {MAX_WORKERS} threads...")
    print(f"   (Interval: {INTERVAL_UNIT}, From: {START_DATE.date()} to {END_DATE.date()})")

    # Threaded Execution
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_stock, sym, isin): sym for sym, isin in stock_map.items()}
        
        with tqdm(total=len(stock_map), unit="stock") as pbar:
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    sym, df = future.result()
                    if not df.empty:
                        all_data.append(df)
                        # Detailed success message
                        pbar.write(f"  ✅ {sym}: {len(df)} candles ({df['Date'].min().date()} to {df['Date'].max().date()})")
                    else:
                        pbar.write(f"  ⚠️ {sym}: No data found")
                except Exception as e:
                    pbar.write(f"  ❌ {symbol}: Error - {e}")
                
                pbar.update(1)

    # Merge all data and save to CSV
    if all_data:
        print(f"\n💾 Merging data from {len(all_data)} stocks...")
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Sort by Symbol and Date
        final_df = final_df.sort_values(['Symbol', 'Date']).reset_index(drop=True)
        
        # Save to CSV
        final_df.to_csv(OUTPUT_FILENAME, index=False)
        
        print(f"🎉 Done! File saved: {os.path.abspath(OUTPUT_FILENAME)}")
        print(f"   Total rows: {len(final_df):,}")
        print(f"   Date range: {final_df['Date'].min().date()} to {final_df['Date'].max().date()}")
        print(f"   Stocks: {final_df['Symbol'].nunique()}")
    else:
        print("❌ No data fetched. Check your Stock.csv ISINs.")