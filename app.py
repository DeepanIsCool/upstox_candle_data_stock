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
OUTPUT_FILENAME = "Stock_Candle_Data_Final.xlsx"
EXCHANGE_PREFIX = "NSE_EQ|"

# --- THE FIX IS HERE ---
INTERVAL_UNIT = "days"  # Changed from 'day' to 'days'
INTERVAL_VALUE = "1"
# -----------------------

# THREADING SETTINGS
MAX_WORKERS = 4  # Keep this low to avoid Rate Limits (429)

# TIME SETTINGS
# We fetch from Today back to year 2000
END_DATE = datetime.now()
STOP_YEAR = 2000
CHUNK_SIZE_DAYS = 365 * 3  # Fetch 3 years per chunk to keep response size manageable

# =================HELPER FUNCTIONS=================

def get_instrument_key(isin):
    # Ensure no spaces and correct format
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
        
        # Safe access to candles
        candles = getattr(response.data, "candles", [])
        return candles if candles else []

    except Exception as e:
        return []

def process_single_stock(symbol, isin):
    """
    Fetches all history for a single stock by looping backwards in time.
    """
    instrument_key = get_instrument_key(isin)
    
    # Create a fresh API instance for this thread
    configuration = upstox_client.Configuration()
    api_instance = upstox_client.HistoryV3Api(upstox_client.ApiClient(configuration))
    
    all_candles = []
    current_end = END_DATE
    
    # Loop backwards from Now until 2000
    while current_end.year >= STOP_YEAR:
        current_start = current_end - timedelta(days=CHUNK_SIZE_DAYS)
        
        # Fetch Data
        chunk_candles = fetch_chunk(api_instance, instrument_key, current_start, current_end)
        
        if chunk_candles:
            all_candles.extend(chunk_candles)
            # If we found data, move the end pointer back to the start of this chunk
            current_end = current_start
        else:
            # If no data found in this period
            # Check if we are already very far back (e.g. before 2005)
            # If so, the stock probably didn't exist then, so we stop.
            if current_end.year < 2010:
                break
            
            # Otherwise, keep searching backwards
            current_end = current_start
            
        # Sleep to avoid Rate Limits
        time.sleep(0.5)

    # Process Data
    if all_candles:
        df = pd.DataFrame(
            all_candles, 
            columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Open_Interest']
        )
        
        # Formatting
        df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
        df[['Open', 'High', 'Low', 'Close']] = df[['Open', 'High', 'Low', 'Close']].astype(float)
        df[['Volume', 'Open_Interest']] = df[['Volume', 'Open_Interest']].astype(int)
        
        # Sort by Date (Ascending) and remove duplicates
        df = df.sort_values('Date').drop_duplicates(subset=['Date']).set_index('Date')
        return symbol, df
    else:
        return symbol, pd.DataFrame()

# =================MAIN EXECUTION=================
if __name__ == "__main__":
    print(f"🚀 Starting Fixed Downloader on {os.uname().machine}...")
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ '{INPUT_FILE}' not found.")
        exit()

    # Load Data
    stock_df = pd.read_csv(INPUT_FILE)
    stock_df.columns = [c.lower() for c in stock_df.columns]
    
    # Map Symbol -> ISIN
    stock_map = pd.Series(stock_df.isin_number.values, index=stock_df.symbol).to_dict()
    
    final_results = {}
    
    print(f"⚡ Fetching data for {len(stock_map)} stocks using {MAX_WORKERS} threads...")
    print(f"   (Interval: {INTERVAL_UNIT}, Backfill until: {STOP_YEAR})")

    # Threaded Execution
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_stock, sym, isin): sym for sym, isin in stock_map.items()}
        
        with tqdm(total=len(stock_map), unit="stock") as pbar:
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    sym, df = future.result()
                    if not df.empty:
                        final_results[sym] = df
                        # Detailed success message
                        pbar.write(f"  ✅ {sym}: {len(df)} candles ({df.index.min().date()} to {df.index.max().date()})")
                    else:
                        pbar.write(f"  ⚠️ {sym}: No data found")
                except Exception as e:
                    pbar.write(f"  ❌ {symbol}: Error - {e}")
                
                pbar.update(1)

    # Save to Excel
    if final_results:
        print(f"\n💾 Saving {len(final_results)} stocks to Excel...")
        with pd.ExcelWriter(OUTPUT_FILENAME, engine='openpyxl') as writer:
            for symbol, df in final_results.items():
                safe_name = symbol.replace('|', '').replace(':', '').replace('/', '')[:30]
                df.to_excel(writer, sheet_name=safe_name)
        print(f"🎉 Done! File saved: {os.path.abspath(OUTPUT_FILENAME)}")
    else:
        print("❌ No data fetched. Check your Stock.csv ISINs.")