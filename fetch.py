import math
import requests
import pandas as pd # type: ignore
import time
import urllib3
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
from config import Config
from format_gmt import format_to_gmt
from model import get_parts, create_envelope, checking_envelope_values


def fetch_single_value(params: Dict) -> Optional[Dict]:
    max_retries = 3
    retry_delay = 2  # seconds
    
    for attempt in range(max_retries):
        try:
            response = params['session'].get(
                f"{params['host']}/streams/{params['web_id']}/value?time={params['datetime']}", 
                auth=params['auth'], 
                verify=False
            )
            
            # Handle rate limiting
            if response.status_code == 429:
                if attempt < max_retries - 1:  # Don't sleep on last attempt
                    time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    print(f"Rate limit exceeded after {max_retries} attempts for {params['datetime']}")
                    return None
                    
            response.raise_for_status()
            data = response.json()
            
            # Ambil nilai dari Value
            signal_value = data.get('Value')
            
            # Jika Value adalah dictionary, kita perlu mengakses nilai sebenarnya
            if isinstance(signal_value, dict):
                signal_value = signal_value.get('Value', 0)
            
            # Konversi ke float dan handle NaN/Inf
            if isinstance(signal_value, (int, float)) and (math.isnan(signal_value) or math.isinf(signal_value)):
                signal_value = 0
                
            return {
                'datetime': pd.to_datetime(format_to_gmt(data['Timestamp'][:19])),
                'signal': float(signal_value) if signal_value is not None else 0
            }
            
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1} failed for {params['datetime']}: {str(e)}")
                time.sleep(retry_delay * (attempt + 1))
                continue
            print(f"Error fetching {params['datetime']} after {max_retries} attempts: {str(e)}")
            return None
            
        except Exception as e:
            print(f"Unexpected error for {params['datetime']}: {str(e)}")
            return None

def fetch(username: str, password: str, host: str, web_id: str) -> pd.DataFrame:
    """Fetch data from PI Web API between dates with rate limiting"""
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Setup session
    session = requests.Session()
    auth = HTTPBasicAuth(username, password)
    
    # Generate all timestamps first
    start_date = datetime(2024, 9, 1, 0, 0, 0, 0) 
    current_date = datetime.now()
    # end_date = current_date.replace(hour=10, minute=59, second=59, microsecond=999999)
    end_date = datetime(2025, 2, 4, 14, 0, 0, 0)
    
    dates = [
        (start_date + timedelta(days=d, hours=h)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        for d in range((end_date - start_date).days + 1)
        for h in range(24)
        if (start_date + timedelta(days=d, hours=h)) <= end_date
    ]
    
    # Prepare parameters for parallel execution
    params_list = [{
        'session': session,
        'auth': auth,
        'host': host,
        'web_id': web_id,
        'datetime': date
    } for date in dates]
    
    # Fetch data in parallel with rate limiting
    data = []
    max_workers = 5  # Reduced from 10 to help prevent rate limiting
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_params = {
            executor.submit(fetch_single_value, params): params
            for params in params_list
        }
        
        for future in as_completed(future_to_params):
            try:
                result = future.result()
                if result:
                    data.append(result)
            except Exception as e:
                print(f"Error processing result: {str(e)}")
    
    # Create and process DataFrame
    if not data:  # Check if data list is empty
        print("Warning: No data was successfully fetched")
        return pd.DataFrame(columns=['datetime', 'signal'])
    
    df = pd.DataFrame(data)
    if not df.empty and not pd.api.types.is_datetime64_any_dtype(df['datetime']):
        df['datetime'] = pd.to_datetime(df['datetime'])
    
    return df

def main():
    try:
        config = Config()
        parts = get_parts()
        print(f"start fetching for {len(parts)}")
        
        for part in parts:
            try:
                data = fetch(
                    config.PIWEB_API_USER,
                    config.PIWEB_API_PASS,
                    config.PIWEB_API_URL,
                    part[1]
                )
                if data.empty:
                    print(f"No data fetched for part {part[3]}")
                    continue
                    
                print(f"Fetched {len(data)} records for part {part[3]}")
                create_envelope(data, part[0])
                
            except Exception as e:
                print(f"Failed to process part {part[3]}: {str(e)}")
                continue
            
    except Exception as e:
        print(f"Failed to initialize: {str(e)}")
        raise

def run_selected_part():
    try:
        config = Config()
        parts = get_parts()
        counter = 0
        
        for part in parts:
            try:
                exists = checking_envelope_values(part[0])
                if exists is not None:
                    print(f"Data envelope sudah ada untuk part {part[3]}")
                    continue
                    
                print(f"Data envelope belum ada untuk part {part[3]}")
                counter += 1
                
                data = fetch(
                    config.PIWEB_API_USER,
                    config.PIWEB_API_PASS,
                    config.PIWEB_API_URL,
                    part[1]
                )
                
                if data.empty:
                    print(f"No data fetched for part {part[3]}")
                    continue

                print(f"Fetched {len(data)} records for part {part[3]}")
                create_envelope(data, part[0])
                
            except Exception as e:
                print(f"Gagal memproses part {part[3]}: {str(e)}")
                continue
            
        print(f"Total part yang belum memiliki data: {counter}")
        print(f"Total part yang dicek: {len(parts)}")
    except Exception as e:
        print(f"Gagal menginisialisasi: {str(e)}")
        raise
    
if __name__ == "__main__":
    run_selected_part()
    # main()
    # pass