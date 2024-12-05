import math
import requests
import pandas as pd # type: ignore
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict
import urllib3
from config import Config
from format_gmt import format_to_gmt
from model import get_parts, create_envelope

def fetch_single_value(params: Dict) -> Dict:
    try:
        response = params['session'].get(
            f"{params['host']}/streams/{params['web_id']}/value?time={params['datetime']}", 
            auth=params['auth'], 
            verify=False
        )
        response.raise_for_status()
        data = response.json()
        
        # Ambil nilai dari Value
        signal_value = data['Value']
        
        # Jika Value adalah dictionary, kita perlu mengakses nilai sebenarnya
        if isinstance(signal_value, dict):
            # Sesuaikan key ini dengan struktur response yang sebenarnya
            signal_value = signal_value.get('Value', 0)  # atau key lain yang sesuai
        
        # Konversi ke float dan handle NaN/Inf
        if isinstance(signal_value, (int, float)) and (math.isnan(signal_value) or math.isinf(signal_value)):
            signal_value = 0
            
        return {
            'datetime': pd.to_datetime(format_to_gmt(data['Timestamp'][:19])),
            'signal': float(signal_value) if signal_value is not None else 0
        }
    except Exception as e:
        print(f"Error fetching {params['datetime']}: {str(e)}")
        print(f"Full response data: {data}")  # Tambahan debug info
        return None

def fetch(username: str, password: str, host: str, web_id: str) -> pd.DataFrame:
    """Fetch data from PI Web API between dates"""
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Setup session
    session = requests.Session()
    auth = HTTPBasicAuth(username, password)
    
    # Generate all timestamps first
    start_date = datetime(2024, 10, 21)
    end_date = datetime(2024, 12, 3)
    dates = [
        (start_date + timedelta(days=d, hours=h)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        for d in range((end_date - start_date).days + 1)
        for h in range(24)
    ]
    
    # Prepare parameters for parallel execution
    params_list = [{
        'session': session,
        'auth': auth,
        'host': host,
        'web_id': web_id,
        'datetime': date
    } for date in dates]
    
    # Fetch data in parallel
    data = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_params = {
            executor.submit(fetch_single_value, params): params
            for params in params_list
        }
        
        for future in as_completed(future_to_params):
            result = future.result()
            if result:
                data.append(result)
    
    # Create and process DataFrame
    df = pd.DataFrame(data)
    if not df.empty and not pd.api.types.is_datetime64_any_dtype(df['datetime']):
        df['datetime'] = pd.to_datetime(df['datetime'])
    
    return df

def main():
    try:
        config = Config()
        parts = get_parts()
        
        for part in parts:
            data = fetch(
                config.PIWEB_API_USER,
                config.PIWEB_API_PASS,
                config.PIWEB_API_URL,
                part[1]
            )
            print(f"Fetched {len(data)} records for part {part[3]}")
            create_envelope(data, part[0])
            
    except Exception as e:
        print(f"Failed to fetch data: {str(e)}")

if __name__ == "__main__":
    # main()
    pass