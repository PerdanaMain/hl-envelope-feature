from datetime import datetime, timedelta
from plot import find_signal_envelopes
from format_gmt import format_to_gmt
from requests.auth import HTTPBasicAuth
from config import Config
from model import *
from log import print_log
from arima import execute_arima
import pandas as pd  # type: ignore
import math
import requests
import urllib3
import time
import pytz
import schedule # type: ignore

def fetch(username: str, password: str, host: str, web_id: str) -> pd.DataFrame:
    """
    Fetch data from PI Web API for the current hour
    
    Args:
        username (str): API username
        password (str): API password
        host (str): Base API URL
        web_id (str): Web ID for the data stream
    
    Returns:
        pd.DataFrame: DataFrame containing fetched data
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Setup session
    session = requests.Session()
    auth = HTTPBasicAuth(username, password)
    
    # Get current time and calculate time range for this hour
    current_time = datetime.now(pytz.timezone('Asia/Jakarta'))
    start_time = current_time.replace(minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(hours=1)
    
    # Format the timestamp
    timestamp = start_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    try:
        # Make single API call for the hour
        url = f"{host}/streams/{web_id}/value?time={timestamp}"
        response = session.get(url, auth=auth, verify=False)
        response.raise_for_status()
        
        data = response.json()
        
        # Process the response
        signal_value = data['Value']
        if isinstance(signal_value, dict):
            signal_value = signal_value.get('Value', 0)
        
        if isinstance(signal_value, (int, float)) and (math.isnan(signal_value) or math.isinf(signal_value)):
            signal_value = 0
        
        # Create DataFrame with single record
        df = pd.DataFrame([{
            'datetime': pd.to_datetime(format_to_gmt(data['Timestamp'][:19])),
            'signal': float(signal_value) if signal_value is not None else 0
        }])
        
        print(f"Fetched data for {timestamp} - Web ID: {web_id}")
        print_log(f"Fetched data for {timestamp} - Web ID: {web_id}")
        return df
        
    except Exception as e:
        print(f"Error fetching data for {web_id} at {timestamp}: {e}")
        print_log(f"Error fetching data for {web_id} at {timestamp}: {e}")
        return pd.DataFrame()

def task():
    try:
        current_time = datetime.now(pytz.timezone('Asia/Jakarta'))
        print(f"Task running at: {current_time}")
        print_log(f"Task running at: {current_time}")
        
        config = Config()
        parts = get_parts()
        
        for part in parts:
            try:
                print(f"Processing part: {part[3]}")  # Assuming part[3] contains part name
                print_log(f"Processing part: {part[3]}")  # Assuming part[3] contains part name
                data = fetch(
                    config.PIWEB_API_USER,
                    config.PIWEB_API_PASS,
                    config.PIWEB_API_URL,
                    part[1]
                )
                
                if not data.empty:
                    create_envelope(data, part[0])
                    print(f"Successfully processed part {part[3]}")
                    print_log(f"Successfully processed part {part[3]}")
                else:
                    print(f"No data retrieved for part {part[3]}")
                    print_log(f"No data retrieved for part {part[3]}")
                    
            except Exception as e:
                print(f"Error processing part {part[3]}: {e}")
                continue
        
        print(f"Task completed at: {datetime.now(pytz.timezone('Asia/Jakarta'))}")
        print_log(f"Task completed at: {datetime.now(pytz.timezone('Asia/Jakarta'))}")
        
    except Exception as e:
        print(f"Error executing task: {e}")
        print_log(f"Error executing task: {e}")

def feature():
    current_timestamp = datetime.now(pytz.timezone('Asia/Jakarta'))
    past_timestamp = current_timestamp - timedelta(hours=6)

    parts = get_parts()

    for part in parts:
        data = get_envelope_values_by_date(part[0], start_date=past_timestamp, end_date=current_timestamp)
        df = pd.DataFrame(data, columns=["value", "datetime"])
        signal_values = df["value"].values
        min_indices, max_indices = find_signal_envelopes(signal_values)

        print(f"Found {len(max_indices)} maxima")
        save_envelopes_to_db(part[0], df, max_indices, features_id='9dcb7e40-ada7-43eb-baf4-2ed584233de7')


def index():
    print(f"Starting scheduler at: {datetime.now(pytz.timezone('Asia/Jakarta'))}")
    print_log(f"Starting scheduler at: {datetime.now(pytz.timezone('Asia/Jakarta'))}")

    # Schedule task setiap 1 jam
    schedule.every().hour.at(":00").do(task)
    schedule.every(6).hour.at(":00").do(feature)
    
    next_run = schedule.next_run()
    print(f"Next scheduled run at: {next_run}")
    print_log(f"Next scheduled run at: {next_run}")
    
    # Run task immediately for current hour
    # task()
    # feature()

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            print("Scheduler stopped by user")
            print_log("Scheduler stopped by user")
            break
        except Exception as e:
            print(f"Scheduler error: {e}")
            print_log(f"Scheduler error: {e}")
            time.sleep(60)

if __name__ == '__main__':
    index()
    # feature()
    # task()