import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from config import Config
from format_gmt import format_to_gmt
from model import *
import pandas as pd # type: ignore
import warnings
import urllib3

def fetch(username, password, host, web_id):
    """
    Fetch data from PI Web API between 21 October 2024 and current date
    
    Args:
        username (str): API username
        password (str): API password
        host (str): Base API URL
    
    Returns:
        pandas.DataFrame: Data with datetime and signal columns
    """
    # Disable SSL warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Define date range
    start_date = datetime(2024, 10, 21)
    end_date = datetime(2024, 12, 3)
    
    data = []
    current_date = start_date
    
    # Prepare API URL
    
    # Configure auth and session
    auth = HTTPBasicAuth(username, password)
    session = requests.Session()
    
    while current_date <= end_date:
        for hour in range(24):
            current_datetime = current_date + timedelta(hours=hour)
            formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            
            try:
                url = f"{host}/streams/{web_id}/value?time={formatted_datetime}"
                

                response = session.get(url, auth=auth, verify=False)
                response.raise_for_status()  # Raise exception for bad status codes
                
                response_data = response.json()

                data.append({
                    'datetime': format_to_gmt(response_data['Timestamp'][:19]),
                    'signal': response_data['Value'] if not isinstance(response_data['Value'], list) else response_data['Value'][0]
                })
                
            except requests.exceptions.RequestException as e:
                print(f"Request failed for {formatted_datetime}: {str(e)}")
            except Exception as e:
                print(f"Unexpected error for {formatted_datetime}: {str(e)}")
                
        current_date += timedelta(days=1)
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Convert timestamp to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(df['datetime']):
        df['datetime'] = pd.to_datetime(df['datetime'])
    
    return df

if __name__ == "__main__":
    try:
        username = Config().PIWEB_API_USER
        password = Config().PIWEB_API_PASS
        url = Config().PIWEB_API_URL
        
        # df = fetch(username, password, url)
        parts = get_parts()

        for part in parts:
            data = fetch(username, password, url, part[1])
        
        
    except Exception as e:
        print(f"Failed to fetch data: {str(e)}")