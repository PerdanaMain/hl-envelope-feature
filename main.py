from datetime import datetime, timedelta
from predict_detail import main as predict_detail
from plot import find_signal_envelopes
from format_gmt import format_to_gmt
from requests.auth import HTTPBasicAuth
from config import Config
from model import *
from log import print_log
from arima import execute_arima
from flask import Flask, request, jsonify
import pandas as pd  # type: ignore
import math
import requests
import urllib3
import time
import pytz
import schedule  # type: ignore

# maximo wdsl fetch dependencies
from zeep import Client
from zeep.transports import Transport
from requests import Session
from zeep.plugins import HistoryPlugin

app = Flask(__name__)


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
    current_time = datetime.now(pytz.timezone("Asia/Jakarta"))
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
        signal_value = data["Value"]
        if isinstance(signal_value, dict):
            signal_value = signal_value.get("Value", 0)

        if isinstance(signal_value, (int, float)) and (
            math.isnan(signal_value) or math.isinf(signal_value)
        ):
            signal_value = 0

        # Create DataFrame with single record
        df = pd.DataFrame(
            [
                {
                    "datetime": pd.to_datetime(format_to_gmt(data["Timestamp"][:19])),
                    "signal": float(signal_value) if signal_value is not None else 0,
                }
            ]
        )

        print(f"Fetched data for {timestamp} - Web ID: {web_id}")
        print_log(f"Fetched data for {timestamp} - Web ID: {web_id}")
        return df

    except Exception as e:
        print(f"Error fetching data for {web_id} at {timestamp}: {e}")
        print_log(f"Error fetching data for {web_id} at {timestamp}: {e}")
        return pd.DataFrame()

# Fungsi untuk mengambil nilai dari objek Zeep yang kompleks
def extract_value(field):
    """Mengambil nilai dari objek Zeep dengan memastikan hanya nilai murni yang diambil."""
    if field is None:
        return None
    
    try:
        if hasattr(field, "_value_1"):
            return field._value_1
        elif isinstance(field, dict) and "_value_1" in field:
            return field["_value_1"]
        return field
    except (AttributeError, TypeError, KeyError):
        return None

# Fungsi untuk menghapus timezone dari datetime
def remove_timezone(date_str):
    """Menghapus informasi timezone dari format datetime."""
    if date_str is None:
        return None
        
    try:
        dt = pd.to_datetime(date_str)
        return dt.replace(tzinfo=None)
    except Exception:
        # Fix typo from date_st to date_str
        return date_str

def extract_maximo():
    try:
        current_time = datetime.now(pytz.timezone("Asia/Jakarta"))
        print(f"Extract Maximo running at: {current_time}")
        print_log(f"Extract Maximo running at: {current_time}")

        config = Config()

        # Definisi variabel tanggal format: YYYY-MM-DD
        # start_date = "2019-01-01"
        # end_date = "2019-03-31"

        metadata = get_metadata_maximo_etl()

        if not metadata:
            start_date_str = "2019-01-01"
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = start_date + timedelta(weeks=1)
            end_date_str = end_date.strftime("%Y-%m-%d")
        else:
            start_date = metadata
            end_date = start_date + timedelta(weeks=1)
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
        
        # Membuat body request SOAP dengan format string yang benar
        query_params = {
            "CXWOQuery": {
                "WHERE": f"((WORKORDER.REPORTDATE < '{end_date_str}' " # f"WORKORDER.STATUS = 'COMP' AND "
                        f"AND WORKORDER.REPORTDATE > '{start_date_str}') "
                        f"OR (WORKORDER.STATUSDATE < '{end_date_str}' "
                        f"AND WORKORDER.STATUSDATE > '{start_date_str}')) "
                        "ORDER BY WORKORDER.STATUSDATE DESC",
                "WORKORDER": {}
            }
        }

        # URL WSDL
        wsdl_url = "http://172.16.3.40:9080/meaweb/wsdl/MX-DT_CXWOQuery.wsdl"

        # Base64-encoded credentials
        maxauth_token = "bWF4YWRtaW46cjM1N3IxYzczZDM0"

        # Inisialisasi sesi dengan autentikasi menggunakan header "maxauth"
        session = Session()
        session.headers.update({
            "maxauth": maxauth_token,
            "Content-Type": "text/xml; charset=utf-8",
        })

        transport = Transport(session=session)
        history = HistoryPlugin()

        # Inisialisasi klien SOAP
        client = Client(wsdl_url, transport=transport, plugins=[history])

        print("Fetching Maximo...")
        print_log("Fetching Maximo...")
        # Memanggil API SOAP
        try:
            response = client.service.QueryCXWO(**query_params)

            # Debugging: Cetak response untuk memastikan ada data
            # print("Response dari API:")
            # print(response[0])

            # Pastikan `CXWOSet` ada sebelum mengakses `WORKORDER`
            if not response or not hasattr(response, "CXWOSet") or response.CXWOSet is None:
                print("Tidak ada data yang ditemukan dalam response.")
                exit()

            workorders = response.CXWOSet.WORKORDER if hasattr(response.CXWOSet, "WORKORDER") else []

            # Jika tidak ada workorder
            if not workorders:
                print("Tidak ada data WORKORDER yang ditemukan.")
                exit()

            # Tampilkan jumlah data yang ditemukan
            total_records = len(workorders)
            print(f"Found {total_records} work orders in response")
            print_log(f"Found {total_records} work orders in response")

            # Menyiapkan data untuk DataFrame pandas
            data = []
            for i, wo in enumerate(workorders):
                # Print progress every 100 records
                if i % 100 == 0 or i == total_records - 1:
                    progress_pct = (i + 1) / total_records * 100
                    print(f"Processing {i+1}/{total_records} records ({progress_pct:.1f}%)")
                    print_log(f"Processing {i+1}/{total_records} records ({progress_pct:.1f}%)")

                data.append({
                    "WONUM": extract_value(wo.WONUM),
                    "WORKTYPE": extract_value(wo.WORKTYPE),
                    "ASSETNUM": extract_value(wo.ASSETNUM),
                    "SITEID": extract_value(wo.SITEID),
                    "STATUS": extract_value(wo.STATUS),
                    "STATUSDATE": remove_timezone(extract_value(wo.STATUSDATE)),
                    "REPORTDATE": remove_timezone(extract_value(wo.REPORTDATE)),
                    "ACTMATCOST": extract_value(wo.ACTMATCOST),
                    "ACTSERVCOST": extract_value(wo.ACTSERVCOST),
                    "ACTSTART": remove_timezone(extract_value(wo.ACTSTART)),  # Tambahan
                    "ACTFINISH": remove_timezone(extract_value(wo.ACTFINISH)),  # Tambahan
                    "TARGSTARTDATE": remove_timezone(extract_value(wo.TARGSTARTDATE)),
                    "TARGCOMPDATE": remove_timezone(extract_value(wo.TARGCOMPDATE)),
                    "WOGROUP": extract_value(wo.WOGROUP),
                    "WOJP8": extract_value(wo.WOJP8),
                    # "STATUSIFACE": extract_value(wo.STATUSIFACE),  # Tambahan
                    # "DESCRIPTION_LONGDESCRIPTION": extract_value(wo.DESCRIPTION_LONGDESCRIPTION),  # Tambahan
                    # "FCPROJECTID": extract_value(wo.FCPROJECTID),  # Tambahan
                    # "FCTASKID": extract_value(wo.FCTASKID),  # Tambahan
                    # "NP_STATUSMEMO": extract_value(wo.NP_STATUSMEMO)  # Tambahan
                })
        
            # Membuat DataFrame pandas
            df = pd.DataFrame(data)

            # Simpan ke file Excel
            # file_path = "hasil.xlsx"
            # df.to_excel(file_path, index=False, engine="openpyxl")
            
            # Simpan ke database collector
            save_maximo_to_db(df, start_date, end_date)

            # save_maximo_metadata_etl_to_db(start_date, end_date, len(df))

            # print(f"Data berhasil disimpan ke {file_path}")
            print(f"Data berhasil disimpan ke database collector")
            print(f"Task Extract Maximo completed at: {datetime.now(pytz.timezone('Asia/Jakarta'))}")
            print_log(f"Task Extract Maximo completed at: {datetime.now(pytz.timezone('Asia/Jakarta'))}")

            return f"Maximo Data Extracted: {len(df)}"
        
        except Exception as e:
            print(f"Error executing task: {e}")
            print_log(f"Error executing task: {e}")

    except Exception as e:
        print(f"Error executing task: {e}")
        print_log(f"Error executing task: {e}")
          

def task():
    try:
        current_time = datetime.now(pytz.timezone("Asia/Jakarta"))
        print(f"Task running at: {current_time}")
        print_log(f"Task running at: {current_time}")

        config = Config()
        parts = get_parts()
        print(f"Processing total: {len(parts)} parts")

        for part in parts:
            try:
                if part[2] == None:
                    continue

                print(
                    f"Processing part: {part[3]}"
                )  # Assuming part[3] contains part name
                print_log(
                    f"Processing part: {part[3]}"
                )  # Assuming part[3] contains part name
                data = fetch(
                    config.PIWEB_API_USER,
                    config.PIWEB_API_PASS,
                    config.PIWEB_API_URL,
                    part[1],
                )

                print(f"Data: {data}")

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
    # Mendapatkan timestamp untuk awal hari kemarin (00:00:00)
    yesterday_start = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # Mendapatkan timestamp untuk akhir hari kemarin (23:59:59)
    yesterday_end = yesterday_start + timedelta(days=1) - timedelta(seconds=1)
    print(yesterday_start)
    print(yesterday_end)

    parts = get_parts()
    print(f"processing feature for {len(parts)}")
    print_log(f"processing feature for {len(parts)}")

    for part in parts:
        print(f"processing feature for {part[3]}")

        data = get_envelope_values_by_date(
            part[0], start_date=yesterday_start, end_date=yesterday_end
        )
        df = pd.DataFrame(data, columns=["value", "datetime"])
        signal_values = df["value"].values
        min_indices, max_indices = find_signal_envelopes(signal_values)

        print(f"Found {len(max_indices)} maxima")
        save_envelopes_to_db(
            part[0], df, max_indices, features_id="9dcb7e40-ada7-43eb-baf4-2ed584233de7"
        )
        predict_detail(part[0])

    print(
        f"Task feature high env completed at: {datetime.now(pytz.timezone('Asia/Jakarta'))}"
    )
    print_log(
        f"Task feature high env completed at: {datetime.now(pytz.timezone('Asia/Jakarta'))}"
    )


def index():
    print(f"Starting scheduler at: {datetime.now(pytz.timezone('Asia/Jakarta'))}")
    print_log(f"Starting scheduler at: {datetime.now(pytz.timezone('Asia/Jakarta'))}")

    # Schedule task setiap 1 jam
    schedule.every().hour.at(":00").do(task)

    next_run = schedule.next_run()
    print(f"Next scheduled run at: {next_run}")
    print_log(f"Next scheduled run at: {next_run}")

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


@app.route("/", methods=["GET"])
def hai():
    return jsonify({"message": "Welcome to the API!"})

@app.route("/fetch-maximo", methods=["GET"])
def home_maximo():
    try:
        maximo_data = extract_maximo()
        return (
            jsonify(
                {
                    "message": f"{maximo_data} at: {datetime.now(pytz.timezone('Asia/Jakarta'))}"
                }
            ),
            200,
        )
    except Exception as e:
        return jsonify({"maximo route error": str(e)}), 500

@app.route("/fetch-envelope", methods=["GET"])
def home():
    try:
        task()
        return (
            jsonify(
                {
                    "message": f"Task completed at: {datetime.now(pytz.timezone('Asia/Jakarta'))}"
                }
            ),
            200,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/calculate-feature", methods=["GET"])
def home_feature():
    try:
        feature()
        return (
            jsonify(
                {
                    "message": f"Task completed at: {datetime.now(pytz.timezone('Asia/Jakarta'))}"
                }
            ),
            200,
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=Config.PORT, host='0.0.0.0')
    # index()
    # feature()
    # task()
    # predict_detail(part_id='30513c74-4f25-4543-99d7-90503e022c5c')
    # extract_maximo()
