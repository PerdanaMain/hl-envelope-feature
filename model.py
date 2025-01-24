import uuid
import pandas as pd  # type: ignore
import pytz
from config import Config
from datetime import datetime


def get_parts():
    try:
        conn = Config.get_connection()
        cur = conn.cursor()

        query = "SELECT id, web_id, type_id, part_name  FROM pf_parts WHERE web_id IS NOT NULL"

        cur.execute(query)
        parts = cur.fetchall()
        return parts
    except Exception as e:
        print(f"An exception occurred: {e}")
    finally:
        if conn:
            conn.close()


def get_part(part_id):
    try:
        conn = Config.get_connection()
        cur = conn.cursor()

        query = "SELECT id, part_name FROM pf_parts WHERE id = %s "
        cur.execute(query, (part_id,))
        parts = cur.fetchone()
        return parts
    except Exception as e:
        print(f"An exception occurred: {e}")
    finally:
        if conn:
            conn.close()

def delete_feature_by_part(part_id):
    try:
        conn = Config.get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM dl_features_data WHERE part_id = %s", (part_id,))
        conn.commit()
    except Exception as e:
        print(f"An exception occurred: {e}")
    finally:
        if conn:
            conn.close()    

def get_envelope_values(part_id, current_date):
    conn = None
    try:
        conn = Config.get_fetch_connection()
        cur = conn.cursor()

        query = "SELECT value, created_at as datetime FROM dl_envelope_fetch WHERE part_id = %s AND created_at < %s ORDER BY created_at ASC"
        cur.execute(query, (part_id, current_date))
        parts = cur.fetchall()
        return parts
    except Exception as e:
        print(f"An exception occurred: {e}")
    finally:
        if conn:
            conn.close()

def get_envelope_values_by_date(part_id, start_date, end_date):
    conn = None
    try:
        conn = Config.get_fetch_connection()
        cur = conn.cursor()

        query = "SELECT value, created_at as datetime FROM dl_envelope_fetch WHERE part_id = %s AND created_at BETWEEN %s AND %s ORDER BY created_at ASC"
        cur.execute(query, (part_id, start_date, end_date))
        parts = cur.fetchall()
        return parts
    except Exception as e:
        print(f"An exception occurred: {e}")
    finally:
        if conn:
            conn.close()    
def checking_envelope_values(part_id):
    conn = None
    try:
        conn = Config.get_fetch_connection()
        cur = conn.cursor()
        
        query = "SELECT id, part_id, value, created_at as datetime FROM dl_envelope_fetch where part_id = %s limit 1"
        cur.execute(query, (part_id, ))
        parts = cur.fetchone()
        return parts
    except Exception as e:
        print(f"An exception occurred: {e}")
        return None
    finally:
        if conn:
            conn.close()
            
def checking_features_values(part_id):
    conn = None
    try:
        conn = Config.get_connection()
        cur = conn.cursor()
        
        query = "SELECT * FROM dl_features_data where part_id = %s limit 1"
        cur.execute(query, (part_id, ))
        parts = cur.fetchone()
        return parts
    except Exception as e:
        print(f"An exception occurred: {e}")
        return None
    finally:
        if conn:
            conn.close()

def get_feature_values(part_id,features_id):
    try:
        conn = Config.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, part_id, date_time, value 
            FROM dl_features_data
            WHERE part_id = %s AND features_id = %s
            order by date_time asc
            """,
            (part_id, features_id),
        )
        values = cur.fetchall()
        cur.close()
        conn.close()
        print("Data fetched successfully, count: ", len(values))
        return values
    except Exception as e:
        print(f"An exception occurred {e}")

def create_predict(part_id, features_id, values, timestamps):
    try:
        now = datetime.now(pytz.timezone("Asia/Jakarta")).strftime("%Y-%m-%d %H:%M:%S")
        conn = Config.get_connection()
        cur = conn.cursor()
        
        # SQL Query
        sql = """
        INSERT INTO dl_predict (id, part_id, features_id, date_time, pfi_value, status, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s,%s, %s)
        """
        
        # Iterasi dan eksekusi untuk setiap prediksi
        data_to_insert = []
        for value, timestamp in zip(values, timestamps):
            predict_id = str(uuid.uuid4())  # Generate a new UUID for each record
            value = float(value)
            data_to_insert.append((predict_id, part_id, features_id, timestamp, value, "normal", now, now))
        
        # Execute batch insert
        cur.executemany(sql, data_to_insert)
        # Commit perubahan
        conn.commit()
    
    except Exception as e:
        print(f"An exception occurred: {e}")
    
    finally:
        # Pastikan koneksi ditutup
        if cur:
            cur.close()
        if conn:
            conn.close()


def delete_predicts(part_id, features_id):
    try:
        conn = Config.get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM dl_predict
            WHERE part_id = %s AND features_id = %s
            """,
            (part_id, features_id),
        )
        conn.commit()
    except Exception as e:
        print(f"An exception occurred while deleting predicts: {e}")


def create_envelope(data: pd.DataFrame, part_id: str) -> None:
    conn = None
    try:
        conn = Config.get_fetch_connection()
        cur = conn.cursor()

        # Debug print
        print("DataFrame info:")
        print(data.info())
        print("\nFirst row sample:")
        print(data.iloc[0])

        # Konversi data ke format yang sesuai
        values = [
            (
                str(uuid.uuid4()),
                part_id,
                float(row.signal),  # Pastikan signal adalah float
                row.datetime.strftime("%Y-%m-%d %H:%M:%S"),  # Format datetime ke string
                row.datetime.strftime("%Y-%m-%d %H:%M:%S"),  # Format datetime ke string
            )
            for _, row in data.iterrows()
        ]

        cur.executemany(
            """
            INSERT INTO dl_envelope_fetch
            (id, part_id, value, created_at, updated_at) 
            VALUES (%s, %s, %s, %s, %s)
        """,
            values,
        )
        conn.commit()

        print(f"Successfully inserted {len(values)} records for part {part_id}")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Failed to insert envelope data: {e}")

    finally:
        if conn:
            cur.close()
            conn.close()

def save_envelopes_to_db(part_id: str, df: pd.DataFrame, max_indices, features_id):
    """
    Menyimpan nilai envelope ke database
    
    Args:
        part_id: ID dari part
        df: DataFrame dengan data signal
        max_indices: Array indeks nilai maksimum
    """
    conn = None
    try:
        conn = Config.get_connection()
        cur = conn.cursor()
        now = datetime.now(pytz.timezone("Asia/Jakarta")).strftime("%Y-%m-%d %H:%M:%S")
        
        # Siapkan query insert
        query = """
            INSERT INTO dl_features_data 
            (id, features_id, date_time, part_id, value, created_at, updated_at) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        # Siapkan data untuk high envelope
        high_envelopes = [
            (
                str(uuid.uuid4()),
                features_id,
                df['datetime'].iloc[idx],
                part_id,
                float(df['value'].iloc[idx]),
                now,
                now,
            )
            for idx in max_indices
        ]
        
        # Insert data
        cur.executemany(query, high_envelopes)
        conn.commit()
        
        print(f"Successfully saved {len(high_envelopes)} high envelopes  for part {part_id}")
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Failed to save envelopes: {str(e)}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()
