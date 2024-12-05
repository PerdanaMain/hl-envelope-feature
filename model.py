import uuid
import pandas as pd # type: ignore
from config import Config

def get_parts():
    try:
        conn = Config.get_connection()
        cur = conn.cursor()

        query = "SELECT id, web_id, type_id, part_name  FROM pf_parts WHERE type_id != '673b26b9-fb94-40aa-8c33-ccea214c0ef3'"

        cur.execute(query)
        parts = cur.fetchall()
        return parts
    except Exception as e:
        print(f'An exception occurred: {e}')
    finally:
        if conn:
            conn.close()

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
                row.datetime.strftime('%Y-%m-%d %H:%M:%S'),  # Format datetime ke string
                row.datetime.strftime('%Y-%m-%d %H:%M:%S')   # Format datetime ke string
            )
            for _, row in data.iterrows()
        ]
        
        cur.executemany("""
            INSERT INTO dl_envelope_fetch 
            (id, part_id, value, created_at, updated_at) 
            VALUES (%s, %s, %s, %s, %s)
        """, values)
        conn.commit()
        
        print(f"Successfully inserted {len(values)} records for part {part_id}")
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f'Failed to insert envelope data: {e}')
        
    finally:
        if conn:
            cur.close()
            conn.close()