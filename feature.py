from model import *
from signal_envelope import find_signal_envelopes
import pandas as pd  # type: ignore

def index():
    try:
        parts = get_parts()
        for part in parts:
            data = get_envelope_values(part[0])
            print(f"Fetched {len(data)} records for part {part[3]}")

            df = pd.DataFrame(data, columns=["value", "datetime"])
            signal_values = df["value"].values
            min_indices, max_indices = find_signal_envelopes(signal_values)

            print(f"Found {len(max_indices)} maxima")

            save_envelopes_to_db(part[0], df, max_indices, features_id='24c24b32-949b-474e-b500-9232c9f7ab65')
            
    except Exception as e:
        print(f"Failed to fetch data: {str(e)}")


if __name__ == '__main__':
  # index()
  pass