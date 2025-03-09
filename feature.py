from model import *
from signal_envelope import find_signal_envelopes
from arima import execute_arima
import pandas as pd  # type: ignore
from predict_detail import main as predict_detail


def execute_feature(part_id):
    part = get_part(part_id)

    # data = get_envelope_values(part[0])
    # print(f"Fetched {len(data)} records for part {part[1]}")

    # df = pd.DataFrame(data, columns=["value", "datetime"])
    # signal_values = df["value"].values
    # min_indices, max_indices = find_signal_envelopes(signal_values)

    # print(f"Found {len(max_indices)} maxima")
    # save_envelopes_to_db(
    #     part[0], df, max_indices, features_id="9dcb7e40-ada7-43eb-baf4-2ed584233de7"
    # )

    predict_detail(part[0])


def index():
    try:
        # parts = get_parts()
        parts = get_non_dcs()
        for part in parts:
            try:
                execute_feature(part[0])
                print(f"success execute part: {part[3]}")
            except:
                continue
    except Exception as e:
        print(f"Failed to fetch data: {str(e)}, part: {part[3]}")


def run_selected_part():
    try:
        config = Config()
        # parts = get_parts()
        parts = get_new_parts()
        counter = 0

        for part in parts:
            try:
                exists = checking_features_values(part[0])
                if exists is not None:
                    print(f"Data feature sudah ada untuk part {part[3]}")
                    continue

                print(f"Data feature belum ada untuk part {part[3]}")
                counter += 1

                execute_feature(part[0])

            except Exception as e:
                print(f"Gagal memproses part {part[3]}: {str(e)}")
                continue

        print(f"Total part yang belum memiliki data: {counter}")
        print(f"Total part yang dicek: {len(parts)}")
    except Exception as e:
        print(f"Gagal menginisialisasi: {str(e)}")
        raise


def delete_feature_by_selected_part():
    try:
        parts = get_parts()
        print(len(parts))
        for part in parts:
            delete_feature_by_part(part[0])
    except Exception as e:
        print(f"Gagal menghapus data feature: {str(e)}")
        raise


def undo_fetch_envelope():
    try:
        parts = get_parts()
        counter = 0

        print("checking part prepare for deleting")
        for part in parts:
            count = get_count_envelope(part_id=part[0])
            if count[0] < 30:
                delete_envelope(part_id=part[0])
                counter += 1

        print("total deleted part in envelope: ", counter)
    except Exception as e:
        print(f"Gagal menghapus data feature: {str(e)}")
        raise


if __name__ == "__main__":
    index()
    # undo_fetch_envelope()
    # execute_feature()
    # run_selected_part()
    # delete_feature_by_selected_part()
    # parts = get_parts()
    # print(f"Fetched {len(parts)} parts")

    # for part in parts:
    #     # show if empty values
    #     data = get_envelope_values(part[0])
    #     print(f"Fetched {len(data)} records for part {part[3]}")
