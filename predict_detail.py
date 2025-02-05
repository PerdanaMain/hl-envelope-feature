from model import (
    get_current_feature_value,
    get_detail,
    update_detail,
    update_percent_condition,
)
from datetime import datetime, timedelta


def checking_status(values, detail):
    """
    Mencari status prediksi berdasarkan upper dan lower threshold
    """
    upper_threshold = detail[2]
    lower_threshold = detail[3]

    result = []

    value = float(values[3]) if values[3] is not None else 0 # nilai
    dt = values[2]  # datetime
    # print(value[1] < lower_threshold)

    if value >= lower_threshold and value < upper_threshold:
        result.append({"datetime": dt, "status": "warning", "value": value})
    elif value >= upper_threshold:
        result.append({"datetime": dt, "status": "predicted failed", "value": value})
    else:
        result.append({"datetime": dt, "status": "normal", "value": value})
    return result[0]


def percent_calculation(part_id, feature_id, status):
    if status == "predicted failed":
        return update_percent_condition(part_id, 0, 0)

    detail = get_detail(part_id)
    upper_threshold = detail[2]  # fail threshold
    lower_threshold = detail[3]  # warning threshold
    one_percent_condition = detail[6]  # normal value

    current_value, data = get_current_feature_value(part_id, feature_id=feature_id)

    # Hitung percent_condition menggunakan batas fail
    percent_condition = (
        abs(upper_threshold - current_value)
        / abs(upper_threshold - one_percent_condition)
        * 100
    )

    # Set warning_percent sama dengan percent_condition jika current_value dalam range normal
    warning_percent = percent_condition if current_value <= lower_threshold else 100

    percent_condition = round(percent_condition, 2)
    warning_percent = round(warning_percent, 2)

    print(percent_condition, warning_percent)

    update_percent_condition(part_id, percent_condition, warning_percent)


def main(part_id):
    print("mengambil data ...")
    detail = get_detail(part_id)
    features_id = "9dcb7e40-ada7-43eb-baf4-2ed584233de7"
    value, current_value = get_current_feature_value(part_id, features_id)
    # print("detail: ", detail)
    # print("current: ", current_value)

    print("menghitung status ...")
    result = checking_status(current_value, detail)

    if result["status"] == "predicted failed":
        update_detail(part_id, result["status"], result["datetime"], result["value"])
    else:
        update_detail(part_id, result["status"], None, None)

    print("menghitung persentase kondisi ...")
    percent_calculation(part_id, features_id, result["status"])

    print("done")


if __name__ == "__main__":
    # main()
    main("d631f5a1-832b-4c76-9b70-9f47bd1e8aa9")
    # percent_calculation("64492e3f-8e1f-4eb4-b9ea-8a2ead652c8e", "9dcb7e40-ada7-43eb-baf4-2ed584233de7")
    # print("test command")
