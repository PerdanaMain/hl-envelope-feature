from datetime import datetime, timedelta
import pytz


def format_to_gmt(date_str):
    date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
    local_tz = pytz.timezone("Asia/Jakarta")
    local_date = local_tz.localize(date)

    updated_date = local_date + timedelta(hours=7)

    return updated_date.strftime("%Y-%m-%dT%H:%M:%SZ")
