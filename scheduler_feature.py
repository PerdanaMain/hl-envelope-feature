import time
import pytz
import schedule # type: ignore
from log import print_log
from datetime import datetime
from main import feature



def index():
    print(f"Starting scheduler for feature high env at: {datetime.now(pytz.timezone('Asia/Jakarta'))}")
    print_log(f"Starting scheduler for feature high env at: {datetime.now(pytz.timezone('Asia/Jakarta'))}")

    # Schedule task setiap hari
    schedule.every().day.at("00:15").do(feature)
    
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