import schedule
import time
import subprocess

def run_load_to_staging():
    subprocess.run(['python3', 'load_to_staging.py'])

def run_load_to_dwh():
    subprocess.run(['python3', 'load_to_dwh.py'])

def main():
    print("Enter the time to run the scripts (in 24-hour format, e.g., 14:30 for 2:30 PM): ")
    run_time = input("Time (HH:MM): ")

    # Schedule the tasks
    schedule.every().day.at(run_time).do(run_load_to_staging)
    schedule.every().day.at(run_time).do(run_load_to_dwh)

    print(f"Scheduled 'load_to_staging.py' and 'load_to_dwh.py' to run daily at {run_time}")

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
