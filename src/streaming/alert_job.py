import os
from deltalake import DeltaTable
from pyspark.sql import SparkSession
from src.common.spark_utils import local_get_spark
import time
import json
from dotenv import load_dotenv
load_dotenv()
import requests

SEEN_TX_FILE = "seen_tx_ids.json"
ALERTS_PATH = "C:/fraud_detection_project/silver/alerts"
USERS_PATH = "C:/fraud_detection_project/raw/users"
CARDS_PATH = "C:/fraud_detection_project/raw/credit_cards"
POLL_INTERVAL = 5  # seconds

def load_seen_tx_ids():
    if os.path.exists(SEEN_TX_FILE):
        with open(SEEN_TX_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_seen_tx_ids(tx_ids):
    with open(SEEN_TX_FILE, "w") as f:
        json.dump(list(tx_ids), f)

def get_dimension_details(spark : SparkSession):
    cc_df = spark.read.format("delta").table("raw.credit_cards")
    customer_df = spark.read.format("delta").table("raw.users")
    return cc_df, customer_df

def send_to_slack(alert_row, user_info, card_info):
    webhook_url = os.environ.get("SLACK_WEBHOOK")
    if not webhook_url:
        print("[Slack] No webhook URL found.")
        return

    message = (
        f"*FRAUD ALERT*\n"
        f"*Transaction ID:* {alert_row['transaction_id']}\n"
        f"*Amount:* ${alert_row['amount']:.2f}\n"
        f"*Rule Triggered:* {alert_row['rule_triggered']}\n"
        f"*User Name:* {user_info.get('name', 'N/A')}\n"
        f"*Email:* {user_info.get('email', 'N/A')}\n"
        f"*Credit Card:* {card_info.get('credit_card_number', 'N/A')}\n"
    )
    try:
        response = requests.post(webhook_url, json={"text": message})
        if response.status_code != 200:
            print(f"[Slack] Failed to send alert: {response.status_code} - {response.text}")
        else:
            print("[Slack] Alert sent successfully")
    except Exception as e:
        print(f"[Slack] Exception while sending alert: {e}")

def read_delta_table_as_df(path):
    return DeltaTable(path).to_pandas()

if __name__ == "__main__":
    print("Starting alert engine...")
    seen_tx_ids = load_seen_tx_ids()

    try:
        print("Loading dimension tables...")
        users_df = read_delta_table_as_df(USERS_PATH)
        cards_df = read_delta_table_as_df(CARDS_PATH)
    except Exception as e:
        print(f"[INIT] Failed to load dimension tables: {e}")
        exit(1)

    while True:
        try:
            alerts_df = read_delta_table_as_df(ALERTS_PATH)
            new_alerts = alerts_df[alerts_df['fraud_flag'] == True]
            new_alerts = new_alerts[~new_alerts['transaction_id'].isin(seen_tx_ids)]

            if new_alerts.empty:
                print("No new fraud alerts...")
            else:
                enriched = (
                    new_alerts
                    .merge(users_df, on='user_id', how='left', suffixes=('', '_user'))
                    .merge(cards_df, on='card_id', how='left', suffixes=('', '_card'))
                )

                for _, row in enriched.iterrows():
                    try:
                        user_info = {
                            "name": row.get("name"),
                            "email": row.get("email"),
                        }
                        card_info = {
                            "credit_card_number": row.get("credit_card_number"),
                        }
                        send_to_slack(row, user_info, card_info)
                        seen_tx_ids.add(row["transaction_id"])
                    except Exception as inner_e:
                        print(f"[PROCESS] Failed to process row: {inner_e}")

                save_seen_tx_ids(seen_tx_ids)

        except Exception as loop_e:
            print(f"[LOOP] Main loop error: {loop_e}")

        time.sleep(POLL_INTERVAL)