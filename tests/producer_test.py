import csv
import json
import os
from confluent_kafka import Consumer
from src.streaming.producer import producer_main
from time import sleep

KAFKA_CONFIG = {
    "bootstrap.servers": os.environ.get("BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.environ.get("KAFKA_API_KEY"),
    'sasl.password': os.environ.get("KAFKA_API_SECRET"),
    "group.id": "pytest-group",
    "auto.offset.reset": "earliest"
}

def test_producer():
    test_login_file = '../data/test_logins.csv'
    test_transaction_file = '../data/test_transactions.csv'
    login_topic = "test_logins"
    txn_topic = "test_transactions"

    # Load expected data
    with open(test_login_file, 'r') as f:
        expected_logins = list(csv.DictReader(f))

    with open(test_transaction_file, 'r') as f:
        expected_txns = list(csv.DictReader(f))

    # Testing Main producer function
    producer_main(test_login_file, test_transaction_file, login_topic=login_topic, transaction_topic=txn_topic)

    sleep(5)

    # Step 1: Consume logins
    login_consumer = Consumer(KAFKA_CONFIG)
    login_consumer.subscribe([login_topic])
    login_messages = []
    for _ in range(len(expected_logins)):
        msg = login_consumer.poll(timeout=3.0)
        if msg is None:
            print("No message received in this poll")
        elif msg.error():
            print(f"Error in message: {msg.error()}")
        else:
            print("Appending msg in logins")
            login_messages.append(json.loads(msg.value().decode()))

    login_consumer.close()

    # Step 2: Consume transactions
    txn_consumer = Consumer(KAFKA_CONFIG)
    txn_consumer.subscribe([txn_topic])
    txn_messages = []
    for _ in range(len(expected_txns)):
        msg = txn_consumer.poll(timeout=3.0)
        if msg is None:
            print("No message received in this poll")
        elif msg.error():
            print(f"Error in message: {msg.error()}")
        else:
            print("Appending msg in transactions")
            txn_messages.append(json.loads(msg.value().decode()))

    txn_consumer.close()
    # Step 3: Assert values (rough match - depending on what producer does)
    assert len(login_messages) == len(expected_logins)
    assert len(txn_messages) == len(expected_txns)

    assert login_messages == expected_logins
    assert txn_messages == expected_txns
    # This test needs to be modified to only read the latest message it finds from the test topics not all





