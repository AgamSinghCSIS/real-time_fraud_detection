import os
from confluent_kafka import Producer
from csv import DictReader
from src.common.logger import get_logger
import socket
import json

logger = get_logger(name=os.environ.get("LOGGER_NAME", "root"))


def get_producer(bootstrap_server : str, security_protocol : str, username : str, password : str) -> Producer:
    """
    :return: A Kafka Producer, that can be used to publish events to kafka
    """
    if security_protocol is not None:
        conf = {
            'bootstrap.servers' : bootstrap_server,
            'security.protocol' : security_protocol,
            'sasl.mechanism' : 'PLAIN',
            'sasl.username' : username,
            'sasl.password' : password,
            'client.id' : socket.gethostname()
        }
        return Producer(conf)
    else:
        conf = {
            'bootstrap.servers': bootstrap_server
        }
        return Producer(conf)

def delivery_report(err, msg):
    """
    Logs the acknowledges received from kafka
    """
    if err is not None:
        logger.error(f"Delivery Failed for {msg.key()}: {err}")
    else:
        logger.info(f"Delivered {msg.key()} to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()} Successfully")

def simulate_transactions(login_file_path : str, transaction_file_path : str, producer : Producer, login_topic : str, transaction_topic : str, key_dict : dict):
    logger.info(f"""Starting stream Simulation...
                    login file: {login_file_path} to topic: {login_topic}
                    transaction file: {transaction_file_path} to topic: {transaction_topic}""")

    with open(login_file_path) as l, open(transaction_file_path) as t:
        lgn_reader = DictReader(l)
        txn_list = list(DictReader(t))

        for login in lgn_reader:
            login_key = login[key_dict[login_topic]]
            login_record = json.dumps(login)
            producer.produce(
                topic=login_topic,
                key=login_key.encode('utf-8'),
                value=login_record.encode('utf-8'),
                callback=delivery_report
            )

            txn_list = find(login["session_id"], txn_list)

            for txn in txn_list:
                txn_key = txn[key_dict[transaction_topic]]
                txn_record = json.dumps(txn)
                print(txn_key, txn_record)
                producer.produce(
                    topic=transaction_topic,
                    key=txn_key.encode('utf-8'),
                    value=txn_record.encode('utf-8'),
                    callback=delivery_report
                )
        producer.flush()

def find(session_id, txn_data : list):
    print(f"Session_id : {session_id}")
    return [txn for txn in txn_data if txn["session_id"] == session_id]

