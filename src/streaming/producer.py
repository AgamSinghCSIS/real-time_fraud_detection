import os
from dotenv import load_dotenv
load_dotenv()

from src.common.logger import init_logger

os.environ["LOGGER_NAME"] = "Producer"
logger = init_logger(name=os.environ["LOGGER_NAME"], logfile="kafka_producer.log")

from src.streaming.kafka_utils import get_producer, simulate_transactions

def producer_main(login_file, transaction_file, login_topic : str = 'logins', transaction_topic : str = 'transactions'):
    logger.info("Starting Producer...")
    bootstrap_server: str = os.environ.get("BOOTSTRAP_SERVERS")
    security_protocol: str = 'SASL_SSL'
    username: str = os.environ.get("KAFKA_API_KEY")
    password: str = os.environ.get("KAFKA_API_SECRET")

    key_dict = {
        "transactions": "transaction_id",
        "logins": "login_id",
        "test_transactions": "transaction_id",
        "test_logins": "login_id",
    }

    producer = get_producer(bootstrap_server=bootstrap_server, security_protocol=None, username=username, password=password)
    simulate_transactions(
        login_file_path=login_file,
        transaction_file_path=transaction_file,
        producer=producer, transaction_topic=transaction_topic, login_topic=login_topic, key_dict=key_dict
    )


if __name__ == '__main__':
    producer_main(login_file='../../data/test_logins.csv', transaction_file='../../data/test_transactions.csv')


