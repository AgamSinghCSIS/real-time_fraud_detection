import os
from dotenv import load_dotenv
load_dotenv()
from src.common.logger import init_logger
logger_name = 'db_loader'
os.environ["LOGGER_NAME"] = logger_name
logger = init_logger(logger_name, 'database_loader.log')

from src.batch.pg_utils import get_engine, initial_load, simulate_load
from src.common.config_loader import load_database_config



if __name__ == "__main__":
    user = os.environ.get('DIMSTORE_USER')
    password = os.environ.get('DIMSTORE_PASS')
    host = os.environ.get('DIMSTORE_HOST')
    db = os.environ.get('DIMSTORE_DB')

    engine = get_engine(username=user, password=password, host=host, database=db)
    initial_load_flag, table_list = load_database_config()

    if initial_load_flag:
        print("Initial Load!")
        logger.info(f"Starting Initial Load...")
        initial_load(engine=engine, table_list=table_list)
    else:
        print("Incremental Load!")
        logger.info("Starting Simulation of records into database...")
        simulate_load(engine=engine, table_list=table_list)


