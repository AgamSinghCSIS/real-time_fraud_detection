import yaml
import os

from src.common.logger import get_logger

logger = get_logger(os.environ.get("LOGGER_NAME"))
PARENT_RELATIVE_PATH = '../../'

def load_config(config_path):
    if not os.path.exists(config_path):
        logger.error(f"Config file {config_path} Not Found!")
        return None
    else:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)

def load_database_config():
    config_path = '../../configs/db_config.yaml'
    logger.info(f"Trying to load config file: {config_path}")
    configs = load_config(config_path)
    return configs['initial_load'], configs['tables']

def load_schema_file_path():
    config_path = '../../configs/db_config.yaml'
    configs = load_config(config_path)
    try:
        schema_file_path = configs['schema_file_path']
        if schema_file_path:
            return schema_file_path
        else:
            return None
    except Exception as e:
        logger.error(f"No Schema File path found: {e}")
        print("No schema file path var in yaml file found")
        return None

def load_schema(schema_file_path):
    schema = load_config(schema_file_path)
    return schema

def load_ingestion_configs(pipeline : str, source : str):
    config_path = PARENT_RELATIVE_PATH + 'configs/ingestion.yaml'
    logger.info(f"Trying to load config file: {config_path}")
    configs = load_config(config_path=config_path)

    if pipeline.lower() == 'batch':
        if source.lower() == 'dim_store':
            tables_list = configs["batch"]["dim_store"]
            return tables_list

        elif source.lower() == 'filedrops':
            pass

    elif pipeline.lower() == 'streaming':
        if source.lower() == 'kafka':
            sources = configs['streaming']['kafka']
            return sources

        else:
            logger.critical("Source is not configured!")
            return False
    else:
        return False

def load_column_list(table_name):
    config_path = PARENT_RELATIVE_PATH + 'configs/ingestion.yaml'
    configs =  load_config(config_path)
    table_list = configs["batch"]["dim_store"]["tables"]

    for table in table_list:
        if table['source_table'] == table_name:
            return table['expected_columns']
    logger.error(f"Expected Columns list could not be loaded for table: {table_name}")
    return None

