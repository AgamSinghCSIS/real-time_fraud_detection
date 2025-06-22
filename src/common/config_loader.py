import yaml
import os
from src.common.logger import get_logger

logger = get_logger(os.environ.get("LOGGER_NAME"))

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



