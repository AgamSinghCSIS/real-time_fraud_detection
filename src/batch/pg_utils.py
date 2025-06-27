import os
from sqlalchemy import create_engine, Engine, text
from src.common.logger import get_logger
from src.common.config_loader import load_schema, load_schema_file_path
import pandas as pd

logger = get_logger(os.environ.get("LOGGER_NAME"))
PARENT_RELATIVE_PATH = "../../"


def get_engine(username : str, password : str, host : str, database : str):
    logger.info(f"Creating postgres Engine for Database: {database}")
    try:
        engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}")
        logger.info(f"Successfully acquired engine!")
        return engine

    except Exception as e:
        logger.error(f"Failed to create engine: {e}")
        exit()


def upload_file_to_db(filepath, engine, schema_name, table_name, initial_load : bool = False):
    try:
        logger.info(f"Trying to upload file {filepath} to object {schema_name}.{table_name}")
        with open(filepath) as f:
            df = pd.read_csv(f)
            if initial_load:
                df.to_sql(con=engine, schema=schema_name, name=table_name, index=False, if_exists='fail')
            else:
                df.to_sql(con=engine, schema=schema_name, name=table_name, index=False, if_exists='append')
            logger.info(f"File uploaded Successfully!!!")

    except Exception as e:
        logger.error(f"load_file Function failed with error: {e}")

def initial_load(engine : Engine, table_list):
    try:
        schema_file = load_schema_file_path()
        schema_file_path = PARENT_RELATIVE_PATH + schema_file   # Starting path at the repo root
        logger.info(f"Schema file found at: {schema_file_path}")
        schema = load_schema(schema_file_path)

        for table in table_list:
            logger.info(f"Processing table {table['table_name']}")
            table_name = table['table_name']
            schema_name = table['schema']
            data_file = PARENT_RELATIVE_PATH + table['file_path']
            table_schema = schema[table_name]

            ddl = create_ddl(schema_name, table_name, table_schema)
            logger.debug(f"Trying to execute query: {ddl}")
            execute_query(engine, ddl)
            upload_file_to_db(filepath=data_file,engine=engine,schema_name=schema_name,table_name=table_name,initial_load=True)

    except Exception as e:
        logger.error(f"Error: Initial loading failed with error {e}")


def create_ddl(schema, table, table_schema):
    query = f"CREATE TABLE {schema}.{table} ( "
    i = 0
    for col in table_schema:
        if i != 0:
            query += ', '
        query += f"{col} {table_schema[col]}"
        i += 1
    query += " );"
    return query


def execute_query(engine : Engine, query : str, response_expected : bool = False, values : dict = None ):
    try:
        logger.info(f"POSTGRES QUERY EXECUTION: Trying to execute query {query}")
        with engine.connect() as conn:
            if response_expected:
                if values is None:
                    response = conn.execute(text(query)).fetchall()
                    logger.info(f"POSTGRES QUERY EXECUTION SUCCESSFUL: Response received! {response}")

                else:
                    response = conn.execute(text(query), values).fetchall()
                    logger.info(f"POSTGRES QUERY EXECUTION: Values: {values} successfully uploaded and response received!")

                conn.commit()
                return response

            else:
                if values is None:
                     conn.execute(text(query))
                else:
                    conn.execute(text(query), values)
                conn.commit()
                logger.info("POSTGRES QUERY EXECUTION: Query executed successfully")

    except Exception as e:
        logger.error(f"POSTGRES QUERY EXECUTION EXCEPTION: Error while executing Query: {e}")
        logger.info(f"POSTGRES QUERY EXECUTION EXCEPTION: Returning False")
        return False


def simulate_load(engine : Engine, table_list):
    """
    Description: This function simulates new users + updates into the dimension database.
               : For now new records are simply inserted, and no updates to existing entities are done
    :param table_list: List of Dictionaries loaded from the yaml file
    :return:
    """
    try:
        for table in table_list:
            logger.info(f"Processing table {table['table_name']}")
            table_name = table['table_name']
            schema_name = table['schema']
            data_file = PARENT_RELATIVE_PATH + table['file_path']

            upload_file_to_db(filepath=data_file, engine=engine, table_name=table_name, schema_name=schema_name, initial_load=False)

    except Exception as e:
        logger.error(f"Error while simulating load: {e}")






