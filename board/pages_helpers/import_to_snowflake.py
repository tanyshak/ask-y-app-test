import snowflake.connector
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_conn(user, password, account):
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account)
    return conn

def create_storage_integration(conn,
                storage_allowed_location,
                si_name):

    sql = f"""CREATE OR REPLACE STORAGE INTEGRATION {si_name}
          TYPE = EXTERNAL_STAGE
          STORAGE_PROVIDER = 'GCS'
          ENABLED = TRUE
          STORAGE_ALLOWED_LOCATIONS = ('{storage_allowed_location}')"""
    logger.info(f'create_storage_integration: storage_allowed_location={storage_allowed_location}, si_name={si_name}')
    logger.info(sql)
    try:
        conn.cursor().execute(sql)
        logger.info('create storage integration executed successfully')
    except Exception as e:
        logger.error(f'Error during create storage integration execution: {e}')

def create_db(conn, db_name):

    sql = f"""CREATE DATABASE IF NOT EXISTS {db_name}"""
    logger.info(f'create_db: db_name={db_name}')
    logger.info(sql)
    try:
        conn.cursor().execute(sql)
        logger.info('create db executed successfully')
    except Exception as e:
        logger.error(f'Error during create db execution: {e}')

def use_db(conn, db_name):

    sql = f"""USE  DATABASE  {db_name}"""
    logger.info(f'use_db: db_name={db_name}')
    logger.info(sql)
    try:
        conn.cursor().execute(sql)
        logger.info('Use db executed successfully')
    except Exception as e:
        logger.error(f'Error during use db execution: {e}')

def create_stage(conn,
                storage_allowed_location,
                si_name,
                stage_name):

    sql = f"""CREATE OR REPLACE STAGE {stage_name}
              url = '{storage_allowed_location}'
              storage_integration = {si_name}"""
    logger.info(f'create_stage: storage_allowed_location={storage_allowed_location}, si_name={si_name}, stage_name={stage_name}')
    logger.info(sql)
    try:
        conn.cursor().execute(sql)
        logger.info('Create stage executed successfully')
    except Exception as e:
        logger.error(f'Error during create stage execution: {e}')

def create_file_format(conn,
                      file_format_name):

    sql = f"""CREATE OR REPLACE FILE FORMAT {file_format_name}
    TYPE = PARQUET"""
    logger.info(f'create_file_format: file_format_name={file_format_name}')
    logger.info(sql)
    try:
        conn.cursor().execute(sql)
        logger.info('Create file format executed successfully')
    except Exception as e:
        logger.error(f'Error during create file format execution: {e}')

def create_table(conn,
                 table_name,
                 file_format_name,
                 stage_name):

    sql = f"""CREATE TABLE if not exists {table_name}
          USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) WITHIN GROUP (ORDER BY ORDER_ID)
              FROM TABLE(
                INFER_SCHEMA(
                  LOCATION=>'@{stage_name}',
                  FILE_FORMAT=>'{file_format_name}')))"""
    logger.info(f'create_table: table_name={table_name}, file_format_name={file_format_name}, stage_name={stage_name}')
    logger.info(sql)
    try:
        conn.cursor().execute(sql)
        logger.info('Create table executed successfully')
    except Exception as e:
        logger.error(f'Error during create table execution: {e}')

def copy_into_table(conn,
                    table_name,
                    file_format_name,
                    stage_name):

    sql = f"""COPY INTO {table_name}
        FROM @{stage_name}
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE,
        FILE_FORMAT = (FORMAT_NAME = {file_format_name})"""
    logger.info(f'copy_into_table: table_name={table_name}, file_format_name={file_format_name}, stage_name={stage_name}')
    logger.info(sql)
    try:
        conn.cursor().execute(sql)
        logger.info('copy into table executed successfully')
    except Exception as e:
        logger.error(f'Error during copy into table execution: {e}')
