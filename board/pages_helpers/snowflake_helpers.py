import snowflake.connector
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_conn(user, password, account):
    try:
        logger.info('Connection try')
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account)
        logger.info('Connection successful')
        return conn
    except snowflake.connector.errors.OperationalError as e:
        logger.error(f'OperationalError: {e}')
        raise

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
        raise

def create_db(conn, db_name):
    sql = f"""CREATE DATABASE IF NOT EXISTS {db_name}"""
    logger.info(f'create_db: db_name={db_name}')
    logger.info(sql)
    try:
        conn.cursor().execute(sql)
        logger.info('create db executed successfully')
    except Exception as e:
        logger.error(f'Error during create db execution: {e}')
        raise

def use_db(conn, db_name):
    sql = f"""USE  DATABASE  {db_name}"""
    logger.info(f'use_db: db_name={db_name}')
    logger.info(sql)
    try:
        conn.cursor().execute(sql)
        logger.info('Use db executed successfully')
    except Exception as e:
        logger.error(f'Error during use db execution: {e}')
        raise

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
        raise

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
        raise

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
        raise

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
        raise

def parse_schema(schema, target_table_name, source_table_name):
    select_statements = []
    from_statements = [source_table_name]
    flatten_statements = []
    all_columns = []

    def process_record_field(field):
        for subfield in field["fields"]:
            if subfield["type"] == "RECORD" and subfield["mode"] == "REPEATED":
                if any(subsubfield["name"] == "key" and subsubfield["type"] == "STRING" for subsubfield in subfield["fields"]) and \
                   any(subsubfield["name"] == "value" and subsubfield["type"] == "RECORD" for subsubfield in subfield["fields"]):
                    flatten_statements.append(f'LATERAL FLATTEN(INPUT => "{field["name"]}") AS {field["name"]}')
                    flatten_statements.append(f'LATERAL FLATTEN(INPUT => {field["name"]}.value:value) AS {field["name"]}_value')
                    select_statements.append(f'{field["name"]}.value:key AS {field["name"]}_key')
                    select_statements.append(f'{field["name"]}_value.value AS {field["name"]}_value')
                    all_columns.extend([f'{field["name"]}_key', f'{field["name"]}_value'])
                else:
                    flatten_statements.append(f'LATERAL FLATTEN(INPUT => "{field["name"]}", OUTER => TRUE) AS {field["name"]}')
                    for subsubfield in subfield["fields"]:
                        select_statements.append(f'{field["name"]}.value:{subsubfield["name"]} AS {field["name"]}_{subsubfield["name"]}')
                        all_columns.append(f'{field["name"]}_{subsubfield["name"]}')
            else:
                select_statements.append(f'"{field["name"]}":{subfield["name"]} AS {field["name"]}_{subfield["name"]}')
                all_columns.append(f'{field["name"]}_{subfield["name"]}')

    for field in schema:
        if field["type"] == "RECORD":
            if field["mode"] == "REPEATED":
                if any(subfield["name"] == "key" and subfield["type"] == "STRING" for subfield in field["fields"]) and \
                   any(subfield["name"] == "value" and subfield["type"] == "RECORD" for subfield in field["fields"]):
                    flatten_statements.append(f'LATERAL FLATTEN(INPUT => "{field["name"]}") AS {field["name"]}')
                    flatten_statements.append(f'LATERAL FLATTEN(INPUT => {field["name"]}.value:value) AS {field["name"]}_value')
                    select_statements.append(f'{field["name"]}.value:key AS {field["name"]}_key')
                    select_statements.append(f'{field["name"]}_value.value AS {field["name"]}_value')
                    all_columns.extend([f'{field["name"]}_key', f'{field["name"]}_value'])
                else:
                    flatten_statements.append(f'LATERAL FLATTEN(INPUT => "{field["name"]}", OUTER => TRUE) AS {field["name"]}')
                    for subfield in field["fields"]:
                        select_statements.append(f'{field["name"]}.value:{subfield["name"]} AS {field["name"]}_{subfield["name"]}')
                        all_columns.append(f'{field["name"]}_{subfield["name"]}')
            else:
                process_record_field(field)
        else:
            if ":" in field["name"]:
                field_name_parts = field["name"].split(":")
                select_statements.append(f'"{field_name_parts[0]}":{field_name_parts[1]} AS {field_name_parts[0]}_{field_name_parts[1]}')
                all_columns.append(f'{field_name_parts[0]}_{field_name_parts[1]}')
            else:
                select_statements.append(f'"{field["name"]}" AS {field["name"]}')
                all_columns.append(f'{field["name"]}')

    return select_statements, from_statements, flatten_statements, all_columns

def generate_unnest_sql(schema, target_table_name, source_table_name):
    select_statements, from_statements, flatten_statements, all_columns = parse_schema(schema, target_table_name, source_table_name)

    sql_query = (
        "CREATE OR REPLACE TABLE {} AS\n"
        "  SELECT\n"
        "    {}\n"
        "  FROM\n"
        "    {},\n"
    ).format(
        target_table_name,
        ",\n    ".join(select_statements),
        ",\n    ".join(from_statements)
    )

    if flatten_statements:
        sql_query += "\n    " + ",\n    ".join(flatten_statements)

    sql_query += ";"
    return sql_query, all_columns
