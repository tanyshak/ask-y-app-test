from board.pages_helpers.snowflake_helpers import (
    create_conn, generate_unnest_sql
)
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def unnest_snowflake_table(conn,
                           target_table_name,
                           source_table_name):

    with open('uploads/table_schema.json', 'r') as file:
        schema = json.load(file)
    sql, all_columns = generate_unnest_sql(schema = schema,
                             target_table_name = target_table_name,
                             source_table_name = source_table_name)

    logger.info(f'unnest table: source_table_name={source_table_name}')
    logger.info(sql)
    try:
        conn.cursor().execute(sql)
        logger.info('unnest table executed successfully')
        return target_table_name, all_columns
    except Exception as e:
        logger.error(f'Error during unnest table execution: {e}')
        raise
