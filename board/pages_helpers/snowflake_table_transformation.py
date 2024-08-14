from board.pages_helpers.snowflake_helpers import (
    create_conn, generate_unnest_sql, geneate_pivot_sql
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

def pivot_snowflake_table(conn,
                          target_table_name,
                          source_table_name):

    sql = geneate_pivot_sql( target_table_name = target_table_name,
                             source_table_name = source_table_name)

    logger.info(f'pivot table: source_table_name={source_table_name}')
    logger.info(sql)
    try:
        conn.cursor().execute(sql)
        logger.info('pivot table executed successfully')
        return
    except Exception as e:
        logger.error(f'Error during pivot table execution: {e}')
        raise

def get_columns_list(conn, source_table_name):
    logger.info(f'show columns: source_table_name={source_table_name}')
    try:
        cursor = conn.cursor().execute(f"SHOW COLUMNS IN TABLE {source_table_name}")
        columns = cursor.fetchall()
        column_names = [column[2] for column in columns]
        return column_names
    except Exception as e:
        logger.error(f'Error show columns execution: {e}')
        raise
