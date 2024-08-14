import snowflake.connector
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def to_camel_case(snake_str):
    components = snake_str.replace('-', '_').split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

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
                    flatten_statements.append(f'LATERAL FLATTEN(INPUT => "{field["name"]}", OUTER => TRUE) AS {field["name"]}')
                    flatten_statements.append(f'LATERAL FLATTEN(INPUT => {field["name"]}.value:value, OUTER => TRUE) AS {field["name"]}_value')
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


#TODO properly generate based on table schema
def geneate_pivot_sql(target_table_name, source_table_name):
    sql =  f""" CREATE OR REPLACE TABLE {target_table_name} AS
                SELECT
                  event_date,
                  event_timestamp,
                  event_name,
                  event_previous_timestamp,
                  event_value_in_usd,
                  event_bundle_sequence_id,
                  event_server_timestamp_offset,
                  user_id,
                  user_pseudo_id,
                  privacy_info_analytics_storage,
                  privacy_info_ads_storage,
                  privacy_info_uses_transient_token,
                  user_first_touch_timestamp,
                  user_ltv_revenue,
                  user_ltv_currency,
                  device_category,
                  device_mobile_brand_name,
                  device_mobile_model_name,
                  device_mobile_marketing_name,
                  device_mobile_os_hardware_model,
                  device_operating_system,
                  device_operating_system_version,
                  device_vendor_id,
                  device_advertising_id,
                  device_language,
                  device_is_limited_ad_tracking,
                  device_time_zone_offset_seconds,
                  device_browser,
                  device_browser_version,
                  device_web_info,
                  geo_city,
                  geo_country,
                  geo_continent,
                  geo_region,
                  geo_sub_continent,
                  geo_metro,
                  app_info_id,
                  app_info_version,
                  app_info_install_store,
                  app_info_firebase_app_id,
                  app_info_install_source,
                  traffic_source_name,
                  traffic_source_medium,
                  traffic_source_source,
                  stream_id,
                  platform,
                  event_dimensions_hostname,
                  ecommerce_total_item_quantity,
                  ecommerce_purchase_revenue_in_usd,
                  ecommerce_purchase_revenue,
                  ecommerce_refund_value_in_usd,
                  ecommerce_refund_value,
                  ecommerce_shipping_value_in_usd,
                  ecommerce_shipping_value,
                  ecommerce_tax_value_in_usd,
                  ecommerce_tax_value,
                  ecommerce_unique_items,
                  ecommerce_transaction_id,
                  -- Include any additional columns that are not part of the pivot here

                  -- Pivoted columns using CASE within MAX
                  MAX(CASE WHEN event_params_key = 'gclid' THEN event_params_value ELSE NULL END) AS event_params_gclid,
                  MAX(CASE WHEN event_params_key = 'ga_session_id' THEN event_params_value ELSE NULL END) AS event_params_ga_session_id,
                  MAX(CASE WHEN event_params_key = 'batch_page_id' THEN event_params_value ELSE NULL END) AS event_params_batch_page_id,
                  MAX(CASE WHEN event_params_key = 'campaign' THEN event_params_value ELSE NULL END) AS event_params_campaign,
                  MAX(CASE WHEN event_params_key = 'batch_ordering_id' THEN event_params_value ELSE NULL END) AS event_params_batch_ordering_id,
                  MAX(CASE WHEN event_params_key = 'ga_session_number' THEN event_params_value ELSE NULL END) AS event_params_ga_session_number,
                  MAX(CASE WHEN event_params_key = 'session_engaged' THEN event_params_value ELSE NULL END) AS event_params_session_engaged,
                  MAX(CASE WHEN event_params_key = 'source' THEN event_params_value ELSE NULL END) AS event_params_source,
                  MAX(CASE WHEN event_params_key = 'page_referrer' THEN event_params_value ELSE NULL END) AS event_params_page_referrer,
                  MAX(CASE WHEN event_params_key = 'term' THEN event_params_value ELSE NULL END) AS event_params_term,
                  MAX(CASE WHEN event_params_key = 'engaged_session_event' THEN event_params_value ELSE NULL END) AS event_params_engaged_session_event,
                  MAX(CASE WHEN event_params_key = 'page_location' THEN event_params_value ELSE NULL END) AS event_params_page_location,
                  MAX(CASE WHEN event_params_key = 'medium' THEN event_params_value ELSE NULL END) AS event_params_medium,
                  MAX(CASE WHEN event_params_key = 'page_title' THEN event_params_value ELSE NULL END) AS event_params_page_title,
                  MAX(CASE WHEN event_params_key = 'ignore_referrer' THEN event_params_value ELSE NULL END) AS event_params_ignore_referrer,
                  MAX(CASE WHEN event_params_key = 'entrances' THEN event_params_value ELSE NULL END) AS event_params_entrances,
                  MAX(CASE WHEN event_params_key = 'engagement_time_msec' THEN event_params_value ELSE NULL END) AS event_params_engagement_time_msec,
                  MAX(CASE WHEN event_params_key = 'percent_scrolled' THEN event_params_value ELSE NULL END) AS event_params_percent_scrolled,
                  MAX(CASE WHEN event_params_key = 'event_category' THEN event_params_value ELSE NULL END) AS event_params_event_category,
                  MAX(CASE WHEN event_params_key = 'event_label' THEN event_params_value ELSE NULL END) AS event_params_event_label,
                  MAX(CASE WHEN event_params_key = 'content' THEN event_params_value ELSE NULL END) AS event_params_content,
                  MAX(CASE WHEN event_params_key = 'outbound' THEN event_params_value ELSE NULL END) AS event_params_outbound,
                  MAX(CASE WHEN event_params_key = 'link_url' THEN event_params_value ELSE NULL END) AS event_params_link_url,
                  MAX(CASE WHEN event_params_key = 'link_domain' THEN event_params_value ELSE NULL END) AS event_params_link_domain,
                  MAX(CASE WHEN event_params_key = 'link_text' THEN event_params_value ELSE NULL END) AS event_params_link_text,
                  MAX(CASE WHEN event_params_key = 'file_extension' THEN event_params_value ELSE NULL END) AS event_params_file_extension,
                  MAX(CASE WHEN event_params_key = 'file_name' THEN event_params_value ELSE NULL END) AS event_params_file_name,
                  MAX(CASE WHEN event_params_key = 'link_classes' THEN event_params_value ELSE NULL END) AS event_params_link_classes,
                  MAX(CASE WHEN event_params_key = 'currency' THEN event_params_value ELSE NULL END) AS event_params_currency,
                  MAX(CASE WHEN event_params_key = 'item_name' THEN event_params_value ELSE NULL END) AS event_params_item_name,
                  MAX(CASE WHEN event_params_key = 'value' THEN event_params_value ELSE NULL END) AS event_params_value_value,
                  MAX(CASE WHEN event_params_key = 'transaction_id' THEN event_params_value ELSE NULL END) AS event_params_transaction_id,
                  MAX(CASE WHEN event_params_key = 'tax' THEN event_params_value ELSE NULL END) AS event_params_tax,
                  MAX(CASE WHEN event_params_key = 'item_variant' THEN event_params_value ELSE NULL END) AS event_params_item_variant,
                  MAX(CASE WHEN event_params_key = 'link_id' THEN event_params_value ELSE NULL END) AS event_params_link_id,
                  MAX(CASE WHEN event_params_key = 'search_term' THEN event_params_value ELSE NULL END) AS event_params_search_term,
                  MAX(CASE WHEN event_params_key = 'unique_search_term' THEN event_params_value ELSE NULL END) AS event_params_unique_search_term,
                  MAX(CASE WHEN event_params_key = 'campaign_id' THEN event_params_value ELSE NULL END) AS event_params_campaign_id

                FROM {source_table_name}
                GROUP BY
                  event_date,
                  event_timestamp,
                  event_name,
                  event_previous_timestamp,
                  event_value_in_usd,
                  event_bundle_sequence_id,
                  event_server_timestamp_offset,
                  user_id,
                  user_pseudo_id,
                  privacy_info_analytics_storage,
                  privacy_info_ads_storage,
                  privacy_info_uses_transient_token,
                  user_first_touch_timestamp,
                  user_ltv_revenue,
                  user_ltv_currency,
                  device_category,
                  device_mobile_brand_name,
                  device_mobile_model_name,
                  device_mobile_marketing_name,
                  device_mobile_os_hardware_model,
                  device_operating_system,
                  device_operating_system_version,
                  device_vendor_id,
                  device_advertising_id,
                  device_language,
                  device_is_limited_ad_tracking,
                  device_time_zone_offset_seconds,
                  device_browser,
                  device_browser_version,
                  device_web_info,
                  geo_city,
                  geo_country,
                  geo_continent,
                  geo_region,
                  geo_sub_continent,
                  geo_metro,
                  app_info_id,
                  app_info_version,
                  app_info_install_store,
                  app_info_firebase_app_id,
                  app_info_install_source,
                  traffic_source_name,
                  traffic_source_medium,
                  traffic_source_source,
                  stream_id,
                  platform,
                  event_dimensions_hostname,
                  ecommerce_total_item_quantity,
                  ecommerce_purchase_revenue_in_usd,
                  ecommerce_purchase_revenue,
                  ecommerce_refund_value_in_usd,
                  ecommerce_refund_value,
                  ecommerce_shipping_value_in_usd,
                  ecommerce_shipping_value,
                  ecommerce_tax_value_in_usd,
                  ecommerce_tax_value,
                  ecommerce_unique_items,
                  ecommerce_transaction_id
                ORDER BY event_timestamp, event_name, user_pseudo_id;"""
    return sql
