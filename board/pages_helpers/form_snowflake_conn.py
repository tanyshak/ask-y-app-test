from board.pages_helpers.snowflake_helpers import (
    create_storage_integration,
    create_db,
    use_db,
    create_stage,
    create_file_format,
    create_table,
    copy_into_table,
    to_camel_case
)

def imort_data_to_snowflake(conn,
                            storage_allowed_location,
                            table_name,
                            si_name,
                            db_name,
                            stage_name,
                            file_format_name):

    db_name = to_camel_case(db_name)
    create_storage_integration(conn, storage_allowed_location, si_name)
    create_db(conn, db_name)
    use_db(conn, db_name)
    create_stage(conn, storage_allowed_location, si_name, stage_name)
    create_file_format(conn, file_format_name)
    create_table(conn, table_name, file_format_name, stage_name)
    copy_into_table(conn, table_name, file_format_name, stage_name)
    return table_name, db_name
