from board.pages_helpers.import_to_snowflake import (
    create_conn,
    create_storage_integration,
    create_db,
    use_db,
    create_stage,
    create_file_format,
    create_table,
    copy_into_table
)

def imort_data_to_snowflake(user,
                            password,
                            account,
                            storage_allowed_location,
                            table_name = 'app_test',
                            si_name = 'si_snowflake_test_app',
                            db_name = 'app_test',
                            stage_name = 'app_test',
                            file_format_name = 'app_test_format'):

    conn = create_conn(user, password, account)
    create_storage_integration(conn, storage_allowed_location, si_name)
    create_db(conn, db_name)
    use_db(conn, db_name)
    create_stage(conn, storage_allowed_location, si_name, stage_name)
    create_file_format(conn, file_format_name)
    create_table(conn, table_name, file_format_name, stage_name)
    copy_into_table(conn, table_name, file_format_name, stage_name)
    return table_name, db_name, conn
