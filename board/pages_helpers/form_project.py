from board.pages_helpers.bigquery import(
     get_storage_client,
     create_table_with_time_range,
     create_bucket_class_location,
     export_to_gcs,
     add_bucket_iam_member,
     download_table_schema,
     get_dataset_location)
from google.cloud import bigquery

def bigquery_save_to_storage(key_path, project_id, dataset_id,
                             start_date, end_date,
                             file_path,
                             bucket):
    storage_client, client = get_storage_client(key_path)
    location = get_dataset_location(client, project_id, dataset_id)
    print("form_pr @@@@@@@@@@@@@@@@")
    print(location)
    create_bucket_class_location(bucket, location, storage_client)
    table_id = create_table_with_time_range(client, project_id, dataset_id,
                                            start_date, end_date)
    storage_allowed_location  = export_to_gcs( client = client,
                                               bigquery = bigquery,
                                               bucket = bucket,
                                               location = location,
                                               file_path = file_path,
                                               project_id = project_id,
                                               dataset_id = dataset_id,
                                               table_id = table_id)

    add_bucket_iam_member(project_id, bucket, storage_client)
    download_table_schema(client, project_id, dataset_id, table_id)
    return storage_allowed_location, table_id
