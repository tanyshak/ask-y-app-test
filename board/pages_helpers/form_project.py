from board.pages_helpers.bigquery_save_to_storage import get_storage_client, create_bucket_class_location, export_to_gcs, add_bucket_iam_member
from google.cloud import bigquery

def bigquery_save_to_storage(location, key_path, project, dataset_id, table_id, file_path = '/sample_app/*.parquet', bucket= 'data_fisheye_unnest_test_app_1'):
    storage_client, client = get_storage_client(key_path)
    create_bucket_class_location(bucket, location, storage_client)
    storage_allowed_location  = export_to_gcs( client = client,
                                               bigquery = bigquery,
                                               bucket = bucket,
                                               location = location,
                                               file_path = file_path,
                                               project = project,
                                               dataset_id = dataset_id,
                                               table_id = table_id)

    add_bucket_iam_member(bucket, storage_client)
    return storage_allowed_location
