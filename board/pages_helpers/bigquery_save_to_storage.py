from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage

def get_storage_client(key_path):
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    storage_client = storage.Client(credentials=credentials, project=credentials.project_id,)
    print(get_storage_client)
    return storage_client, client

def create_bucket_class_location(bucket_name, location, storage_client):
    try:
        bucket = storage_client.get_bucket(bucket_name)
        print(f"Bucket {bucket_name} already exists.")
    except NotFound:
        bucket = storage_client.bucket(bucket_name)
        new_bucket = storage_client.create_bucket(bucket, location=location)
        print(
            "Created bucket {} in {} with storage class {}".format(
                new_bucket.name, new_bucket.location, new_bucket.storage_class
            )
        )
        return new_bucket

def export_to_gcs(client,
                  bigquery,
                  bucket,
                  location,
                  file_path,
                  project,
                  dataset_id,
                  table_id):
    destination_uri = f"gs://{bucket}/{file_path}"
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = (bigquery.DestinationFormat.PARQUET)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location=location,

    )
    extract_job.result()

    print(
        f"Exported {project}:{dataset_id}.{table_id} to {destination_uri}")
    storage_allowed_location = f"gcs://{bucket}/{file_path.lstrip('/').rsplit('/', 1)[0]}/"
    return storage_allowed_location

def add_bucket_iam_member(bucket, storage_client):
    """Add a new member to an IAM Policy"""
    #replace with your role
    #TODO: add function to create role and member
    role = "projects/ga4-export-for-fisheye-1/roles/CustomRole726"
    member = "serviceAccount:sijujecwdr@va3-22da.iam.gserviceaccount.com"

    bucket = storage_client.bucket(bucket)
    policy = bucket.get_iam_policy(requested_policy_version=3)
    policy.bindings.append({"role": role, "members": {member}})
    bucket.set_iam_policy(policy)
    print(f"Added {member} with role {role} to {bucket}.")
