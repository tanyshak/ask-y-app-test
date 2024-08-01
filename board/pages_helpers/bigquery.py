from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
import json
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_storage_client(key_path):
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    storage_client = storage.Client(credentials=credentials, project=credentials.project_id,)
    print(get_storage_client)
    return storage_client, client

def bigquery_get_date_range(key_path ,project_id, dataset_id):
    _, client = get_storage_client(key_path)
    table_pattern = "events_*"
    query = f"""
    SELECT
        MIN(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)) AS first_date,
        MAX(PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)) AS last_date
    FROM `{project_id}.{dataset_id}.{table_pattern}`
    WHERE REGEXP_CONTAINS(_TABLE_SUFFIX, r'^\\d{{8}}$');
    """
    logger.info(f"Get date range {project_id}.{dataset_id}.")
    logger.info(f"{query}")
    try:
        query_job = client.query(query)
        results = query_job.result()

        for row in results:
            very_start_date = row.first_date.strftime('%Y%m%d')
            very_end_date = row.last_date.strftime('%Y%m%d')

            return very_start_date, very_end_date
    except Exception as e:
        logger.error(f'Error during create table execution: {e}')
        raise

def get_dataset_location(client, project_id, dataset_id):

    dataset_ref = client.dataset(dataset_id)
    dataset = client.get_dataset(dataset_ref)

    return dataset.location

def create_table_with_time_range(client, project_id, dataset_id,
                                 start_date, end_date):
    table_id = f"events_{start_date}_{end_date}"
    destination_table = f"{project_id}.{dataset_id}.{table_id}"
    source_table = f"{project_id}.{dataset_id}.events_*"

    query = f"""
    CREATE OR REPLACE TABLE `{destination_table}`
    AS
    SELECT *
    FROM `{source_table}`
    WHERE _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
    """
    logger.info(f"Create {destination_table}.")
    try:
        query_job = client.query(query)
        query_job.result()
        logger.info(f"Table {destination_table} created successfully.")
        return table_id
    except Exception as e:
        logger.error(f'Error during create table {destination_table} execution: {e}')
        raise



def create_bucket_class_location(bucket_name, location, storage_client):
    try:
        bucket = storage_client.get_bucket(bucket_name)
    except NotFound:
        new_bucket = storage_client.create_bucket(bucket_name, location=location)
        logger.info(f"Created bucket {new_bucket.name} in {new_bucket.location} with storage class {new_bucket.storage_class}")
        return new_bucket

def export_to_gcs(client,
                  bigquery,
                  bucket,
                  location,
                  file_path,
                  project_id,
                  dataset_id,
                  table_id):
    destination_uri = f"gs://{bucket}/{file_path}"
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
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
        f"Exported {project_id}:{dataset_id}.{table_id} to {destination_uri}")
    storage_allowed_location = f"gcs://{bucket}/{file_path.lstrip('/').rsplit('/', 1)[0]}/"
    return storage_allowed_location

def add_bucket_iam_member(project_id, bucket, storage_client):
    """Add a new member to an IAM Policy"""
    #replace with your role
    #TODO: add function to create role and member
    role_name = "asky_permissions"
    role = f"projects/{project_id}/roles/{role_name}"
    member = "serviceAccount:sijujecwdr@va3-22da.iam.gserviceaccount.com"

    bucket = storage_client.bucket(bucket)
    policy = bucket.get_iam_policy(requested_policy_version=3)
    policy.bindings.append({"role": role, "members": {member}})
    bucket.set_iam_policy(policy)
    print(f"Added {member} with role {role} to {bucket}.")

def download_table_schema(client, project_id, dataset_id, table_id):

    output_file = 'uploads/table_schema.json'
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    schema = client.schema_to_json(table.schema, output_file)
    print(f"Schema has been written to {output_file}")

def generate_gcloud_commands(project_id):
    commands = f"""
    gcloud config set project {project_id} && \\
    gcloud services enable iam.googleapis.com --project={project_id} && \\
    gcloud iam roles create asky_permissions \\
      --project={project_id} \\
      --title="Ask Y Permissions Role" \\
      --description="Custom role to enable read and write operations between BigQuery and Snowflake for ask-y app" \\
      --permissions="bigquery.jobs.create,bigquery.tables.export,bigquery.tables.get,storage.buckets.create,storage.buckets.get,storage.buckets.getIamPolicy,storage.buckets.setIamPolicy,storage.objects.create,storage.objects.delete,storage.objects.get,storage.objects.list" \\
      --stage="GA" && \\
    gcloud iam service-accounts create ask-y-service-account \\
        --description="This service account is used for BigQuery access for Ask-Y app" \\
        --display-name="Ask-Y App Service Account" && \\
    gcloud projects add-iam-policy-binding {project_id} \\
        --member="serviceAccount:ask-y-service-account@{project_id}.iam.gserviceaccount.com" \\
        --role="projects/{project_id}/roles/asky_permissions" && \\
    gcloud projects add-iam-policy-binding {project_id} \\
      --member="serviceAccount:ask-y-service-account@{project_id}.iam.gserviceaccount.com" \\
      --role="roles/bigquery.dataViewer" && \\
    gcloud projects add-iam-policy-binding {project_id} \\
      --member="serviceAccount:ask-y-service-account@{project_id}.iam.gserviceaccount.com" \\
      --role="roles/bigquery.dataEditor" && \\
    gcloud projects add-iam-policy-binding {project_id} \\
      --member="serviceAccount:ask-y-service-account@{project_id}.iam.gserviceaccount.com" \\
      --role="roles/bigquery.jobUser" && \\
    gcloud iam service-accounts keys create key.json \\
        --iam-account=ask-y-service-account@{project_id}.iam.gserviceaccount.com
    """
    return commands.strip()
