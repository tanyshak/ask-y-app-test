# Creating a Service Account and Generating a Private Key JSON

## Step 1: Create a Service Account
NB!Enable the IAM API
it can do  Role Administrator (roles/iam.roleAdmin) or Organization Role Administrator (roles/iam.organizationRoleAdmin)

1. **Navigate to the Google Cloud Console**:
   - Open [Google Cloud Console](https://console.cloud.google.com/).

2. **Select Your Project**:
   - Ensure you are working in the correct project by selecting it from the project dropdown at the top of the page.

3. **Open the IAM & Admin Section**:
   - In the left sidebar, navigate to **IAM & Admin** and then click on **Service Accounts**.

4. **Create a New Service Account**:
   - Click the **+ CREATE SERVICE ACCOUNT** button at the top.
   - Fill in the service account details:
     - **Service account name**: Give your service account a name.
     - **Service account ID**: This is automatically filled based on the name.
     - **Service account description**: Optionally, provide a description.

5. **Set Permissions**:
   - Click **Create and Continue**.
   - Add the required roles to the service account. For example, if you need access to BigQuery, you might add the **BigQuery Admin** role.

6. **Finalize the Service Account Creation**:
   - Click **Continue**.
   - Optionally, grant users access to the service account. Click **Done**.

## Step 2: Create a Private Key JSON

1. **Navigate to the Service Accounts List**:
   - You should see the newly created service account in the list. Click on the service account name.

2. **Generate Key**:
   - In the service account details page, go to the **KEYS** tab.
   - Click **Add Key**, then select **Create New Key**.

3. **Select Key Type**:
   - Choose **JSON** as the key type.
   - Click **Create**.

4. **Download the Key**:
   - The private key JSON file will be automatically downloaded to your computer. Keep this file secure as it contains credentials to authenticate your service account.

## Important Notes
- **Security**: Store the JSON file securely. Avoid committing it to version control systems.
- **Permissions**: Assign only the necessary permissions to the service account to follow the principle of least privilege.

By following these steps, you should be able to create a service account and generate a private key JSON for use in your applications.



gcloud iam roles describe snowflake_reader_writer --project=clean-trees-398620


snowflake_reader_writer:

- bigquery.jobs.create
- bigquery.tables.export
- bigquery.tables.get
- storage.buckets.create
- storage.buckets.get
- storage.buckets.getIamPolicy
- storage.buckets.setIamPolicy
- storage.objects.create
- storage.objects.delete
- storage.objects.get
- storage.objects.list
bigquery.dataViewer
bigquery.dataEditor
bigquery.jobUser



clean-trees-398620.analytics_308302956.events_20240601_20240630`;]
clean-trees-398620.analytics_308302956.events_20240601_20240630`;
gcloud config set project clean-trees-398620

gcloud config set project ga4-export-for-fisheye-1
gcloud services enable storage.googleapis.com

gcloud iam roles create asky_permissions \
  --title="Ask Y Permissions Role" \
  --description="Custom role to enable read and write operations between BigQuery and Snowflake for ask-y app" \
  --permissions="bigquery.jobs.create,bigquery.tables.export,bigquery.tables.get,storage.buckets.create,storage.buckets.get,storage.buckets.getIamPolicy,storage.buckets.setIamPolicy,storage.objects.create,storage.objects.delete,storage.objects.get,storage.objects.list" \
  --stage="GA"



  gcloud projects add-iam-policy-binding ga4-export-for-fisheye-1 \
      --member="serviceAccount:ask-y-service-account@ga4-export-for-fisheye-1.iam.gserviceaccount.com" \
      --role="roles/bigquery.dataViewer"

  gcloud projects add-iam-policy-binding ga4-export-for-fisheye-1 \
      --member="serviceAccount:ask-y-service-account@ga4-export-for-fisheye-1.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

  gcloud projects add-iam-policy-binding ga4-export-for-fisheye-1 \
      --member="serviceAccount:ask-y-service-account@ga4-export-for-fisheye-1.iam.gserviceaccount.com" \
      --role="roles/bigquery.jobUser"


gcloud config set project ga4-export-for-fisheye-1

# Enable IAM API
gcloud services enable iam.googleapis.com --project=ga4-export-for-fisheye-1


# Create the service account
gcloud iam service-accounts create ask-y-service-account \
    --description="This service account is used for BigQuery access for Ask-Y app" \
    --display-name="Ask-Y App Service Account"

# Grant BigQuery Admin role to the service account
gcloud projects add-iam-policy-binding ga4-export-for-fisheye-1 \
    --member="serviceAccount:ask-y-service-account@ga4-export-for-fisheye-1.iam.gserviceaccount.com" \
    --role="projects/ga4-export-for-fisheye-1/roles/asky_permissions"


# Generate the private key JSON file
gcloud iam service-accounts keys create key.json \
    --iam-account=ask-y-service-account@ga4-export-for-fisheye-1.iam.gserviceaccount.com
