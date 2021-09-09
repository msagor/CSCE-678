from cloud import AWS_S3, Azure_Blob_Storage, Google_Cloud_Storage
import boto3
from azure.storage import blob
from google.cloud import storage

print "Cleaning up AWS S3..."

aws_s3 = AWS_S3()
session = boto3.Session(
          aws_access_key_id = aws_s3.access_key_id,
          aws_secret_access_key = aws_s3.access_secret_key)
s3 = session.resource('s3')
bucket = s3.Bucket(aws_s3.bucket_name)

for obj in bucket.objects.all():
    print "    Deleting", obj.key, "from AWS bucket", aws_s3.bucket_name
    s3.Object(aws_s3.bucket_name, obj.key).delete()

print "Cleaning up Azure Blob Storage..."

azure_blob_storage = Azure_Blob_Storage()
service_client = blob.BlobServiceClient.from_connection_string(azure_blob_storage.conn_str)
container = service_client.get_container_client(container=azure_blob_storage.container_name)

for blob in container.list_blobs():
    print "    Deleting", blob.name, "from AZURE container", azure_blob_storage.container_name
    service_client.get_blob_client(container=azure_blob_storage.container_name, blob=blob.name).delete_blob()

print "Cleaning up Google Cloud Storage..."

google_cloud_storage = Google_Cloud_Storage()
storage_client = storage.Client.from_service_account_json(google_cloud_storage.credential_file)
bucket = storage_client.get_bucket(google_cloud_storage.bucket_name)

for blob in bucket.list_blobs():
    print "    Deleting", blob.name, "from GCP bucket", google_cloud_storage.bucket_name
    bucket.blob(blob.name).delete()
