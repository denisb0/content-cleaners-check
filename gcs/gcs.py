from google.cloud import storage


class GCSDownloader:
    def __init__(self, gcp_project):
        self.client = storage.Client(project=gcp_project)

    def download(self, bucket_name, blob_name):
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        return blob.download_as_string()
