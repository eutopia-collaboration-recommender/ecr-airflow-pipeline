import base64
import hashlib
import os
from os.path import expanduser

from google.cloud import storage

BUCKET_NAME = "ecr-composer-bucket-main"


def hex_to_base64(hex_str: str):
    return base64.b64encode(bytes.fromhex(hex_str)).decode("utf-8")


def get_md5_hash(file_path):
    """
    Generate MD5 hash for the given file.
    :param file_path: Path to the file
    :return: MD5 hash string
    """
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        buf = f.read()
        hasher.update(buf)
    return hex_to_base64(hex_str=hasher.hexdigest())


def blobs_md5_hash(bucket: storage.Bucket, prefix=None):
    """
    List all blobs in a GCS bucket along with their MD5 hashes.
    :param bucket: The GCS bucket.
    :param prefix: (Optional) Prefix to filter blobs by folder or filename.
    :return: A dictionary with blob names as keys and their MD5 hashes as values.
    """

    blobs_md5_dict = dict()

    # List all blobs in the bucket
    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        if (
            blob.name.endswith(".py")
            or blob.name.endswith(".sql")
            or blob.name.endswith(".yml")
        ):
            blob_file_name = blob.name.split("/")[-1]
            blobs_md5_dict[blob_file_name] = blob.md5_hash

    return blobs_md5_dict


def upload_file(
    bucket: storage.Bucket,
    source_file_name: str,
    destination_blob_name: str,
):
    """
    Uploads a file to the bucket if it has changed.
    :param bucket: GCS bucket
    :param source_file_name: Source file path
    :param destination_blob_name: Destination file path in the bucket
    :return: None
    """

    # Upload the file to the bucket
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")


def upload_folder(bucket: storage.Bucket, source_folder: str, destination_folder: str):
    """
    Uploads all files in a folder to the bucket.
    :param bucket: The GCS bucket.
    :param source_folder: Source folder path
    :param destination_folder: Destination folder path in the bucket
    :return: None
    """

    # Get MD5 hashes of all files in the destination folder
    blobs_md5_dict = blobs_md5_hash(bucket=bucket, prefix=destination_folder)

    for root, dirs, files in os.walk(source_folder):
        for file in files:
            # If the file is pycache or hidden, skip it
            if "__pycache__" in root:
                continue
            source_file_path = os.path.join(root, file)

            # Calculate the local file's MD5 hash
            file_md5_hash = get_md5_hash(source_file_path)

            if (
                file in blobs_md5_dict.keys()
                and blobs_md5_dict[file] == file_md5_hash
                and file != "__init__.py"
            ):
                continue

            # Preserve the folder structure by creating the destination path
            relative_path = os.path.relpath(source_file_path, source_folder)
            destination_blob_name = os.path.join(
                destination_folder, relative_path
            ).replace("\\", "/")

            # Upload the file to the bucket
            upload_file(
                bucket=bucket,
                source_file_name=source_file_path,
                destination_blob_name=destination_blob_name,
            )


if __name__ == "__main__":
    # Set the environment variable for Google Cloud credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "config/service_account_key.json"

    # Initialize the GCS client
    storage_client = storage.Client()

    # Get the bucket
    storage_bucket = storage_client.bucket(BUCKET_NAME)

    ############################################################################################################
    # Upload Airflow DAGs
    ############################################################################################################
    # Upload all files in the DAGs folder to the destination folder in the bucket
    upload_folder(
        bucket=storage_bucket, source_folder="dags", destination_folder="dags"
    )

    ############################################################################################################
    # Upload dbt models
    ############################################################################################################

    # Upload all files in the dbt/models folder to the destination folder in the bucket
    upload_folder(
        bucket=storage_bucket,
        source_folder="dbt/models",
        destination_folder="dags/models",
    )

    # Upload all files in the dbt/macros folder to the destination folder in the bucket
    upload_folder(
        bucket=storage_bucket,
        source_folder="dbt/macros",
        destination_folder="dags/macros",
    )

    # Upload the profiles.yml file to the bucket
    upload_file(
        bucket=storage_bucket,
        source_file_name=f"{expanduser('~')}/.dbt/profiles.yml",
        destination_blob_name="data/profiles.yml",
    )

    # Upload the dbt_project.yml file to the bucket
    upload_file(
        bucket=storage_bucket,
        source_file_name="dbt/dbt_project.yml",
        destination_blob_name="dags/dbt_project.yml",
    )

    ############################################################################################################
    # Upload PySpark jobs
    ############################################################################################################

    # Upload all files in the dbt/models folder to the destination folder in the bucket
    upload_folder(
        bucket=storage_bucket,
        source_folder="spark",
        destination_folder="spark",
    )
