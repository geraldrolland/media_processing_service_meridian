"""MinIO client for the media processing service.

Provides a singleton MinIO client, a download helper that fetches
objects from the configured download bucket, an upload helper
that sends files to the configured upload bucket, and a delete
helper that removes objects from a bucket.
"""

import logging
from urllib.parse import urlparse

from minio import Minio

from app.config import settings

logger = logging.getLogger(__name__)

client = Minio(
    endpoint=settings.minio_endpoint,
    access_key=settings.minio_access_key,
    secret_key=settings.minio_secret_key,
    secure=settings.minio_secure,
)


def download_object(object_url: str, save_path: str, bucket_name: str) -> str:
    """Download an object from MinIO to a local file.

    Parses the object_url to extract the object key, then downloads
    from the configured download bucket (viduploads).

    Args:
        object_url: Full URL, e.g. http://minio:9000/viduploads/videos/{id}/{file}
        save_path: Local path to save the downloaded file.
        bucket_name: The name of the bucket to download from.

    Returns:
        The local file path.
    """
    parsed = urlparse(object_url)
    # URL path is /{bucket}/{object_key...}
    path_parts = parsed.path.lstrip("/").split("/", 1)
    object_key = path_parts[1] if len(path_parts) > 1 else path_parts[0]

    client.fget_object(
        bucket_name=bucket_name,
        object_name=object_key,
        file_path=save_path,
    )
    logger.info(
        "Downloaded %s/%s → %s",
        bucket_name,
        object_key,
        save_path,
    )
    return save_path


def upload_object(file_path: str, object_key: str, bucket_name: str) -> str:
    """Upload a local file to the specified bucket.

    Args:
        file_path: Local path of the file to upload.
        object_key: The destination object key in the upload bucket.
        bucket_name: The name of the bucket to upload to.

    Returns:
        The object key.
    """
    client.fput_object(
        bucket_name=bucket_name,
        object_name=object_key,
        file_path=file_path,
    )
    logger.info(
        "Uploaded %s → %s/%s",
        file_path,
        bucket_name,
        object_key,
    )
    return object_key


def delete_object(object_key: str, bucket_name: str) -> None:
    """Delete an object from the specified bucket.

    Args:
        object_key: The object key to delete.
        bucket_name: The name of the bucket to delete from.
    """
    client.remove_object(
        bucket_name=bucket_name,
        object_name=object_key,
    )
    logger.info("Deleted %s/%s", bucket_name, object_key)
