"""Utility functions for the media processing service."""

import os

from app.config import settings


def build_object_url(object_key: str, bucket_name: str) -> str:
    """Build a full MinIO object URL from an object key and bucket name.

    Args:
        object_key: The object key within the bucket.
        bucket_name: The name of the bucket.

    Returns:
        Full URL, e.g. "http://minio:9000/bucket/object_key"
    """
    return f"http://{settings.minio_endpoint}/{bucket_name}/{object_key}"


def resolve_object_key(file_path: str, prefix: str) -> str:
    """Compute the MinIO object key by stripping the first directory prefix.

    File paths look like: /app/vid_transcoded/{video_id}/{rendition}/{segment}.mp4
    Object key: {video_id}/{rendition}/{segment}.mp4

    Args:
        file_path: The local file path to resolve.
        prefix: The directory prefix to strip (e.g. vid_transcode_dir).

    Returns:
        The computed object key.
    """
    parts = file_path.replace("\\", "/").split("/")
    prefix = prefix.strip("/").split("/")[-1]
    try:
        idx = parts.index(prefix)
        return "/".join(parts[idx + 1:])
    except ValueError:
        return "/".join(parts[-3:]) if len(parts) >= 3 else os.path.basename(file_path)
