"""Utility functions for the media processing service."""

import os
import re

import ffmpeg

from app.config import settings


def get_video_duration(file_path: str) -> float:
    """Get the duration of a video file in seconds.

    Args:
        file_path: Path to the video file.

    Returns:
        Duration in seconds as a float.
    """
    probe = ffmpeg.probe(file_path)
    return float(probe["format"]["duration"])


def get_video_framerate(video_file_path: str) -> float:
    """Get the frame rate of a video file in frames per second.

    Args:
        video_file_path: Path to the video file.

    Returns:
        Frame rate in frames per second as a float.
    """
    probe = ffmpeg.probe(video_file_path)
    video_stream = next(
        s for s in probe["streams"] if s["codec_type"] == "video"
    )
    r_frame_rate: str = video_stream["r_frame_rate"]
    num, den = map(int, r_frame_rate.split("/"))
    return num / den


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

    For .m4s files, trailing _<8-hex-char uuid> is also stripped.

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
        key = "/".join(parts[idx + 1:])
    except ValueError:
        key = "/".join(parts[-3:]) if len(parts) >= 3 else os.path.basename(file_path)

    key = re.sub(r"_[0-9a-f]{8}(?=\.m4s$)", "", key)
    return key
