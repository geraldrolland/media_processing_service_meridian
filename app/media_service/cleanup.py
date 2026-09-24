"""Media cleanup utilities for removing bucket objects and temp files."""

import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.minio_client import delete_object
from app.config import settings

logger = logging.getLogger(__name__)


class MediaCleanup:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        pass

    def cleanup_bucket(self, object_keys: list[str], bucket_name: str):
        """Delete multiple objects from a MinIO bucket concurrently."""
        if not object_keys:
            return
        with ThreadPoolExecutor(max_workers=len(object_keys)) as pool:
            futures = {
                pool.submit(delete_object, key, bucket_name): key
                for key in object_keys
            }
            for future in as_completed(futures):
                key = futures[future]
                future.result()
        logger.info("Cleaned up %d objects from %s", len(object_keys), bucket_name)

    def cleanup_temp_files(self, video_id: str):
        """Remove any file or directory whose name starts with *video_id*."""
        dirs = [
            settings.vid_download_dir,
            settings.vid_segment_dir,
            settings.vid_transcode_dir,
            settings.vid_thumbnail_dir,
        ]
        for base_dir in dirs:
            if not os.path.isdir(base_dir):
                continue
            for entry in os.listdir(base_dir):
                if entry.startswith(video_id):
                    path = os.path.join(base_dir, entry)
                    if os.path.isdir(path):
                        shutil.rmtree(path)
                    else:
                        os.remove(path)
                    logger.info("Removed: %s", path)
