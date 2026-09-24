"""Centralized configuration for the media processing service.

All settings are loaded from environment variables via pydantic-settings.
Falls back to sensible defaults for local development.
"""

from typing import Any

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings sourced from environment variables.

    Attributes:
        kafka_bootstrap_servers: Comma-separated Kafka broker addresses.
        kafka_topic: Topic for video queued events.
        kafka_consumer_group_id: Consumer group ID for the Kafka consumer.
        kafka_auto_offset_reset: Where to start reading when no committed offset exists.
        database_url: SQLAlchemy async database connection string.
        redis_host: Redis host for distributed locking.
        redis_port: Redis port.
        minio_endpoint: MinIO (S3-compatible) endpoint address.
        minio_access_key: MinIO access key.
        minio_secret_key: MinIO secret key.
        minio_download_bucket: Bucket for downloading original videos.
        minio_upload_bucket: Bucket for uploading transcoded segments.
        minio_secure: Whether to use HTTPS for MinIO connections.
        celery_broker_url: RabbitMQ broker URL for Celery task dispatch.
        celery_result_backend: Redis URL for Celery result storage.
        log_level: Python logging level.
    """
    kafka_bootstrap_servers: str = "kafka:29092"
    kafka_topic: str = "video.queued"
    kafka_consumer_group_id: str = "meridian-media-processing-consumer-group"
    kafka_auto_offset_reset: str = "earliest"
    database_url: str = "postgresql+asyncpg://postgres:postgres@media-processing-db:5432/media_processing_db"
    redis_host: str = "redis"
    redis_port: int = 6379
    minio_endpoint: str = "minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_download_bucket: str = "viduploads"
    minio_segment_bucket: str = "vidsegments"
    minio_thumbnail_bucket: str = "vidthumbnails"
    minio_secure: bool = False
    celery_broker_url: str = "amqp://guest:guest@rabbitmq:5672//"
    celery_result_backend: str = "redis://redis:6379/1"
    log_level: str = "info"

    vid_download_dir: str = "/tmp/downloads"
    vid_segment_dir: str = "/tmp/segments"
    vid_thumbnail_dir: str = "/tmp/thumbnails"
    vid_transcode_dir: str = "/tmp/transcoded"
    segment_duration: int = 6
    segment_prefix: str = "seg_"

    @property
    def db_dsn(self) -> str:
        return self.database_url

    @property
    def renditions(self) -> dict[str, dict[str, Any]]:
        return {
            "360p": {"width": 640, "height": 360, "bitrate": "800k"},
            "480p": {"width": 854, "height": 480, "bitrate": "1400k"},
            "720p": {"width": 1280, "height": 720, "bitrate": "2500k"},
            "1080p": {"width": 1920, "height": 1080, "bitrate": "4500k"},
        }

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
