"""SQLModel table and enums for processing jobs."""

import uuid
from datetime import datetime, timezone
from enum import Enum

from sqlmodel import SQLModel, Field


class JobStatus(str, Enum):
    """Processing state of a job."""

    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Job(SQLModel, table=True):
    """SQLModel table for processing jobs."""

    __tablename__ = "jobs"

    id: str = Field(primary_key=True, max_length=128)
    status: str = Field(default=JobStatus.QUEUED.value, max_length=16)
    num_of_rendition_processed: int = Field(default=0)
    num_of_retries: int = Field(default=0)
    video_id: str = Field(max_length=255)
    object_url: str = Field(max_length=1024)
    retry_after: datetime | None = Field(default=None, nullable=True)
    published: bool = Field(default=False)
    vid_thumbnail_url: str | None = Field(default=None, max_length=1024, nullable=True)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )
