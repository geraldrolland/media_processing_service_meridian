"""SQLModel table and enums for transcode tasks."""

import uuid
from datetime import datetime, timezone
from enum import Enum

from sqlmodel import SQLModel, Field


class TranscodeTaskStatus(str, Enum):
    """Processing state of a transcode task."""

    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TranscodeTask(SQLModel, table=True):
    """SQLModel table for transcode tasks."""

    __tablename__ = "transcode_tasks"

    id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        primary_key=True,
        max_length=36,
    )
    job_id: str = Field(max_length=128, foreign_key="jobs.id")
    status: str = Field(default=TranscodeTaskStatus.QUEUED.value, max_length=16)
    input_file: str = Field(max_length=1024)
    num_of_processed_uploads: int = Field(default=0)
    num_of_retries: int = Field(default=0)
    retry_after: datetime | None = Field(default=None, nullable=True)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )
