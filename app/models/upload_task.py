"""SQLModel table and enums for upload tasks."""

import uuid
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import JSON, Column
from sqlmodel import SQLModel, Field


class UploadStatus(str, Enum):
    """Upload state of a transcoded file."""

    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


class UploadTask(SQLModel, table=True):
    """SQLModel table for upload tasks."""

    __tablename__ = "upload_tasks"

    id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        primary_key=True,
        max_length=36,
    )
    transcode_id: str = Field(max_length=36, foreign_key="transcode_tasks.id")
    upload_files: list = Field(default=[], sa_column=Column(JSON, nullable=True))
    status: str = Field(default=UploadStatus.PENDING.value, max_length=16)
    num_of_retries: int = Field(default=0)
    retry_after: datetime | None = Field(default=None, nullable=True)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )
