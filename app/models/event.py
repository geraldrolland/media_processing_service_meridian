"""Pydantic models for Kafka video queued events."""

from pydantic import BaseModel


class VideoQueuedEvent(BaseModel):
    """Validation model for incoming video.queued Kafka messages."""

    event_id: str
    timestamp: float
    origin_service: str
    video_id: str
    object_url: str
