"""Celery tasks package for the media processing service."""

from app.tasks.process_queued_jobs import process_queued_jobs  # noqa: F401
from app.tasks.process_transcode_tasks import process_transcode_tasks  # noqa: F401
from app.tasks.process_upload_tasks import process_upload_tasks  # noqa: F401
from app.tasks.check_completed_jobs import check_completed_jobs  # noqa: F401
from app.tasks.process_outbox_events import process_outbox_events  # noqa: F401
from app.tasks.process_completed_jobs import process_completed_jobs  # noqa: F401
from app.tasks.process_failed_jobs import process_failed_jobs  # noqa: F401
