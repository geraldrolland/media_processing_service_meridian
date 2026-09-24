"""Celery application for the media processing service.

Configured with RabbitMQ as the broker and Redis as the result backend.
"""

from celery import Celery

from app.config import settings

celery_app = Celery(
    "media_processing_worker",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    result_expires=3600,
    worker_prefetch_multiplier=4,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_acks_on_failure_or_timeout=False,
    task_routes={
        "app.tasks.process_queued_jobs": {"queue": "media_processing"},
        "app.tasks.process_transcode_tasks": {"queue": "media_processing"},
        "app.tasks.process_upload_tasks": {"queue": "media_processing"},
        "app.tasks.check_completed_jobs": {"queue": "media_processing"},
        "app.tasks.process_outbox_events": {"queue": "media_processing"},
        "app.tasks.process_failed_jobs": {"queue": "media_processing"},
        "app.tasks.process_completed_jobs": {"queue": "media_processing"},
    },
)

celery_app.conf.beat_schedule = {
    "process-queued-jobs-every-15-seconds": {
        "task": "app.tasks.process_queued_jobs",
        "schedule": 15.0,
        "options": {"queue": "media_processing"},
    },
    "process-transcode-tasks-every-10-seconds": {
        "task": "app.tasks.process_transcode_tasks",
        "schedule": 10.0,
        "options": {"queue": "media_processing"},
    },
    "process-upload-tasks-every-10-seconds": {
        "task": "app.tasks.process_upload_tasks",
        "schedule": 10.0,
        "options": {"queue": "media_processing"},
    },
    "check-completed-jobs-every-15-seconds": {
        "task": "app.tasks.check_completed_jobs",
        "schedule": 15.0,
        "options": {"queue": "media_processing"},
    },
    "process-outbox-every-10-seconds": {
        "task": "app.tasks.process_outbox_events",
        "schedule": 10.0,
        "options": {"queue": "media_processing"},
    },
    "process-failed-jobs-every-15-seconds": {
        "task": "app.tasks.process_failed_jobs",
        "schedule": 15.0,
        "options": {"queue": "media_processing"},
    },
    "process-completed-jobs-every-15-seconds": {
        "task": "app.tasks.process_completed_jobs",
        "schedule": 15.0,
        "options": {"queue": "media_processing"},
    },
}

celery_app.autodiscover_tasks(["app"])
