"""Smoke tests for the media processing service."""

import sys
from unittest.mock import MagicMock

sys.modules["asyncpg"] = MagicMock()
sys.modules["aiokafka"] = MagicMock()
sys.modules["ffmpeg"] = MagicMock()
sys.modules["redis"] = MagicMock()
sys.modules["minio"] = MagicMock()

_kafka_mock = MagicMock()
_kafka_errors_mock = MagicMock()


class _FakeNoBrokersAvailable(Exception):
    pass


_kafka_errors_mock.NoBrokersAvailable = _FakeNoBrokersAvailable
sys.modules["kafka"] = _kafka_mock
sys.modules["kafka.errors"] = _kafka_errors_mock

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402
from app.celery_app import celery_app  # noqa: E402


class TestFastAPIApp:
    def test_app_title(self):
        assert app.title == "Media Processing Service"

    def test_app_version(self):
        assert app.version == "1.0.0"


class TestHealthEndpoint:
    def test_health_returns_200(self):
        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_health_body(self):
        client = TestClient(app)
        resp = client.get("/health")
        data = resp.json()
        assert data["status"] == "ok"


class TestCeleryConfig:
    def test_celery_has_all_beat_schedules(self):
        schedules = celery_app.conf.beat_schedule
        expected = [
            "process-queued-jobs-every-15-seconds",
            "process-transcode-tasks-every-10-seconds",
            "process-upload-tasks-every-10-seconds",
            "check-completed-jobs-every-15-seconds",
            "process-outbox-every-10-seconds",
            "process-failed-jobs-every-15-seconds",
            "process-completed-jobs-every-15-seconds",
        ]
        for name in expected:
            assert name in schedules, f"Missing beat schedule: {name}"

    def test_celery_prefetch_count(self):
        assert celery_app.conf.worker_prefetch_multiplier == 4

    def test_celery_acks_late(self):
        assert celery_app.conf.task_acks_late is True


class TestImports:
    def test_all_tasks_importable(self):
        from app.tasks.process_queued_jobs import process_queued_jobs
        from app.tasks.process_transcode_tasks import process_transcode_tasks
        from app.tasks.process_upload_tasks import process_upload_tasks
        from app.tasks.check_completed_jobs import check_completed_jobs
        from app.tasks.process_outbox_events import process_outbox_events
        from app.tasks.process_failed_jobs import process_failed_jobs
        from app.tasks.process_completed_jobs import process_completed_jobs

        assert callable(process_queued_jobs)
        assert callable(process_transcode_tasks)
        assert callable(process_upload_tasks)
        assert callable(check_completed_jobs)
        assert callable(process_outbox_events)
        assert callable(process_failed_jobs)
        assert callable(process_completed_jobs)

    def test_models_importable(self):
        from app.models.job import Job, JobStatus
        from app.models.transcode_task import TranscodeTask, TranscodeTaskStatus
        from app.models.upload_task import UploadTask, UploadStatus
        from app.models.outbox import Outbox, OutboxStatus

        assert Job is not None
        assert TranscodeTask is not None
        assert UploadTask is not None
        assert Outbox is not None

    def test_utils_importable(self):
        from app.utils import build_object_url, resolve_object_key, get_video_duration, get_video_framerate

        assert callable(build_object_url)
        assert callable(resolve_object_key)
        assert callable(get_video_duration)
        assert callable(get_video_framerate)
