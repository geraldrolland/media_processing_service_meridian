"""Tests for app.tasks module -- all 7 Celery tasks."""

import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock

import pytest

sys.modules["asyncpg"] = MagicMock()
sys.modules["ffmpeg"] = MagicMock()
sys.modules["minio"] = MagicMock()
sys.modules["redis"] = MagicMock()


class _FakeNoBrokersAvailable(Exception):
    pass


_kafka_mock = MagicMock()
_kafka_errors_mock = MagicMock()
_kafka_errors_mock.NoBrokersAvailable = _FakeNoBrokersAvailable
sys.modules["kafka"] = _kafka_mock
sys.modules["kafka.errors"] = _kafka_errors_mock

from app.models.job import Job, JobStatus
from app.models.transcode_task import TranscodeTask, TranscodeTaskStatus
from app.models.upload_task import UploadTask, UploadStatus
from app.models.outbox import Outbox, OutboxStatus


def _make_job(job_id="job:1", video_id="vid1", status=JobStatus.QUEUED, published=False, retries=0):
    job = MagicMock(spec=Job)
    job.id = job_id
    job.video_id = video_id
    job.status = status.value
    job.published = published
    job.num_of_retries = retries
    job.retry_after = None
    job.object_url = "http://minio:9000/viduploads/videos/vid1/file.mp4"
    job.vid_thumbnail_url = None
    return job


def _make_transcode_task(task_id="tc1", job_id="job:1", status=TranscodeTaskStatus.QUEUED):
    t = MagicMock(spec=TranscodeTask)
    t.id = task_id
    t.job_id = job_id
    t.status = status.value
    t.input_file = "/tmp/segments/vid1/seg_001.mp4"
    t.num_of_retries = 0
    t.retry_after = None
    return t


def _make_upload_task(task_id="up1", transcode_id="tc1", status=UploadStatus.PENDING, files=None):
    u = MagicMock(spec=UploadTask)
    u.id = task_id
    u.transcode_id = transcode_id
    u.status = status.value
    u.upload_files = files or []
    u.num_of_retries = 0
    u.retry_after = None
    return u


def _make_outbox(event_id="ob1", topic="job.completed", payload=None):
    o = MagicMock(spec=Outbox)
    o.id = event_id
    o.topic = topic
    o.payload = payload or {}
    o.status = OutboxStatus.PENDING.value
    o.retry_count = 0
    o.retry_after = None
    return o


# ---------------------------------------------------------------------------
# TestProcessQueuedJobs
# ---------------------------------------------------------------------------

class TestProcessQueuedJobs:

    @patch("app.tasks.process_queued_jobs.get_sync_session")
    def test_no_queued_jobs(self, mock_get_session):
        session = mock_get_session.return_value
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = []

        from app.tasks.process_queued_jobs import process_queued_jobs
        result = process_queued_jobs()

        assert result == {"processed": 0}
        session.close.assert_called_once()

    @patch("app.tasks.process_queued_jobs.release_lock")
    @patch("app.tasks.process_queued_jobs.acquire_lock", return_value=None)
    @patch("app.tasks.process_queued_jobs.get_sync_session")
    def test_lock_acquisition_failed(self, mock_get_session, mock_acquire, mock_release):
        session = mock_get_session.return_value
        job = _make_job()
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [job]

        from app.tasks.process_queued_jobs import process_queued_jobs
        result = process_queued_jobs()

        assert result == {"processed": 0}

    @patch("app.tasks.process_queued_jobs.resolve_object_key", return_value="vid1/thumb.jpg")
    @patch("app.tasks.process_queued_jobs.build_object_url", return_value="http://minio:9000/vidthumbnails/vid1/thumb.jpg")
    @patch("app.tasks.process_queued_jobs.GenerateInit")
    @patch("app.tasks.process_queued_jobs.GenerateThumbnail")
    @patch("app.tasks.process_queued_jobs.Segmentation")
    @patch("app.tasks.process_queued_jobs.upload_object")
    @patch("app.tasks.process_queued_jobs.download_object")
    @patch("app.tasks.process_queued_jobs.release_lock")
    @patch("app.tasks.process_queued_jobs.acquire_lock")
    @patch("app.tasks.process_queued_jobs.os.makedirs")
    @patch("app.tasks.process_queued_jobs.os.path.exists", return_value=False)
    @patch("app.tasks.process_queued_jobs.get_sync_session")
    def test_successful_processing(
        self, mock_get_session, mock_exists, mock_makedirs,
        mock_acquire, mock_release, mock_download, mock_upload,
        mock_seg, mock_thumb, mock_init, mock_build_url, mock_resolve,
    ):
        session = mock_get_session.return_value
        job = _make_job()
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [job]

        mock_lock = MagicMock()
        mock_acquire.return_value = mock_lock

        mock_thumb_inst = MagicMock()
        mock_thumb_inst.generate_thumbnail.return_value = "/tmp/thumbnails/vid1/thumb.jpg"
        mock_thumb.return_value = mock_thumb_inst

        mock_init_inst = MagicMock()
        mock_init_inst.generate_init_file.return_value = [
            "/tmp/transcoded/vid1/360p/init.mp4",
            "/tmp/transcoded/vid1/480p/init.mp4",
            "/tmp/transcoded/vid1/720p/init.mp4",
            "/tmp/transcoded/vid1/1080p/init.mp4",
        ]
        mock_init.return_value = mock_init_inst

        mock_seg_inst = MagicMock()
        mock_seg_inst.generate_segments.return_value = [
            "/tmp/segments/vid1/seg_001.mp4",
            "/tmp/segments/vid1/seg_002.mp4",
        ]
        mock_seg.return_value = mock_seg_inst

        from app.tasks.process_queued_jobs import process_queued_jobs
        result = process_queued_jobs()

        assert result["processed"] == 1
        assert job.status == JobStatus.PROCESSING.value
        assert mock_acquire.call_count == 2
        assert mock_release.call_count == 2
        mock_init_inst.generate_init_file.assert_called_once()
        session.add.assert_called()
        session.commit.assert_called()

    @patch("app.tasks.process_queued_jobs.resolve_object_key", return_value="vid1/thumb.jpg")
    @patch("app.tasks.process_queued_jobs.build_object_url")
    @patch("app.tasks.process_queued_jobs.GenerateInit")
    @patch("app.tasks.process_queued_jobs.GenerateThumbnail")
    @patch("app.tasks.process_queued_jobs.Segmentation")
    @patch("app.tasks.process_queued_jobs.upload_object")
    @patch("app.tasks.process_queued_jobs.download_object", side_effect=Exception("download failed"))
    @patch("app.tasks.process_queued_jobs.release_lock")
    @patch("app.tasks.process_queued_jobs.acquire_lock")
    @patch("app.tasks.process_queued_jobs.os.makedirs")
    @patch("app.tasks.process_queued_jobs.os.path.exists", return_value=False)
    @patch("app.tasks.process_queued_jobs.get_sync_session")
    def test_retry_on_failure(
        self, mock_get_session, mock_exists, mock_makedirs,
        mock_acquire, mock_release, mock_download, mock_upload,
        mock_seg, mock_thumb, mock_init, mock_build_url, mock_resolve,
    ):
        session = mock_get_session.return_value
        job = _make_job(retries=0)
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [job]

        mock_lock = MagicMock()
        mock_acquire.return_value = mock_lock

        current_job = _make_job(retries=0)
        session.get.return_value = current_job

        from app.tasks.process_queued_jobs import process_queued_jobs
        result = process_queued_jobs()

        assert result["processed"] == 0
        assert current_job.num_of_retries == 1
        assert current_job.retry_after is not None


# ---------------------------------------------------------------------------
# TestProcessTranscodeTasks
# ---------------------------------------------------------------------------

class TestProcessTranscodeTasks:

    @patch("app.tasks.process_transcode_tasks.get_sync_session")
    def test_no_queued_tasks(self, mock_get_session):
        session = mock_get_session.return_value
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = []

        from app.tasks.process_transcode_tasks import process_transcode_tasks
        result = process_transcode_tasks()

        assert result == {"processed": 0}
        session.close.assert_called_once()

    @patch("app.tasks.process_transcode_tasks.release_lock")
    @patch("app.tasks.process_transcode_tasks.acquire_lock")
    @patch("app.tasks.process_transcode_tasks.MediaTranscoder")
    @patch("app.tasks.process_transcode_tasks.get_sync_session")
    def test_successful_transcode(
        self, mock_get_session, mock_transcoder, mock_acquire,
        mock_release,
    ):
        session = mock_get_session.return_value
        task = _make_transcode_task()
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [task]

        mock_job = MagicMock(spec=Job)
        mock_job.id = "job:1"
        mock_job.video_id = "vid1"
        session.query.return_value.filter.return_value.all.return_value = [mock_job]

        mock_lock = MagicMock()
        mock_acquire.return_value = mock_lock

        mock_transcoder_inst = MagicMock()
        mock_transcoder_inst.run_transcoder.return_value = [
            "/app/vid_transcoded/vid1/360p/video.m4s",
            "/app/vid_transcoded/vid1/480p/video.m4s",
            "/app/vid_transcoded/vid1/720p/video.m4s",
            "/app/vid_transcoded/vid1/1080p/video.m4s",
            "/app/vid_transcoded/vid1/audio/video.m4s",
        ]
        mock_transcoder.return_value = mock_transcoder_inst

        from app.tasks.process_transcode_tasks import process_transcode_tasks
        result = process_transcode_tasks()

        assert result["processed"] == 1
        assert task.status == TranscodeTaskStatus.PROCESSING.value
        mock_transcoder_inst.run_transcoder.assert_called_once()
        session.add.assert_called()
        session.commit.assert_called()

    @patch("app.tasks.process_transcode_tasks.release_lock")
    @patch("app.tasks.process_transcode_tasks.acquire_lock")
    @patch("app.tasks.process_transcode_tasks.MediaTranscoder", side_effect=Exception("transcode failed"))
    @patch("app.tasks.process_transcode_tasks.get_sync_session")
    def test_failure_marks_job_failed(
        self, mock_get_session, mock_transcoder, mock_acquire, mock_release,
    ):
        session = mock_get_session.return_value
        task = _make_transcode_task()
        task.num_of_retries = 4
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [task]

        mock_job = MagicMock(spec=Job)
        mock_job.id = "job:1"
        mock_job.video_id = "vid1"
        session.query.return_value.filter.return_value.all.return_value = [mock_job]

        mock_lock = MagicMock()
        mock_acquire.return_value = mock_lock

        current_task = _make_transcode_task()
        current_task.num_of_retries = 4
        current_job = MagicMock(spec=Job)
        current_job.id = "job:1"

        def session_get_side_effect(model, pk):
            if model == TranscodeTask:
                return current_task
            if model == Job:
                return current_job
            return None

        session.get.side_effect = session_get_side_effect

        from app.tasks.process_transcode_tasks import process_transcode_tasks
        result = process_transcode_tasks()

        assert result["processed"] == 0
        assert current_task.status == TranscodeTaskStatus.FAILED.value
        assert current_task.num_of_retries == 5
        assert current_task.retry_after is None
        assert current_job.status == JobStatus.FAILED.value


# ---------------------------------------------------------------------------
# TestProcessUploadTasks
# ---------------------------------------------------------------------------

class TestProcessUploadTasks:

    @patch("app.tasks.process_upload_tasks.get_sync_session")
    def test_no_pending_uploads(self, mock_get_session):
        session = mock_get_session.return_value
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = []

        from app.tasks.process_upload_tasks import process_upload_tasks
        result = process_upload_tasks()

        assert result == {"processed": 0}
        session.close.assert_called_once()

    @patch("app.tasks.process_upload_tasks.TranscodeTask")
    @patch("app.tasks.process_upload_tasks.resolve_object_key", return_value="vid1/720p/seg_001.mp4")
    @patch("app.tasks.process_upload_tasks.upload_object")
    @patch("app.tasks.process_upload_tasks.release_lock")
    @patch("app.tasks.process_upload_tasks.acquire_lock")
    @patch("app.tasks.process_upload_tasks.get_sync_session")
    def test_successful_upload(
        self, mock_get_session, mock_acquire, mock_release,
        mock_upload, mock_resolve, mock_tc_model,
    ):
        session = mock_get_session.return_value
        upload_task = _make_upload_task(files=["/tmp/transcoded/vid1/720p/seg_001.mp4"])
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [upload_task]

        mock_lock = MagicMock()
        mock_acquire.return_value = mock_lock

        current_transcode = MagicMock(spec=TranscodeTask)
        current_transcode.status = TranscodeTaskStatus.PROCESSING.value
        session.get.return_value = current_transcode

        from app.tasks.process_upload_tasks import process_upload_tasks
        result = process_upload_tasks()

        assert result["processed"] == 1
        assert upload_task.status == UploadStatus.COMPLETED.value
        assert current_transcode.status == TranscodeTaskStatus.COMPLETED.value
        session.commit.assert_called()

    @patch("app.tasks.process_upload_tasks.resolve_object_key", return_value="vid1/720p/seg_001.mp4")
    @patch("app.tasks.process_upload_tasks.upload_object", side_effect=Exception("upload failed"))
    @patch("app.tasks.process_upload_tasks.release_lock")
    @patch("app.tasks.process_upload_tasks.acquire_lock")
    @patch("app.tasks.process_upload_tasks.get_sync_session")
    def test_failure_cascades(
        self, mock_get_session, mock_acquire, mock_release,
        mock_upload, mock_resolve,
    ):
        session = mock_get_session.return_value
        upload_task = _make_upload_task(files=["/tmp/transcoded/vid1/720p/seg_001.mp4"])
        upload_task.num_of_retries = 4
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [upload_task]

        mock_lock = MagicMock()
        mock_acquire.return_value = mock_lock

        current_upload = _make_upload_task(files=["/tmp/transcoded/vid1/720p/seg_001.mp4"])
        current_upload.num_of_retries = 4

        current_transcode = MagicMock(spec=TranscodeTask)
        current_transcode.job_id = "job:1"
        current_transcode.status = TranscodeTaskStatus.PROCESSING.value

        current_job = MagicMock(spec=Job)
        current_job.status = JobStatus.PROCESSING.value

        session.get.side_effect = [current_upload, current_transcode, current_job]

        from app.tasks.process_upload_tasks import process_upload_tasks
        result = process_upload_tasks()

        assert result["processed"] == 0
        assert current_upload.status == UploadStatus.FAILED.value
        assert current_upload.num_of_retries == 5
        assert current_upload.retry_after is None
        assert current_transcode.status == TranscodeTaskStatus.FAILED.value
        assert current_job.status == JobStatus.FAILED.value


# ---------------------------------------------------------------------------
# TestCheckCompletedJobs
# ---------------------------------------------------------------------------

class TestCheckCompletedJobs:

    @patch("app.tasks.check_completed_jobs.get_sync_session")
    def test_no_processing_jobs(self, mock_get_session):
        session = mock_get_session.return_value
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = []

        from app.tasks.check_completed_jobs import check_completed_jobs
        result = check_completed_jobs()

        assert result == {"processed": 0}
        session.close.assert_called_once()

    @patch("app.tasks.check_completed_jobs.release_lock")
    @patch("app.tasks.check_completed_jobs.acquire_lock")
    @patch("app.tasks.check_completed_jobs.get_sync_session")
    def test_all_completed(self, mock_get_session, mock_acquire, mock_release):
        session = mock_get_session.return_value
        job = _make_job(status=JobStatus.PROCESSING)
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [job]

        mock_lock = MagicMock()
        mock_acquire.return_value = mock_lock

        tc1 = _make_transcode_task(status=TranscodeTaskStatus.COMPLETED)
        tc2 = _make_transcode_task(task_id="tc2", status=TranscodeTaskStatus.COMPLETED)
        session.query.return_value.filter.return_value.all.return_value = [tc1, tc2]

        from app.tasks.check_completed_jobs import check_completed_jobs
        result = check_completed_jobs()

        assert result["processed"] == 1
        assert job.status == JobStatus.COMPLETED.value
        session.commit.assert_called()

    @patch("app.tasks.check_completed_jobs.release_lock")
    @patch("app.tasks.check_completed_jobs.acquire_lock")
    @patch("app.tasks.check_completed_jobs.get_sync_session")
    def test_not_all_completed(self, mock_get_session, mock_acquire, mock_release):
        session = mock_get_session.return_value
        job = _make_job(status=JobStatus.PROCESSING)
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [job]

        mock_lock = MagicMock()
        mock_acquire.return_value = mock_lock

        tc1 = _make_transcode_task(status=TranscodeTaskStatus.COMPLETED)
        tc2 = _make_transcode_task(task_id="tc2", status=TranscodeTaskStatus.PROCESSING)
        session.query.return_value.filter.return_value.all.return_value = [tc1, tc2]

        from app.tasks.check_completed_jobs import check_completed_jobs
        result = check_completed_jobs()

        assert result["processed"] == 0
        assert job.status == JobStatus.PROCESSING.value


# ---------------------------------------------------------------------------
# TestProcessOutboxEvents
# ---------------------------------------------------------------------------

class TestProcessOutboxEvents:

    @patch("app.tasks.process_outbox_events.get_sync_session")
    def test_no_pending_events(self, mock_get_session):
        session = mock_get_session.return_value
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = []

        from app.tasks.process_outbox_events import process_outbox_events
        result = process_outbox_events()

        assert result == {"processed": 0}
        session.close.assert_called_once()

    @patch("app.tasks.process_outbox_events.release_lock")
    @patch("app.tasks.process_outbox_events.acquire_lock")
    @patch("app.tasks.process_outbox_events.kafka_producer")
    @patch("app.tasks.process_outbox_events.get_sync_session")
    def test_successful_publish(self, mock_get_session, mock_kafka, mock_acquire, mock_release):
        session = mock_get_session.return_value
        outbox = _make_outbox()
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [outbox]

        mock_lock = MagicMock()
        mock_acquire.return_value = mock_lock

        session.get.return_value = outbox

        from app.tasks.process_outbox_events import process_outbox_events
        result = process_outbox_events()

        assert result["processed"] == 1
        assert outbox.status == OutboxStatus.PROCESSED.value
        mock_kafka.publish.assert_called_once_with(outbox.topic, outbox.payload)
        session.commit.assert_called()

    @patch("app.tasks.process_outbox_events.release_lock")
    @patch("app.tasks.process_outbox_events.acquire_lock")
    @patch("app.tasks.process_outbox_events.kafka_producer")
    @patch("app.tasks.process_outbox_events.get_sync_session")
    def test_failure_increments_retry(self, mock_get_session, mock_kafka, mock_acquire, mock_release):
        session = mock_get_session.return_value
        outbox = _make_outbox()
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [outbox]

        mock_lock = MagicMock()
        mock_acquire.return_value = mock_lock

        session.get.return_value = outbox

        mock_kafka.publish.side_effect = Exception("kafka unavailable")

        from app.tasks.process_outbox_events import process_outbox_events
        result = process_outbox_events()

        assert result["processed"] == 0
        assert outbox.retry_count == 1
        assert outbox.retry_after is not None


# ---------------------------------------------------------------------------
# TestProcessFailedJobs
# ---------------------------------------------------------------------------

class TestProcessFailedJobs:

    @patch("app.tasks.process_failed_jobs.get_sync_session")
    def test_no_failed_jobs(self, mock_get_session):
        session = mock_get_session.return_value
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = []

        from app.tasks.process_failed_jobs import process_failed_jobs
        result = process_failed_jobs()

        assert result == {"processed": 0}
        session.close.assert_called_once()

    @patch("app.tasks.process_failed_jobs.MediaCleanup")
    @patch("app.tasks.process_failed_jobs.resolve_object_key", return_value="vid1/720p/seg_001.mp4")
    @patch("app.tasks.process_failed_jobs.release_lock")
    @patch("app.tasks.process_failed_jobs.acquire_lock")
    @patch("app.tasks.process_failed_jobs.get_sync_session")
    def test_successful_cleanup(self, mock_get_session, mock_acquire, mock_release, mock_resolve, mock_cleanup_cls):
        session = mock_get_session.return_value
        job = _make_job(status=JobStatus.FAILED, published=False)
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [job]

        mock_lock = MagicMock()
        mock_acquire.return_value = mock_lock

        mock_tc_id = MagicMock()
        mock_tc_id.id = "tc1"
        mock_query_transcode = MagicMock()
        mock_query_transcode.filter.return_value.all.return_value = [mock_tc_id]

        upload_task = _make_upload_task(files=["/tmp/transcoded/vid1/720p/seg_001.mp4"])
        mock_query_upload = MagicMock()
        mock_query_upload.filter.return_value.all.return_value = [upload_task]

        call_count = [0]

        def query_side_effect(model):
            call_count[0] += 1
            if call_count[0] == 1:
                return session.query.return_value
            if call_count[0] == 2:
                return mock_query_transcode
            return mock_query_upload

        session.query.side_effect = query_side_effect

        mock_cleanup = MagicMock()
        mock_cleanup_cls.return_value = mock_cleanup

        from app.tasks.process_failed_jobs import process_failed_jobs
        result = process_failed_jobs()

        assert result["processed"] == 1
        assert job.published is True
        mock_cleanup.cleanup_bucket.assert_called()
        mock_cleanup.cleanup_temp_files.assert_called_once_with(job.video_id)
        session.add.assert_called()
        session.commit.assert_called()

    @patch("app.tasks.process_failed_jobs.MediaCleanup")
    @patch("app.tasks.process_failed_jobs.resolve_object_key", return_value="vid1/720p/seg_001.mp4")
    @patch("app.tasks.process_failed_jobs.release_lock")
    @patch("app.tasks.process_failed_jobs.acquire_lock")
    @patch("app.tasks.process_failed_jobs.get_sync_session")
    def test_thumbnail_cleanup(self, mock_get_session, mock_acquire, mock_release, mock_resolve, mock_cleanup_cls):
        session = mock_get_session.return_value
        job = _make_job(status=JobStatus.FAILED, published=False)
        job.vid_thumbnail_url = "http://minio:9000/vidthumbnails/vid1/thumb.jpg"
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [job]

        mock_lock = MagicMock()
        mock_acquire.return_value = mock_lock

        mock_tc_id = MagicMock()
        mock_tc_id.id = "tc1"
        mock_query_transcode = MagicMock()
        mock_query_transcode.filter.return_value.all.return_value = [mock_tc_id]

        upload_task = _make_upload_task(files=["/tmp/transcoded/vid1/720p/seg_001.mp4"])
        mock_query_upload = MagicMock()
        mock_query_upload.filter.return_value.all.return_value = [upload_task]

        call_count = [0]

        def query_side_effect(model):
            call_count[0] += 1
            if call_count[0] == 1:
                return session.query.return_value
            if call_count[0] == 2:
                return mock_query_transcode
            return mock_query_upload

        session.query.side_effect = query_side_effect

        mock_cleanup = MagicMock()
        mock_cleanup_cls.return_value = mock_cleanup

        from app.tasks.process_failed_jobs import process_failed_jobs
        result = process_failed_jobs()

        assert result["processed"] == 1
        assert job.published is True
        assert mock_cleanup.cleanup_bucket.call_count == 2
        thumb_call = mock_cleanup.cleanup_bucket.call_args_list[1]
        assert "vidthumbnails" in str(thumb_call)


# ---------------------------------------------------------------------------
# TestProcessCompletedJobs
# ---------------------------------------------------------------------------

class TestProcessCompletedJobs:

    @patch("app.tasks.process_completed_jobs.get_sync_session")
    def test_no_completed_jobs(self, mock_get_session):
        session = mock_get_session.return_value
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = []

        from app.tasks.process_completed_jobs import process_completed_jobs
        result = process_completed_jobs()

        assert result == {"processed": 0}
        session.close.assert_called_once()

    @patch("app.tasks.process_completed_jobs.get_video_framerate", return_value=30.0)
    @patch("app.tasks.process_completed_jobs.get_video_duration", return_value=120.5)
    @patch("app.tasks.process_completed_jobs.build_object_url", return_value="http://minio:9000/vidsegments/vid1/720p/seg_001.mp4")
    @patch("app.tasks.process_completed_jobs.resolve_object_key", return_value="vid1/720p/seg_001.mp4")
    @patch("app.tasks.process_completed_jobs.MediaCleanup")
    @patch("app.tasks.process_completed_jobs.release_lock")
    @patch("app.tasks.process_completed_jobs.acquire_lock")
    @patch("app.tasks.process_completed_jobs.get_sync_session")
    def test_successful_publish(
        self, mock_get_session, mock_acquire, mock_release,
        mock_cleanup_cls, mock_resolve, mock_build_url, mock_get_duration,
        mock_get_framerate,
    ):
        session = mock_get_session.return_value
        job = _make_job(status=JobStatus.COMPLETED, published=False)
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [job]

        mock_lock = MagicMock()
        mock_acquire.return_value = mock_lock

        mock_cleanup = MagicMock()
        mock_cleanup_cls.return_value = mock_cleanup

        mock_tc_id = MagicMock()
        mock_tc_id.id = "tc1"
        mock_query_transcode = MagicMock()
        mock_query_transcode.filter.return_value.all.return_value = [mock_tc_id]

        upload_task = _make_upload_task(files=["/tmp/transcoded/vid1/720p/seg_001.mp4"])
        mock_query_upload = MagicMock()
        mock_query_upload.filter.return_value.all.return_value = [upload_task]

        call_count = [0]

        def query_side_effect(model):
            call_count[0] += 1
            if call_count[0] == 1:
                return session.query.return_value
            if call_count[0] == 2:
                return mock_query_transcode
            return mock_query_upload

        session.query.side_effect = query_side_effect

        from app.tasks.process_completed_jobs import process_completed_jobs
        result = process_completed_jobs()

        assert result["processed"] == 1
        assert job.published is True
        mock_cleanup.cleanup_temp_files.assert_called_once_with(job.video_id)
        session.add.assert_called()
        session.commit.assert_called()

        added_outbox = session.add.call_args_list[0][0][0]
        assert added_outbox.topic == "job.completed"
        assert "manifest_metadata" in added_outbox.payload
        assert added_outbox.payload["manifest_metadata"]["manifest_type"] == "static"
        assert added_outbox.payload["manifest_metadata"]["video_duration"] == 120.5
        assert added_outbox.payload["manifest_metadata"]["framerate"] == 30.0
        mock_get_duration.assert_called_once()
        mock_get_framerate.assert_called_once()

    @patch("app.tasks.process_completed_jobs.get_video_framerate", return_value=30.0)
    @patch("app.tasks.process_completed_jobs.get_video_duration", return_value=120.5)
    @patch("app.tasks.process_completed_jobs.build_object_url", return_value="http://minio:9000/vidsegments/vid1/720p/seg_001.mp4")
    @patch("app.tasks.process_completed_jobs.resolve_object_key", return_value="vid1/720p/seg_001.mp4")
    @patch("app.tasks.process_completed_jobs.MediaCleanup")
    @patch("app.tasks.process_completed_jobs.release_lock")
    @patch("app.tasks.process_completed_jobs.acquire_lock")
    @patch("app.tasks.process_completed_jobs.get_sync_session")
    def test_segment_urls_collected(
        self, mock_get_session, mock_acquire, mock_release,
        mock_cleanup_cls, mock_resolve, mock_build_url, mock_get_duration,
        mock_get_framerate,
    ):
        session = mock_get_session.return_value
        job = _make_job(status=JobStatus.COMPLETED, published=False)
        job.vid_thumbnail_url = "http://minio:9000/vidthumbnails/vid1/thumb.jpg"
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [job]

        mock_lock = MagicMock()
        mock_acquire.return_value = mock_lock

        mock_cleanup = MagicMock()
        mock_cleanup_cls.return_value = mock_cleanup

        mock_tc_id = MagicMock()
        mock_tc_id.id = "tc1"
        mock_query_transcode = MagicMock()
        mock_query_transcode.filter.return_value.all.return_value = [mock_tc_id]

        upload_task = _make_upload_task(files=["/tmp/transcoded/vid1/720p/seg_001.mp4"])
        mock_query_upload = MagicMock()
        mock_query_upload.filter.return_value.all.return_value = [upload_task]

        call_count = [0]

        def query_side_effect(model):
            call_count[0] += 1
            if call_count[0] == 1:
                return session.query.return_value
            if call_count[0] == 2:
                return mock_query_transcode
            return mock_query_upload

        session.query.side_effect = query_side_effect

        from app.tasks.process_completed_jobs import process_completed_jobs
        result = process_completed_jobs()

        assert result["processed"] == 1

        added_outbox = session.add.call_args_list[0][0][0]
        assert added_outbox.topic == "job.completed"
        assert added_outbox.payload["video_id"] == job.video_id
        assert added_outbox.payload["job_id"] == job.id
        assert added_outbox.payload["thumbnail_url"] == job.vid_thumbnail_url
        assert "manifest_metadata" in added_outbox.payload
        assert added_outbox.payload["manifest_metadata"]["video_duration"] == 120.5
        assert added_outbox.payload["manifest_metadata"]["framerate"] == 30.0
