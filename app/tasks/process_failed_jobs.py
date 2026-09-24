"""Celery task for processing failed jobs that haven't been published."""

import logging
from urllib.parse import urlparse

from app.celery_app import celery_app
from app.db_config import get_sync_session
from app.lock import acquire_lock, release_lock, LockState
from app.media_service import MediaCleanup
from app.models.job import Job, JobStatus
from app.models.outbox import Outbox
from app.models.transcode_task import TranscodeTask
from app.models.upload_task import UploadTask, UploadStatus
from app.config import settings
from app.utils import resolve_object_key

logger = logging.getLogger(__name__)


@celery_app.task(
    name="app.tasks.process_failed_jobs",
    acks_late=True,
    reject_on_worker_lost=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=5,
    default_retry_delay=60,
)
def process_failed_jobs():
    """Process failed jobs that haven't been published.

    Flow per job:
        1. Acquire PROCESSING lock
        2. Query UploadTasks (via TranscodeTasks) where status=COMPLETED
        3. Collect all object_keys from upload_files using resolve_object_key
        4. If job.vid_thumbnail_url exists, extract thumb object key
        5. MediaCleanup: cleanup_bucket (segments + thumbnail), cleanup_temp_files
        6. Acquire COMMITTING lock
        7. Create Outbox event (payload={video_id, job_id})
        8. Set job.published = True
        9. Commit, release locks
    """
    session = get_sync_session()
    try:
        failed_jobs = (
            session.query(Job)
            .filter(
                Job.status == JobStatus.FAILED.value,
                Job.published == False,  # noqa: E712
            )
            .limit(50)
            .all()
        )

        if not failed_jobs:
            return {"processed": 0}

        processed = 0
        cleanup = MediaCleanup()

        for job in failed_jobs:
            processing_lock = None
            committing_lock = None
            try:
                # 1. PROCESSING lock
                processing_lock = acquire_lock(LockState.PROCESSING, job.id)
                if processing_lock is None:
                    logger.debug("PROCESSING lock held for %s, skipping", job.id)
                    continue

                # 2. Get UploadTasks for this job where status=COMPLETED
                transcode_ids = [
                    t.id for t in
                    session.query(TranscodeTask.id)
                    .filter(TranscodeTask.job_id == job.id)
                    .all()
                ]
                if not transcode_ids:
                    logger.debug("No transcode tasks for job %s, skipping", job.id)
                    continue

                completed_uploads = (
                    session.query(UploadTask)
                    .filter(
                        UploadTask.transcode_id.in_(transcode_ids),
                        UploadTask.status == UploadStatus.COMPLETED.value,
                    )
                    .all()
                )

                # 3. Collect object_keys from all upload_files
                object_keys = []
                for upload_task in completed_uploads:
                    files = upload_task.upload_files or []
                    for fp in files:
                        ok = resolve_object_key(fp, settings.vid_transcode_dir)
                        object_keys.append(ok)

                if not object_keys:
                    logger.debug("No object keys for job %s, skipping", job.id)
                    continue

                # 4. Extract thumbnail object key from vid_thumbnail_url
                thumb_object_keys = []
                if job.vid_thumbnail_url:
                    parsed = urlparse(job.vid_thumbnail_url)
                    # path: /vidthumbnails/{video_id}/{filename}
                    path_parts = parsed.path.lstrip("/").split("/", 1)
                    if len(path_parts) > 1:
                        thumb_object_keys.append(path_parts[1])

                # 5. Cleanup
                cleanup.cleanup_bucket(object_keys, settings.minio_segment_bucket)
                if thumb_object_keys:
                    cleanup.cleanup_bucket(thumb_object_keys, settings.minio_thumbnail_bucket)
                cleanup.cleanup_temp_files(job.video_id)

                # 6. COMMITTING lock
                committing_lock = acquire_lock(LockState.COMMITTING, job.id)
                if committing_lock is None:
                    logger.warning("COMMITTING lock held for %s, skipping", job.id)
                    continue

                # 7. Create Outbox event
                outbox = Outbox(
                    topic="job.failed",
                    payload={"video_id": job.video_id, "job_id": job.id},
                )
                session.add(outbox)

                # 8. Set published = True
                job.published = True

                # 9. Commit
                session.commit()
                processed += 1

                logger.info("Job %s cleanup completed, published=True", job.id)

            except Exception:
                session.rollback()
                logger.exception("Error cleaning up job %s", job.id)
            finally:
                if committing_lock is not None:
                    release_lock(committing_lock)
                if processing_lock is not None:
                    release_lock(processing_lock)

        return {"processed": processed}

    finally:
        session.close()
