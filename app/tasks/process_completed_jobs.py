"""Celery task for processing completed jobs that haven't been published."""

import logging
from datetime import datetime, timedelta, timezone

from app.celery_app import celery_app
from app.db_config import get_sync_session
from app.lock import acquire_lock, release_lock, LockState
from app.media_service import MediaCleanup
from app.models.job import Job, JobStatus
from app.models.outbox import Outbox
from app.models.transcode_task import TranscodeTask
from app.models.upload_task import UploadTask, UploadStatus
from app.config import settings
from app.utils import resolve_object_key, build_object_url

logger = logging.getLogger(__name__)


@celery_app.task(
    name="app.tasks.process_completed_jobs",
    acks_late=True,
    reject_on_worker_lost=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=5,
    default_retry_delay=60,
)
def process_completed_jobs():
    """Process completed jobs that haven't been published.

    Flow per job:
        1. Acquire PROCESSING lock
        2. MediaCleanup: cleanup_temp_files(video_id)
        3. Get UploadTasks (via TranscodeTasks) where status=COMPLETED
        4. Collect segments_object_urls from upload_files
        5. Acquire COMMITTING lock
        6. Create Outbox event (payload={video_id, job_id, segments_object_urls, thumbnail_url})
        7. Set job.published = True
        8. Commit, release locks
    """
    session = get_sync_session()
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        completed_jobs = (
            session.query(Job)
            .filter(
                Job.status == JobStatus.COMPLETED.value,
                Job.published == False,  # noqa: E712
                (Job.retry_after.is_(None)) | (Job.retry_after < now),
            )
            .limit(50)
            .all()
        )

        if not completed_jobs:
            return {"processed": 0}

        processed = 0
        cleanup = MediaCleanup()

        for job in completed_jobs:
            processing_lock = None
            committing_lock = None
            try:
                # 1. PROCESSING lock
                processing_lock = acquire_lock(LockState.PROCESSING, job.id)
                if processing_lock is None:
                    logger.debug("PROCESSING lock held for %s, skipping", job.id)
                    continue

                # 2. Cleanup temp files
                cleanup.cleanup_temp_files(job.video_id)

                # 3. Get UploadTasks for this job where status=COMPLETED
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

                # 4. Collect segments_object_urls
                segments_object_urls = []
                for upload_task in completed_uploads:
                    files = upload_task.upload_files or []
                    for fp in files:
                        ok = resolve_object_key(fp, settings.vid_transcode_dir)
                        url = build_object_url(ok, settings.minio_segment_bucket)
                        segments_object_urls.append(url)

                if not segments_object_urls:
                    logger.debug("No segment URLs for job %s, skipping", job.id)
                    continue

                # 5. COMMITTING lock
                committing_lock = acquire_lock(LockState.COMMITTING, job.id)
                if committing_lock is None:
                    logger.warning("COMMITTING lock held for %s, skipping", job.id)
                    continue

                # 6. Create Outbox event
                outbox = Outbox(
                    topic="job.completed",
                    payload={
                        "video_id": job.video_id,
                        "job_id": job.id,
                        "segments_object_urls": segments_object_urls,
                        "thumbnail_url": job.vid_thumbnail_url,
                    },
                )
                session.add(outbox)

                # 7. Set published = True
                job.published = True

                # 8. Commit
                session.commit()
                processed += 1

                logger.info("Job %s published, %d segment URLs", job.id, len(segments_object_urls))

            except Exception:
                session.rollback()
                logger.exception("Error processing completed job %s", job.id)
            finally:
                if committing_lock is not None:
                    release_lock(committing_lock)
                if processing_lock is not None:
                    release_lock(processing_lock)

        return {"processed": processed}

    finally:
        session.close()
