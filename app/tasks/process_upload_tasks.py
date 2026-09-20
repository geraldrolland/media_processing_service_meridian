"""Celery task for processing upload tasks."""

import logging
import os
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from app.celery_app import celery_app
from app.db_config import get_sync_session
from app.lock import acquire_lock, release_lock, LockState
from app.minio_client import upload_object
from app.utils import resolve_object_key
from app.models.job import Job, JobStatus
from app.models.outbox import Outbox
from app.models.transcode_task import TranscodeTask, TranscodeTaskStatus
from app.models.upload_task import UploadTask, UploadStatus
from app.config import settings

logger = logging.getLogger(__name__)


@celery_app.task(
    name="app.tasks.process_upload_tasks",
    acks_late=True,
    reject_on_worker_lost=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=5,
    default_retry_delay=60,
)
def process_upload_tasks():
    """Process PENDING upload tasks by uploading files to MinIO vidsegments bucket.

    Flow per task:
        1. Acquire PROCESSING lock (guards upload)
        2. Upload all files concurrently via ThreadPoolExecutor
        3. Acquire COMMITTING lock (guards DB write)
        4. Set upload_task status to COMPLETED
        5. Set corresponding TranscodeTask status to COMPLETED
        6. Commit atomically
        7. Release both locks

    On failure:
        - Re-fetch upload_task from DB
        - If num_of_retries + 1 >= 5 → mark upload_task, TranscodeTask, Job FAILED,
          create Outbox(topic="job.failed")
        - Else → increment num_of_retries, set retry_after = now + 2 minutes
    """
    session = get_sync_session()
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        pending_uploads = (
            session.query(UploadTask)
            .filter(
                UploadTask.status == UploadStatus.PENDING.value,
                (UploadTask.retry_after.is_(None))
                | (UploadTask.retry_after < now),
            )
            .limit(50)
            .all()
        )

        if not pending_uploads:
            return {"processed": 0}

        processed = 0

        for upload_task in pending_uploads:
            processing_lock = None
            committing_lock = None
            try:
                # 1. PROCESSING lock — guards upload
                processing_lock = acquire_lock(LockState.PROCESSING, upload_task.id)
                if processing_lock is None:
                    logger.debug(
                        "PROCESSING lock held for %s, skipping", upload_task.id
                    )
                    continue

                # 2. Upload files concurrently via ThreadPoolExecutor
                files_to_upload = upload_task.upload_files or []
                object_keys = [resolve_object_key(fp, settings.vid_transcode_dir) for fp in files_to_upload]

                with ThreadPoolExecutor(max_workers=min(len(files_to_upload), 3)) as pool:
                    futures = {
                        pool.submit(upload_object, fp, ok): (fp, ok)
                        for fp, ok in zip(files_to_upload, object_keys)
                    }
                    for future in as_completed(futures):
                        fp, ok = futures[future]
                        future.result()  # raise if upload failed

                logger.info(
                    "Uploaded %d files for upload_task %s",
                    len(files_to_upload),
                    upload_task.id,
                )

                # 3. COMMITTING lock — guards DB write
                committing_lock = acquire_lock(LockState.COMMITTING, upload_task.id)
                if committing_lock is None:
                    logger.warning(
                        "COMMITTING lock held for %s, skipping", upload_task.id
                    )
                    continue

                # 4. Set upload_task to COMPLETED
                upload_task.status = UploadStatus.COMPLETED.value

                # 5. Set corresponding TranscodeTask to COMPLETED
                current_transcode = session.get(TranscodeTask, upload_task.transcode_id)
                if current_transcode is not None:
                    current_transcode.status = TranscodeTaskStatus.COMPLETED.value

                # 6. Commit
                session.commit()
                processed += 1

                logger.info(
                    "Upload task %s → COMPLETED", upload_task.id
                )

            except Exception:
                session.rollback()
                logger.exception("Error uploading task %s", upload_task.id)
                try:
                    current_upload = session.get(UploadTask, upload_task.id)
                    if current_upload is not None:
                        if current_upload.num_of_retries + 1 >= 5:
                            current_upload.status = UploadStatus.FAILED.value
                            current_upload.num_of_retries = 5
                            current_upload.retry_after = None

                            current_transcode = session.get(
                                TranscodeTask, current_upload.transcode_id
                            )
                            if current_transcode is not None:
                                current_transcode.status = (
                                    TranscodeTaskStatus.FAILED.value
                                )
                                current_job = session.get(
                                    Job, current_transcode.job_id
                                )
                                if current_job is not None:
                                    current_job.status = JobStatus.FAILED.value
                        else:
                            current_upload.num_of_retries += 1
                            current_upload.retry_after = (
                                datetime.now(timezone.utc).replace(tzinfo=None)
                                + timedelta(minutes=2)
                            )
                        session.commit()
                except Exception:
                    session.rollback()
                    logger.exception(
                        "Error updating retry state for upload task %s",
                        upload_task.id,
                    )
            finally:
                if committing_lock is not None:
                    release_lock(committing_lock)
                if processing_lock is not None:
                    release_lock(processing_lock)

        return {"processed": processed}

    finally:
        session.close()
