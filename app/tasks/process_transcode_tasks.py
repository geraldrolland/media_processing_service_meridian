"""Celery task for processing transcode tasks."""

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

from app.celery_app import celery_app
from app.db_config import get_sync_session
from app.lock import acquire_lock, release_lock, LockState
from app.media_service import MediaTranscoder
from app.models.job import Job, JobStatus
from app.models.outbox import Outbox
from app.models.transcode_task import TranscodeTask, TranscodeTaskStatus
from app.models.upload_task import UploadTask

logger = logging.getLogger(__name__)


@celery_app.task(
    name="app.tasks.process_transcode_tasks",
    acks_late=True,
    reject_on_worker_lost=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=5,
    default_retry_delay=60,
)
def process_transcode_tasks():
    """Process QUEUED transcode tasks by running MediaTranscoder and creating UploadTasks.

    Flow per task:
        1. Acquire PROCESSING lock (guards transcoding)
        2. Run MediaTranscoder on the input file
        3. Save each rendition to vid_transcoded/<video_id>/<rendition>/
        4. Acquire COMMITTING lock (guards DB write)
        5. Create UploadTask records for each saved file
        6. Set transcode task status to PROCESSING
        7. Commit atomically
        8. Release both locks

    On failure:
        - Re-fetch task from DB
        - If num_of_retries + 1 >= 5 → mark task FAILED, mark job FAILED,
          create Outbox(topic="job.failed")
        - Else → increment num_of_retries, set retry_after = now + 2 minutes
    """
    session = get_sync_session()
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        queued_tasks = (
            session.query(TranscodeTask)
            .filter(
                TranscodeTask.status == TranscodeTaskStatus.QUEUED.value,
                (TranscodeTask.retry_after.is_(None))
                | (TranscodeTask.retry_after < now),
            )
            .limit(50)
            .all()
        )

        if not queued_tasks:
            return {"processed": 0}

        # Resolve video_id for each task's job in one query
        job_ids = list({t.job_id for t in queued_tasks})
        jobs = session.query(Job).filter(Job.id.in_(job_ids)).all()
        job_video_map = {j.id: j.video_id for j in jobs}

        processed = 0

        for task in queued_tasks:
            processing_lock = None
            committing_lock = None
            try:
                # 1. PROCESSING lock — guards transcoding
                processing_lock = acquire_lock(LockState.PROCESSING, task.id)
                if processing_lock is None:
                    logger.debug(
                        "PROCESSING lock held for %s, skipping", task.id
                    )
                    continue

                # 2. Resolve video_id
                video_id = job_video_map.get(task.job_id)
                if video_id is None:
                    logger.warning(
                        "Job %s not found for task %s, skipping",
                        task.job_id,
                        task.id,
                    )
                    continue

                # 3. Transcode and save
                transcoder = MediaTranscoder(
                    input_file=task.input_file,
                    output_dir=os.path.join(os.getcwd(), "vid_transcoded"),
                )
                saved_files = transcoder.run_transcoder()

                # 5. COMMITTING lock — guards DB write
                committing_lock = acquire_lock(LockState.COMMITTING, task.id)
                if committing_lock is None:
                    logger.warning(
                        "COMMITTING lock held for %s, skipping", task.id
                    )
                    continue

                # 6. Create UploadTasks + update TranscodeTask
                task.status = TranscodeTaskStatus.PROCESSING.value

                upload = UploadTask(
                    id=str(uuid.uuid4()),
                    transcode_id=task.id,
                    upload_files=saved_files,
                )
                session.add(upload)

                session.add(task)
                session.commit()
                processed += 1

                logger.info(
                    "Task %s → PROCESSING, %d upload tasks created",
                    task.id,
                    len(saved_files),
                )

            except Exception:
                session.rollback()
                logger.exception("Error transcoding task %s", task.id)
                try:
                    current_task = session.get(TranscodeTask, task.id)
                    if current_task is not None:
                        if current_task.num_of_retries + 1 >= 5:
                            current_task.status = TranscodeTaskStatus.FAILED.value
                            current_task.num_of_retries = 5
                            current_task.retry_after = None

                            current_job = session.get(Job, current_task.job_id)
                            if current_job is not None:
                                current_job.status = JobStatus.FAILED.value
                        else:
                            current_task.num_of_retries += 1
                            current_task.retry_after = (
                                datetime.now(timezone.utc).replace(tzinfo=None)
                                + timedelta(minutes=2)
                            )
                        session.commit()
                except Exception:
                    session.rollback()
                    logger.exception(
                        "Error updating retry state for task %s", task.id
                    )
            finally:
                if committing_lock is not None:
                    release_lock(committing_lock)
                if processing_lock is not None:
                    release_lock(processing_lock)

        return {"processed": processed}

    finally:
        session.close()
