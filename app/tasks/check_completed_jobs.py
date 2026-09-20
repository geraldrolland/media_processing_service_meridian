"""Celery task for checking if processing jobs have completed."""

import logging
import uuid

from app.celery_app import celery_app
from app.db_config import get_sync_session
from app.lock import acquire_lock, release_lock, LockState
from app.models.job import Job, JobStatus
from app.models.outbox import Outbox
from app.models.transcode_task import TranscodeTask, TranscodeTaskStatus
from app.utils import resolve_object_key
from app.config import settings

logger = logging.getLogger(__name__)


@celery_app.task(
    name="app.tasks.check_completed_jobs",
    acks_late=True,
    reject_on_worker_lost=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=5,
    default_retry_delay=60,
)
def check_completed_jobs():
    """Check PROCESSING jobs for completion by verifying all TranscodeTasks are COMPLETED.

    Flow per job:
        1. Acquire PROCESSING lock (guards read + status check)
        2. Query all TranscodeTasks for this job
        3. If ALL transcode tasks have status = COMPLETED:
           a. Acquire COMMITTING lock (guards DB write)
           b. Set job status to COMPLETED
           c. Compute segments_obj_keys from transcode tasks' input_files
           d. Create Outbox(topic="job.completed", payload={video_id, segments_obj_keys})
           e. Commit atomically
           f. Release both locks
    """
    session = get_sync_session()
    try:
        processing_jobs = (
            session.query(Job)
            .filter(Job.status == JobStatus.PROCESSING.value)
            .limit(50)
            .all()
        )

        if not processing_jobs:
            return {"processed": 0}

        processed = 0

        for job in processing_jobs:
            processing_lock = None
            committing_lock = None
            try:
                # 1. PROCESSING lock — guards read + status check
                processing_lock = acquire_lock(LockState.PROCESSING, job.id)
                if processing_lock is None:
                    logger.debug(
                        "PROCESSING lock held for %s, skipping", job.id
                    )
                    continue

                # 2. Query all TranscodeTasks for this job
                transcode_tasks = (
                    session.query(TranscodeTask)
                    .filter(TranscodeTask.job_id == job.id)
                    .all()
                )

                if not transcode_tasks:
                    logger.debug(
                        "No transcode tasks found for job %s, skipping", job.id
                    )
                    continue

                # 3. Check if ALL transcode tasks are COMPLETED
                all_completed = all(
                    t.status == TranscodeTaskStatus.COMPLETED.value
                    for t in transcode_tasks
                )

                if not all_completed:
                    logger.debug(
                        "Job %s: not all transcode tasks completed yet", job.id
                    )
                    continue

                # 4. Acquire COMMITTING lock — guards DB write
                committing_lock = acquire_lock(LockState.COMMITTING, job.id)
                if committing_lock is None:
                    logger.warning(
                        "COMMITTING lock held for %s, skipping", job.id
                    )
                    continue

                # 5. Set job to COMPLETED
                job.status = JobStatus.COMPLETED.value

                # 7. Commit
                session.commit()
                processed += 1

                logger.info(
                    "Job %s → COMPLETED",
                    job.id
                )

            except Exception:
                session.rollback()
                logger.exception("Error checking completion for job %s", job.id)
            finally:
                if committing_lock is not None:
                    release_lock(committing_lock)
                if processing_lock is not None:
                    release_lock(processing_lock)

        return {"processed": processed}

    finally:
        session.close()
