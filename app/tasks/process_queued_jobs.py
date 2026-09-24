"""Celery task for processing queued jobs."""

import logging
import os
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from app.celery_app import celery_app
from app.db_config import get_sync_session
from app.lock import acquire_lock, release_lock, LockState
from app.minio_client import download_object, upload_object
from app.utils import build_object_url, resolve_object_key
from app.media_service import GenerateInit, GenerateThumbnail, Segmentation
from app.models.job import Job, JobStatus
from app.models.outbox import Outbox
from app.models.transcode_task import TranscodeTask
from app.config import settings

logger = logging.getLogger(__name__)


@celery_app.task(
    name="app.tasks.process_queued_jobs",
    acks_late=True,
    reject_on_worker_lost=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=5,
    default_retry_delay=60,
)
def process_queued_jobs():
    """Process QUEUED jobs by downloading video, segmenting, and creating TranscodeTasks.

    Flow per job:
        1. Acquire PROCESSING lock (guards download + segment)
        2. Download video from viduploads bucket (skip if already cached)
        3. Segment video into 6-second chunks
        4. Acquire COMMITTING lock (guards DB write)
        5. Create TranscodeTask records + set job status to PROCESSING
        6. Commit atomically
        7. Release both locks

    On failure:
        - Re-fetch job from DB
        - If num_of_retries + 1 >= 5 → mark FAILED, create Outbox(topic="job.failed")
        - Else → set retry_after = now + 2 minutes
    """
    session = get_sync_session()
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        queued_jobs = (
            session.query(Job)
            .filter(
                Job.status == JobStatus.QUEUED.value,
                (Job.retry_after.is_(None)) | (Job.retry_after < now),
            )
            .limit(10)
            .all()
        )

        if not queued_jobs:
            return {"processed": 0}

        processed = 0

        for job in queued_jobs:
            processing_lock = None
            committing_lock = None
            try:
                # 1. PROCESSING lock — guards download + segment
                processing_lock = acquire_lock(LockState.PROCESSING, job.id)
                if processing_lock is None:
                    logger.debug(
                        "PROCESSING lock held for %s, skipping", job.id
                    )
                    continue

                # 2. Download from viduploads (if not cached locally)
                download_dir = settings.vid_download_dir
                os.makedirs(download_dir, exist_ok=True)

                parsed_url = urlparse(job.object_url)
                original_filename = os.path.basename(parsed_url.path)
                _, ext = os.path.splitext(original_filename)
                save_path = os.path.join(download_dir, f"{job.video_id}{ext}")

                if not os.path.exists(save_path):
                    download_object(job.object_url, save_path, settings.minio_download_bucket)
                else:
                    logger.info(
                        "Video %s already downloaded at %s",
                        job.video_id,
                        save_path,
                    )

                # 3. Generate thumbnail
                thumb_output_dir = os.path.join(settings.vid_thumbnail_dir, job.video_id)
                os.makedirs(thumb_output_dir, exist_ok=True)

                thumb_prefix = f"{uuid.uuid4().hex[:8]}_thumbnail"
                generator = GenerateThumbnail(
                    video_path=save_path,
                    output_dir=thumb_output_dir,
                    thumbnail_prefix=thumb_prefix,
                )
                thumbnail_path = generator.generate_thumbnail()

                # 4. Upload thumbnail to MinIO
                thumb_object_key = resolve_object_key(thumbnail_path, settings.vid_thumbnail_dir)
                upload_object(thumbnail_path, thumb_object_key, settings.minio_thumbnail_bucket)

                job.vid_thumbnail_url = build_object_url(thumb_object_key, settings.minio_thumbnail_bucket)

                logger.info(
                    "Generated thumbnail for job %s → %s",
                    job.id,
                    job.vid_thumbnail_url,
                )

                # 5. Generate init segments
                init_output_dir = os.path.join(settings.vid_transcode_dir, job.video_id)
                os.makedirs(init_output_dir, exist_ok=True)

                init_generator = GenerateInit(
                    input_file=save_path,
                    output_dir=init_output_dir,
                    representation=list(settings.renditions.keys()),
                )
                init_paths = init_generator.generate_init_file()

                # 6. Upload init files to MinIO via thread pool
                init_object_keys = [
                    resolve_object_key(fp, settings.vid_transcode_dir)
                    for fp in init_paths
                ]

                with ThreadPoolExecutor(max_workers=len(init_paths)) as pool:
                    futures = {
                        pool.submit(
                            upload_object, fp, ok, settings.minio_segment_bucket
                        ): (fp, ok)
                        for fp, ok in zip(init_paths, init_object_keys)
                    }
                    for future in as_completed(futures):
                        fp, ok = futures[future]
                        future.result()

                logger.info(
                    "Uploaded %d init files for job %s",
                    len(init_paths),
                    job.id,
                )

                # 7. Segment the video
                seg_output_dir = os.path.join(
                    settings.vid_segment_dir, job.video_id
                )
                os.makedirs(seg_output_dir, exist_ok=True)

                segmenter = Segmentation(
                    video_path=save_path,
                    output_dir=seg_output_dir,
                    seg_prefix=settings.segment_prefix,
                    seg_duration=settings.segment_duration,
                )
                segment_paths = segmenter.generate_segments()

                # 8. COMMITTING lock — guards DB write
                committing_lock = acquire_lock(LockState.COMMITTING, job.id)
                if committing_lock is None:
                    logger.warning(
                        "COMMITTING lock held for %s, skipping", job.id
                    )
                    continue

                # 9. Create TranscodeTask for each segment + update Job
                job.status = JobStatus.PROCESSING.value
                for path in segment_paths:
                    task = TranscodeTask(
                        id=str(uuid.uuid4()),
                        job_id=job.id,
                        input_file=path,
                    )
                    session.add(task)

                session.add(job)
                session.commit()
                processed += 1

                logger.info(
                    "Job %s → PROCESSING, %d segments created",
                    job.id,
                    len(segment_paths),
                )

            except Exception:
                session.rollback()
                logger.exception("Error processing job %s", job.id)
                try:
                    current_job = session.get(Job, job.id)
                    if current_job is not None:
                        if current_job.num_of_retries + 1 >= 5:
                            current_job.status = JobStatus.FAILED.value
                            current_job.num_of_retries = 5
                            current_job.retry_after = None
                        else:
                            current_job.num_of_retries += 1
                            current_job.retry_after = (
                                datetime.now(timezone.utc).replace(tzinfo=None)
                                + timedelta(minutes=2)
                            )
                        session.commit()
                except Exception:
                    session.rollback()
                    logger.exception(
                        "Error updating retry state for job %s", job.id
                    )
            finally:
                if committing_lock is not None:
                    release_lock(committing_lock)
                if processing_lock is not None:
                    release_lock(processing_lock)

        return {"processed": processed}

    finally:
        session.close()
