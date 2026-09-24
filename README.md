# Media Processing Service — MERIDIAN

Production-grade video processing pipeline service for the MERIDIAN monorepo. Consumes video events from Kafka, downloads segments from MinIO, transcodes to multiple renditions, uploads back to MinIO, and publishes completion/failure events via the transactional outbox pattern.

## Architecture

```
┌─────────────┐     ┌─────────┐     ┌──────────────────────┐
│  video.queued │────▶│  Kafka  │────▶│  FastAPI + Consumer  │
│    (topic)    │     │         │     │    (port 8001)       │
└─────────────┘     └─────────┘     └──────────┬───────────┘
                                                │
                                    ┌───────────▼───────────┐
                                    │      Celery Worker     │
                                    │  (7 periodic tasks)    │
                                    └───────────┬───────────┘
                                                │
                        ┌───────────────────────┼───────────────────────┐
                        │                       │                       │
                ┌───────▼───────┐       ┌───────▼───────┐       ┌───────▼───────┐
                │  PostgreSQL   │       │     Redis     │       │     MinIO     │
                │  (jobs, tasks,│       │  (distributed │       │  (viduploads, │
                │   outbox)     │       │    locks)     │       │  vidsegments, │
                └───────────────┘       └───────────────┘       │  vidthumbnails)│
                                                                └───────────────┘
```

## Processing Pipeline

```
video.queued event
    │
    ▼
┌─────────────────────────────────────┐
│  1. Kafka Consumer                  │
│     Creates Job (status: QUEUED)    │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  2. process_queued_jobs (every 15s) │
│     Download video from MinIO       │
│     Generate thumbnail              │
│     Segment into 6-second chunks    │
│     Generate init segments (CMAF)   │
│     Create TranscodeTasks           │
│     Status → PROCESSING             │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  3. process_transcode_tasks (10s)   │
│     Transcode each segment:         │
│     360p / 480p / 720p / 1080p      │
│     Create UploadTasks              │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  4. process_upload_tasks (every 10s)│
│     Upload CMAF .m4s segments to    │
│     MinIO vidsegments bucket        │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  5. check_completed_jobs (every 15s)│
│     Verify all TranscodeTasks done  │
│     Status → COMPLETED              │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  6. process_completed_jobs (15s)    │
│     Cleanup temp files              │
│     Publish job.completed to Kafka  │
│     via Outbox pattern              │
└─────────────────────────────────────┘
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.12 |
| Web Framework | FastAPI |
| Task Queue | Celery (RabbitMQ broker) |
| Message Broker | Apache Kafka (aiokafka) |
| Database | PostgreSQL (asyncpg + psycopg2) |
| Object Storage | MinIO (S3-compatible) |
| Distributed Locks | Redis |
| Video Processing | FFmpeg |
| ORM | SQLModel |
| Containerization | Docker |

## Project Structure

```
media_processing_service/
├── Dockerfile                  # FastAPI web server + Kafka consumer
├── Dockerfile.celery           # Celery worker (solo pool)
├── requirements.txt
├── .env.example
├── start.sh
└── app/
    ├── main.py                 # FastAPI application entry point
    ├── celery_app.py           # Celery configuration + beat schedule
    ├── config.py               # Pydantic settings (env-based)
    ├── consumer.py             # Kafka consumer (video.queued)
    ├── producer.py             # Singleton sync Kafka producer (auto event_id + ISO timestamp)
    ├── lock.py                 # Redis distributed lock (PROCESSING + COMMITTING)
    ├── utils.py                # build_object_url, resolve_object_key, get_video_duration
    ├── db_config/
    │   ├── __init__.py         # Re-exports all DB symbols
    │   ├── database.py         # Async engine + session factory
    │   └── database_sync.py    # Sync engine for Celery tasks
    ├── media_service/
    │   ├── __init__.py         # Re-exports all media utilities
    │   ├── segmentation.py     # FFmpeg video segmentation
    │   ├── thumbnail.py        # FFmpeg thumbnail generation
    │   ├── transcoder.py       # CMAF multi-rendition transcoding (360p–1080p)
    │   ├── generate_init.py    # CMAF init segment generation (video + audio)
    │   └── cleanup.py          # Singleton MinIO + temp file cleanup
    ├── minio_client/
    │   └── __init__.py         # download, upload, delete (MinIO SDK)
    ├── models/
    │   ├── job.py              # Job table + JobStatus enum
    │   ├── transcode_task.py   # TranscodeTask table + TranscodeTaskStatus
    │   ├── upload_task.py      # UploadTask table + UploadStatus
    │   ├── outbox.py           # Transactional Outbox table + OutboxStatus
    │   └── event.py            # Pydantic model for incoming Kafka events (timestamp: str)
    ├── routes/
    │   ├── health.py           # GET /health
    │   └── ready.py            # GET /ready (unified multi-dependency readiness)
    └── tasks/
        ├── process_queued_jobs.py        # Download, segment, create TranscodeTasks
        ├── process_transcode_tasks.py    # Transcode segments into renditions
        ├── process_upload_tasks.py       # Upload to MinIO vidsegments
        ├── check_completed_jobs.py       # Verify completion, mark COMPLETED
        ├── process_outbox_events.py      # Publish outbox events to Kafka
        ├── process_failed_jobs.py        # Cleanup failed jobs, publish job.failed
        └── process_completed_jobs.py     # Cleanup completed jobs, publish job.completed
```

## Celery Beat Schedule

| Task | Schedule | Description |
|------|----------|-------------|
| `process_queued_jobs` | Every 15s | Download video, generate thumbnail, segment, generate init segments, create TranscodeTasks |
| `process_transcode_tasks` | Every 10s | Transcode segments into 360p/480p/720p/1080p |
| `process_upload_tasks` | Every 10s | Upload CMAF .m4s segments and init files to MinIO vidsegments bucket |
| `check_completed_jobs` | Every 15s | Check all TranscodeTasks are COMPLETED, mark job COMPLETED |
| `process_outbox_events` | Every 10s | Publish pending outbox events to Kafka |
| `process_failed_jobs` | Every 15s | Cleanup failed jobs (MinIO + temp), publish `job.failed` |
| `process_completed_jobs` | Every 15s | Extract video duration, cleanup temp files, publish `job.completed` |

## Kafka Topics

| Topic | Partitions | Purpose |
|-------|------------|---------|
| `video.queued` | 8 | Incoming video events from video service |
| `bucketnotifications` | 4 | MinIO bucket notification events |
| `job.completed` | 4 | Published when a job completes processing |
| `job.failed` | 4 | Published when a job fails processing |

## Database Schema

### Jobs
| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR(128) PK | Job identifier (`job:{event_id}`) |
| `status` | VARCHAR(16) | QUEUED → PROCESSING → COMPLETED / FAILED |
| `video_id` | VARCHAR(255) | Video identifier |
| `object_url` | VARCHAR(1024) | Original video URL in MinIO |
| `published` | BOOLEAN | Whether outbox event has been published |
| `vid_thumbnail_url` | VARCHAR(1024) | Thumbnail URL |
| `num_of_retries` | INT | Retry counter |
| `retry_after` | TIMESTAMP | Next retry window |

### TranscodeTasks
| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR(36) PK | UUID |
| `job_id` | VARCHAR(128) FK | Parent job |
| `status` | VARCHAR(16) | QUEUED → PROCESSING → COMPLETED / FAILED |
| `input_file` | VARCHAR(1024) | Local segment file path |

### UploadTasks
| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR(36) PK | UUID |
| `transcode_id` | VARCHAR(36) FK | Parent transcode task |
| `upload_files` | JSON | List of file paths to upload |
| `status` | VARCHAR(16) | PENDING → COMPLETED / FAILED |

### Outbox
| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR(36) PK | UUID |
| `topic` | VARCHAR(255) | Kafka topic to publish to |
| `payload` | JSON | Event payload |
| `status` | VARCHAR(32) | PENDING → PROCESSED / FAILED |
| `retry_count` | INT | Retry counter |

## Distributed Locking

Uses a dual-lock mechanism via Redis:

- **PROCESSING lock** — guards download + segment + upload operations
- **COMMITTING lock** — guards database writes

Both locks use a 2-minute TTL with a 5-second blocking timeout. Locks are always released in `finally` blocks to prevent deadlocks.

## Configuration

All settings are loaded from environment variables via `pydantic-settings`. Copy `.env.example` to `.env` and update values.

### Key Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker addresses |
| `KAFKA_TOPIC` | `video.queued` | Incoming video topic |
| `DATABASE_URL` | `postgresql+asyncpg://...` | PostgreSQL connection |
| `REDIS_HOST` | `redis` | Redis host for distributed locks |
| `MINIO_ENDPOINT` | `minio:9000` | MinIO endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `MINIO_DOWNLOAD_BUCKET` | `viduploads` | Bucket to download original videos from |
| `MINIO_UPLOAD_BUCKET` | `vidsegments` | Bucket for transcoded segments |
| `MINIO_SEGMENT_BUCKET` | `vidsegments` | Bucket for segment operations |
| `MINIO_THUMBNAIL_BUCKET` | `vidthumbnails` | Bucket for thumbnails |
| `CELERY_BROKER_URL` | `amqp://guest:guest@rabbitmq:5672//` | RabbitMQ broker |
| `CELERY_RESULT_BACKEND` | `redis://redis:6379/1` | Redis for Celery results |
| `SEGMENT_DURATION` | `6` | Segment duration in seconds |
| `SEGMENT_PREFIX` | `seg_` | Filename prefix for generated segments |
| `LOG_LEVEL` | `info` | Python logging level |

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check (always returns 200) |
| `GET` | `/ready` | Readiness: Redis, DB, MinIO, RabbitMQ, Kafka (`{"status","checks"}`; 200/500) |

## Docker

### FastAPI Server + Kafka Consumer

```bash
docker build -t media-processing-service .
docker run -p 8001:8001 --env-file .env media-processing-service
```

### Celery Worker

```bash
docker build -f Dockerfile.celery -t media-processing-celery .
docker run --env-file .env media-processing-celery
```

## Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run FastAPI server
uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload

# Run Celery worker
celery -A app.celery_app:celery_app worker --loglevel=info --pool=solo

# Run Celery beat (scheduler)
celery -A app.celery_app:celery_app beat --loglevel=info
```

## Testing

```bash
cd media_processing_service
python -m pytest tests/ -v
```

| Suite | Coverage |
|-------|----------|
| `test_ready.py` | Unified `/ready` shape + per-dependency failures |
| `test_consumer.py` | Kafka consumer persistence |
| `test_tasks.py` | Queued/transcode/upload/completion/failed/outbox Celery tasks |
| `test_media_service.py` | Segmentation, transcoding, init segments, thumbnails |
| `test_utils.py` | Object URL/key helpers, duration, framerate |
| `test_smoke.py` | Imports and router wiring |
| Load/perf | Optional autocannon / benchmark suites |

Latest local run: **98 passed**.

## License

Private — MERIDIAN Project
