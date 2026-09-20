"""Kafka consumer for the media processing service.

Subscribes to the video.queued topic and persists Job records using
a dual Redis lock mechanism (PROCESSING + COMMITTING).
"""

import asyncio
import json
import logging
from typing import Any

from aiokafka import AIOKafkaConsumer, TopicPartition, ConsumerRebalanceListener
from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.db_config import async_session_factory
from app.lock import LockState, acquire_lock, release_lock
from app.models.event import VideoQueuedEvent
from app.models.job import Job

logger = logging.getLogger(__name__)


class AppRebalanceListener(ConsumerRebalanceListener):
    """Logs Kafka partition rebalance events for the media processing consumer group."""

    async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        logger.info("Partitions revoked: %s", revoked)

    async def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        logger.info("Partitions assigned: %s", assigned)


async def consume_messages(consumer: AIOKafkaConsumer) -> None:
    """Main consumer loop. Runs until cancelled."""
    try:
        async for msg in consumer:
            processing_lock = None
            committing_lock = None
            try:
                raw_value: bytes = msg.value
                if raw_value is None:
                    logger.warning("Received empty message at offset %d", msg.offset)
                    await consumer.commit(
                        {TopicPartition(msg.topic, msg.partition): msg.offset + 1}
                    )
                    continue

                event_dict: dict[str, Any] = json.loads(raw_value.decode("utf-8"))
                event = VideoQueuedEvent.model_validate(event_dict)

                # 1. Acquire PROCESSING lock
                processing_lock = acquire_lock(LockState.PROCESSING, event.event_id)
                if processing_lock is None:
                    logger.warning(
                        "Could not acquire PROCESSING lock for event_id=%s, skipping",
                        event.event_id,
                    )
                    continue

                # 2. Create Job object in memory
                job = Job(
                    id=f"job:{event.event_id}",
                    video_id=event.video_id,
                    object_url=event.object_url,
                )

                # 3. Acquire COMMITTING lock
                committing_lock = acquire_lock(LockState.COMMITTING, event.event_id)
                if committing_lock is None:
                    logger.warning(
                        "Could not acquire COMMITTING lock for event_id=%s, skipping",
                        event.event_id,
                    )
                    continue

                # 4. Commit job to DB
                async with async_session_factory() as session:
                    session.add(job)
                    await session.commit()

                # 5. Log successful commit
                logger.info(
                    "Job committed event_id=%s origin_service=%s timestamp=%s topic=%s",
                    event.event_id,
                    event.origin_service,
                    event.timestamp,
                    msg.topic,
                )

                # 6. Commit Kafka offset
                await consumer.commit(
                    {TopicPartition(msg.topic, msg.partition): msg.offset + 1}
                )

            except IntegrityError:
                logger.warning(
                    "IntegrityError for event_id=%s, committing offset and releasing locks",
                    event.event_id,
                )
                await consumer.commit(
                    {TopicPartition(msg.topic, msg.partition): msg.offset + 1}
                )

            except json.JSONDecodeError as e:
                logger.error("Failed to decode message JSON: %s", e)
                await consumer.commit(
                    {TopicPartition(msg.topic, msg.partition): msg.offset + 1}
                )

            except Exception as e:
                logger.error("Error processing message: %s", e, exc_info=True)
                await asyncio.sleep(1)

            finally:
                if committing_lock is not None:
                    release_lock(committing_lock)
                if processing_lock is not None:
                    release_lock(processing_lock)

    except asyncio.CancelledError:
        logger.info("Consumer task cancelled")
    finally:
        await consumer.stop()
        logger.info("Consumer stopped")


async def start_consumer() -> None:
    """Create and start the Kafka consumer."""
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group_id,
        auto_offset_reset=settings.kafka_auto_offset_reset,
        enable_auto_commit=False,
        session_timeout_ms=30000,
        max_poll_interval_ms=300000,
        rebalance_timeout_ms=60000,
    )

    await consumer.start()
    consumer.subscribe([settings.kafka_topic], listener=AppRebalanceListener())
    logger.info(
        "Kafka consumer started — topic=%s group=%s",
        settings.kafka_topic,
        settings.kafka_consumer_group_id,
    )

    await consume_messages(consumer)
