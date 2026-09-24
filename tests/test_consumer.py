"""Tests for app.consumer -- Kafka consume_messages loop."""

import asyncio
import json
import sys
from unittest.mock import patch, MagicMock, AsyncMock

from sqlalchemy.exc import IntegrityError

sys.modules.setdefault("asyncpg", MagicMock())
sys.modules.setdefault("ffmpeg", MagicMock())
sys.modules.setdefault("minio", MagicMock())
sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("aiokafka", MagicMock())


class _FakeMsg:
    def __init__(self, value, topic="video.queued", partition=0, offset=0):
        self.value = value
        self.topic = topic
        self.partition = partition
        self.offset = offset


class _FakeConsumer:
    def __init__(self, messages):
        self._messages = list(messages)
        self.commit = AsyncMock()
        self.stop = AsyncMock()

    def __aiter__(self):
        self._iter = iter(self._messages)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise asyncio.CancelledError


async def _consume(consumer):
    from app.consumer import consume_messages
    await consume_messages(consumer)


def _run(consumer):
    asyncio.run(_consume(consumer))


def _payload(**overrides):
    base = {
        "event_id": "evt1",
        "timestamp": "2024-01-01T00:00:00Z",
        "origin_service": "video_service",
        "video_id": "vid1",
        "object_url": "http://minio:9000/viduploads/videos/vid1/file.mp4",
    }
    base.update(overrides)
    return base


class _AsyncSessionCM:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _make_session():
    session = MagicMock()
    session.add = MagicMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    return session


def _factory(session):
    def _f():
        return _AsyncSessionCM(session)
    return _f


class TestConsumeMessages:

    def test_null_value_commits_and_skips(self):
        consumer = _FakeConsumer([_FakeMsg(None)])
        with patch("app.consumer.async_session_factory") as mock_factory, \
             patch("app.consumer.acquire_lock") as mock_acquire:
            _run(consumer)
        mock_factory.assert_not_called()
        mock_acquire.assert_not_called()
        consumer.commit.assert_awaited()
        consumer.stop.assert_awaited()

    def test_validation_error_sleeps_without_commit(self):
        msg = _FakeMsg(json.dumps({"event_id": "evt1"}).encode())
        consumer = _FakeConsumer([msg])
        with patch("app.consumer.async_session_factory") as mock_factory, \
             patch("app.consumer.acquire_lock") as mock_acquire, \
             patch("asyncio.sleep", new=AsyncMock()) as mock_sleep:
            _run(consumer)
        mock_factory.assert_not_called()
        mock_acquire.assert_not_called()
        mock_sleep.assert_awaited_with(1)
        consumer.commit.assert_not_awaited()
        consumer.stop.assert_awaited()

    def test_happy_path_acquires_locks_commits_job_and_offset(self):
        msg = _FakeMsg(json.dumps(_payload()).encode())
        consumer = _FakeConsumer([msg])
        session = _make_session()
        processing_lock = MagicMock()
        committing_lock = MagicMock()

        with patch("app.consumer.async_session_factory", return_value=_AsyncSessionCM(session)), \
             patch("app.consumer.acquire_lock", side_effect=[processing_lock, committing_lock]) as mock_acquire, \
             patch("app.consumer.release_lock") as mock_release:
            _run(consumer)

        assert mock_acquire.call_count == 2
        assert session.add.call_count == 1
        job = session.add.call_args[0][0]
        assert job.id == "job:evt1"
        assert job.video_id == "vid1"
        assert job.object_url == "http://minio:9000/viduploads/videos/vid1/file.mp4"
        session.commit.assert_awaited_once()
        consumer.commit.assert_awaited()
        consumer.stop.assert_awaited()
        assert mock_release.call_count == 2
        mock_release.assert_any_call(committing_lock)
        mock_release.assert_any_call(processing_lock)

    def test_processing_lock_none_skips_without_commit(self):
        msg = _FakeMsg(json.dumps(_payload()).encode())
        consumer = _FakeConsumer([msg])
        with patch("app.consumer.async_session_factory") as mock_factory, \
             patch("app.consumer.acquire_lock", return_value=None) as mock_acquire, \
             patch("app.consumer.release_lock") as mock_release:
            _run(consumer)

        assert mock_acquire.call_count == 1
        mock_factory.assert_not_called()
        mock_release.assert_not_called()
        consumer.commit.assert_not_awaited()
        consumer.stop.assert_awaited()

    def test_committing_lock_none_releases_processing_and_skips_commit(self):
        msg = _FakeMsg(json.dumps(_payload()).encode())
        consumer = _FakeConsumer([msg])
        processing_lock = MagicMock()

        with patch("app.consumer.async_session_factory") as mock_factory, \
             patch("app.consumer.acquire_lock", side_effect=[processing_lock, None]), \
             patch("app.consumer.release_lock") as mock_release:
            _run(consumer)

        mock_factory.assert_not_called()
        consumer.commit.assert_not_awaited()
        mock_release.assert_called_once_with(processing_lock)
        consumer.stop.assert_awaited()

    def test_json_decode_error_commits_offset(self):
        msg = _FakeMsg(b"not-json{")
        consumer = _FakeConsumer([msg])
        with patch("app.consumer.async_session_factory") as mock_factory:
            _run(consumer)
        mock_factory.assert_not_called()
        consumer.commit.assert_awaited()
        consumer.stop.assert_awaited()

    def test_integrity_error_commits_offset_and_releases_locks(self):
        msg = _FakeMsg(json.dumps(_payload()).encode())
        consumer = _FakeConsumer([msg])
        session = _make_session()
        session.commit = AsyncMock(
            side_effect=IntegrityError("INSERT", {}, Exception("duplicate"))
        )
        processing_lock = MagicMock()
        committing_lock = MagicMock()

        with patch("app.consumer.async_session_factory", return_value=_AsyncSessionCM(session)), \
             patch("app.consumer.acquire_lock", side_effect=[processing_lock, committing_lock]), \
             patch("app.consumer.release_lock") as mock_release:
            _run(consumer)

        consumer.commit.assert_awaited()
        consumer.stop.assert_awaited()
        assert mock_release.call_count == 2

    def test_json_error_commits_and_releases_no_locks(self):
        msg = _FakeMsg(b"{invalid json")
        consumer = _FakeConsumer([msg])
        with patch("app.consumer.acquire_lock") as mock_acquire, \
             patch("app.consumer.release_lock") as mock_release:
            _run(consumer)
        mock_acquire.assert_not_called()
        mock_release.assert_not_called()
        consumer.commit.assert_awaited()

    def test_generic_error_sleeps_without_kafka_commit(self):
        msg = _FakeMsg(json.dumps(_payload()).encode())
        consumer = _FakeConsumer([msg])
        session = _make_session()
        session.commit = AsyncMock(side_effect=Exception("db down"))
        processing_lock = MagicMock()
        committing_lock = MagicMock()

        with patch("app.consumer.async_session_factory", return_value=_AsyncSessionCM(session)), \
             patch("app.consumer.acquire_lock", side_effect=[processing_lock, committing_lock]), \
             patch("app.consumer.release_lock"), \
             patch("asyncio.sleep", new=AsyncMock()) as mock_sleep:
            _run(consumer)

        mock_sleep.assert_awaited_with(1)
        consumer.commit.assert_not_awaited()
        consumer.stop.assert_awaited()

    def test_cancelled_stops_consumer(self):
        consumer = _FakeConsumer([])
        _run(consumer)
        consumer.stop.assert_awaited_once()
        consumer.commit.assert_not_awaited()
