"""Performance benchmarks for the media processing service."""

import time
from unittest.mock import patch, MagicMock

from app.utils import build_object_url, resolve_object_key
from app.media_service.cleanup import MediaCleanup


class TestBuildObjectUrlPerformance:
    def test_10000_calls(self):
        with patch("app.utils.settings") as mock_settings:
            mock_settings.minio_endpoint = "minio:9000"
            start = time.perf_counter()
            for _ in range(10000):
                build_object_url("abc/seg.mp4", "vidsegments")
            elapsed = time.perf_counter() - start
            assert elapsed < 1.0, f"10000 calls took {elapsed:.3f}s (threshold: 1.0s)"


class TestResolveObjectKeyPerformance:
    def test_10000_calls(self):
        start = time.perf_counter()
        for _ in range(10000):
            resolve_object_key(
                "/app/vid_transcoded/abc/720p/seg_001.mp4",
                "/app/vid_transcoded",
            )
        elapsed = time.perf_counter() - start
        assert elapsed < 1.0, f"10000 calls took {elapsed:.3f}s (threshold: 1.0s)"


class TestCleanupBucketPerformance:
    def test_100_objects_concurrent(self):
        with patch("app.media_service.cleanup.delete_object") as mock_delete:
            start = time.perf_counter()
            cleanup = MediaCleanup()
            cleanup.cleanup_bucket([f"key{i}" for i in range(100)], "bucket")
            elapsed = time.perf_counter() - start
            assert mock_delete.call_count == 100
            assert elapsed < 5.0, f"100 deletes took {elapsed:.3f}s (threshold: 5.0s)"


class TestSingletonInstantiationPerformance:
    def test_10000_calls(self):
        start = time.perf_counter()
        for _ in range(10000):
            MediaCleanup()
        elapsed = time.perf_counter() - start
        assert elapsed < 0.5, f"10000 instantiations took {elapsed:.3f}s (threshold: 0.5s)"


class TestLockPerformance:
    @patch("app.lock.redis_client")
    def test_100_acquire_release_cycles(self, mock_redis):
        mock_lock = MagicMock()
        mock_lock.acquire.return_value = True
        mock_redis.lock.return_value = mock_lock

        from app.lock import acquire_lock, release_lock, LockState

        start = time.perf_counter()
        for i in range(100):
            lock = acquire_lock(LockState.PROCESSING, f"event_{i}")
            if lock:
                release_lock(lock)
        elapsed = time.perf_counter() - start
        assert elapsed < 2.0, f"100 acquire/release cycles took {elapsed:.3f}s (threshold: 2.0s)"
