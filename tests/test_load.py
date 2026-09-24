"""Concurrent load tests for the media processing service."""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import patch, MagicMock

sys.modules["ffmpeg"] = MagicMock()

from app.utils import build_object_url, resolve_object_key
from app.media_service.cleanup import MediaCleanup


class TestBuildObjectUrlConcurrency:
    def test_50_threads(self):
        results = []

        with patch("app.utils.settings") as mock_settings:
            mock_settings.minio_endpoint = "minio:9000"

            def build():
                return build_object_url("abc/seg.mp4", "vidsegments")

            with ThreadPoolExecutor(max_workers=50) as pool:
                futures = [pool.submit(build) for _ in range(50)]
                for f in as_completed(futures):
                    results.append(f.result())

        assert len(results) == 50
        assert all(r is not None for r in results)


class TestResolveObjectKeyConcurrency:
    def test_50_threads(self):
        results = []

        def resolve():
            return resolve_object_key(
                "/app/vid_transcoded/abc/720p/seg_001.mp4",
                "/app/vid_transcoded",
            )

        with ThreadPoolExecutor(max_workers=50) as pool:
            futures = [pool.submit(resolve) for _ in range(50)]
            for f in as_completed(futures):
                results.append(f.result())

        assert len(results) == 50
        assert all(r == "abc/720p/seg_001.mp4" for r in results)


class TestCleanupBucketConcurrency:
    def test_100_threads(self):
        with patch("app.media_service.cleanup.delete_object") as mock_delete:
            cleanup = MediaCleanup()

            with ThreadPoolExecutor(max_workers=100) as pool:
                futures = [
                    pool.submit(cleanup.cleanup_bucket, [f"key{i}"], "bucket")
                    for i in range(100)
                ]
                for f in as_completed(futures):
                    f.result()

            assert mock_delete.call_count == 100


class TestSingletonConcurrency:
    def test_100_threads(self):
        instances = []

        def get_instance():
            return MediaCleanup()

        with ThreadPoolExecutor(max_workers=100) as pool:
            futures = [pool.submit(get_instance) for _ in range(100)]
            for f in as_completed(futures):
                instances.append(f.result())

        assert len(instances) == 100
        assert all(i is instances[0] for i in instances)


class TestMixedWorkloadConcurrency:
    def test_20_build_20_resolve_20_cleanup(self):
        results = []

        with patch("app.utils.settings") as mock_settings:
            mock_settings.minio_endpoint = "minio:9000"

            with patch("app.media_service.cleanup.delete_object"):
                cleanup = MediaCleanup()

                def build():
                    return build_object_url("abc/seg.mp4", "vidsegments")

                def resolve():
                    return resolve_object_key(
                        "/app/vid_transcoded/abc/720p/seg_001.mp4",
                        "/app/vid_transcoded",
                    )

                def clean():
                    cleanup.cleanup_bucket(["key1"], "bucket")
                    return "cleaned"

                with ThreadPoolExecutor(max_workers=60) as pool:
                    futures = []
                    for _ in range(20):
                        futures.append(pool.submit(build))
                        futures.append(pool.submit(resolve))
                        futures.append(pool.submit(clean))
                    for f in as_completed(futures):
                        results.append(f.result())

        assert len(results) == 60
