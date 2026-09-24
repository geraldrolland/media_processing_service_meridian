"""Tests for app.utils module."""

from unittest.mock import patch

from app.utils import build_object_url, resolve_object_key, get_video_duration, get_video_framerate


class TestBuildObjectUrl:
    """Tests for build_object_url."""

    @patch("app.utils.settings")
    def test_basic_url(self, mock_settings):
        mock_settings.minio_endpoint = "minio:9000"
        result = build_object_url("abc/seg_001.mp4", "vidsegments")
        assert result == "http://minio:9000/vidsegments/abc/seg_001.mp4"

    @patch("app.utils.settings")
    def test_thumbnail_bucket(self, mock_settings):
        mock_settings.minio_endpoint = "minio:9000"
        result = build_object_url("vid1/thumb.jpg", "vidthumbnails")
        assert result == "http://minio:9000/vidthumbnails/vid1/thumb.jpg"

    @patch("app.utils.settings")
    def test_custom_endpoint(self, mock_settings):
        mock_settings.minio_endpoint = "minio.local:9000"
        result = build_object_url("key.mp4", "bucket")
        assert result == "http://minio.local:9000/bucket/key.mp4"

    @patch("app.utils.settings")
    def test_empty_key(self, mock_settings):
        mock_settings.minio_endpoint = "minio:9000"
        result = build_object_url("", "bucket")
        assert result == "http://minio:9000/bucket/"


class TestResolveObjectKey:
    """Tests for resolve_object_key."""

    def test_standard_path(self):
        result = resolve_object_key(
            "/app/vid_transcoded/abc/720p/seg_001_a1b2c3d4.m4s",
            "/app/vid_transcoded",
        )
        assert result == "abc/720p/seg_001.m4s"

    def test_short_prefix_matches_last_component(self):
        result = resolve_object_key(
            "/tmp/transcoded/abc/seg.mp4",
            "/tmp/transcoded",
        )
        assert result == "abc/seg.mp4"

    def test_windows_backslashes(self):
        result = resolve_object_key(
            "C:\\app\\vid_transcoded\\abc\\seg.mp4",
            "C:\\app\\vid_transcoded",
        )
        assert result == "vid_transcoded/abc/seg.mp4"

    def test_prefix_not_found_fallback_three_parts(self):
        result = resolve_object_key("/a/b/c/file.mp4", "/nonexistent")
        assert result == "b/c/file.mp4"

    def test_prefix_not_found_fallback_two_parts(self):
        result = resolve_object_key("/a/file.mp4", "/nonexistent")
        assert result == "/a/file.mp4"

    def test_deep_path(self):
        result = resolve_object_key("/a/b/c/d/e/f.mp4", "/a/b/c")
        assert result == "d/e/f.mp4"

    def test_prefix_at_end_of_path(self):
        result = resolve_object_key("/app/transcoded/file.mp4", "transcoded")
        assert result == "file.mp4"

    def test_strips_uuid_from_m4s(self):
        result = resolve_object_key(
            "/app/vid_transcoded/abc/360p/myvideo_f1e2d3c4.m4s",
            "/app/vid_transcoded",
        )
        assert result == "abc/360p/myvideo.m4s"

    def test_no_uuid_strip_for_non_m4s(self):
        result = resolve_object_key(
            "/app/vid_transcoded/abc/720p/seg_001_f1e2d3c4.mp4",
            "/app/vid_transcoded",
        )
        assert result == "abc/720p/seg_001_f1e2d3c4.mp4"


class TestGetVideoDuration:
    """Tests for get_video_duration."""

    @patch("app.utils.ffmpeg.probe")
    def test_returns_duration(self, mock_probe):
        mock_probe.return_value = {"format": {"duration": "120.5"}}
        assert get_video_duration("/tmp/video.mp4") == 120.5

    @patch("app.utils.ffmpeg.probe")
    def test_integer_duration(self, mock_probe):
        mock_probe.return_value = {"format": {"duration": "60"}}
        assert get_video_duration("/tmp/video.mp4") == 60.0


class TestGetVideoFramerate:
    """Tests for get_video_framerate."""

    @patch("app.utils.ffmpeg.probe")
    def test_returns_framerate(self, mock_probe):
        mock_probe.return_value = {
            "streams": [{"codec_type": "video", "r_frame_rate": "30/1"}]
        }
        assert get_video_framerate("/tmp/video.mp4") == 30.0

    @patch("app.utils.ffmpeg.probe")
    def test_ntsc_framerate(self, mock_probe):
        mock_probe.return_value = {
            "streams": [{"codec_type": "video", "r_frame_rate": "30000/1001"}]
        }
        result = get_video_framerate("/tmp/video.mp4")
        assert abs(result - 29.97) < 0.01

    @patch("app.utils.ffmpeg.probe")
    def test_picks_video_stream_over_audio(self, mock_probe):
        mock_probe.return_value = {
            "streams": [
                {"codec_type": "audio"},
                {"codec_type": "video", "r_frame_rate": "24/1"},
            ]
        }
        assert get_video_framerate("/tmp/video.mp4") == 24.0
