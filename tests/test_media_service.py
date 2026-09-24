"""Tests for app.media_service module."""

import os
import sys
from unittest.mock import patch, MagicMock

import pytest

sys.modules["ffmpeg"] = MagicMock()

from app.media_service.segmentation import Segmentation
from app.media_service.thumbnail import GenerateThumbnail
from app.media_service.transcoder import MediaTranscoder
from app.media_service.cleanup import MediaCleanup
from app.media_service.generate_init import GenerateInit
from app.config import settings


# ---------------------------------------------------------------------------
# Segmentation
# ---------------------------------------------------------------------------

class TestSegmentation:
    def test_init(self):
        seg = Segmentation("/input.mp4", "/out", "seg", 6)
        assert seg.video_path == "/input.mp4"
        assert seg.output_dir == "/out"
        assert seg.seg_prefix == "seg"
        assert seg.seg_duration == 6

    @patch("app.media_service.segmentation.glob.glob")
    @patch("app.media_service.segmentation.subprocess.run")
    def test_generate_segments_success(self, mock_run, mock_glob):
        mock_run.return_value = MagicMock(returncode=0)
        mock_glob.return_value = ["/out/seg1.mp4", "/out/seg2.mp4"]

        seg = Segmentation("/input.mp4", "/out", "seg_", 6)
        result = seg.generate_segments()

        assert result == ["/out/seg1.mp4", "/out/seg2.mp4"]
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert "ffmpeg" in args
        assert "-segment_time" in args

    @patch("app.media_service.segmentation.subprocess.run")
    def test_generate_segments_failure(self, mock_run):
        from subprocess import CalledProcessError
        mock_run.side_effect = CalledProcessError(1, "ffmpeg")

        seg = Segmentation("/input.mp4", "/out", "seg", 6)
        with pytest.raises(CalledProcessError):
            seg.generate_segments()


# ---------------------------------------------------------------------------
# GenerateThumbnail
# ---------------------------------------------------------------------------

class TestGenerateThumbnail:
    def test_init(self):
        gen = GenerateThumbnail("/video.mp4", "/thumbs", "thumb1")
        assert gen.video_path == "/video.mp4"
        assert gen.output_dir == "/thumbs"
        assert gen.thumbnail_prefix == "thumb1"

    @patch("app.media_service.thumbnail.subprocess.run")
    def test_generate_thumbnail_success(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)

        gen = GenerateThumbnail("/video.mp4", "/thumbs", "thumb1")
        result = gen.generate_thumbnail()

        assert result == os.path.join("/thumbs", "thumb1.jpg")
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert "ffmpeg" in args
        assert "-vframes" in args

    @patch("app.media_service.thumbnail.subprocess.run")
    def test_generate_thumbnail_failure(self, mock_run):
        from subprocess import CalledProcessError
        mock_run.side_effect = CalledProcessError(1, "ffmpeg")

        gen = GenerateThumbnail("/video.mp4", "/thumbs", "thumb1")
        with pytest.raises(CalledProcessError):
            gen.generate_thumbnail()


# ---------------------------------------------------------------------------
# MediaTranscoder
# ---------------------------------------------------------------------------

class TestMediaTranscoder:
    def test_init(self):
        t = MediaTranscoder("/input.mp4", "/tmp/transcoded")
        assert t.input_file == "/input.mp4"
        assert t.output_dir == "/tmp/transcoded"
        assert t.codec == "libx264"

    def test_init_custom_codec(self):
        t = MediaTranscoder("/input.mp4", "/tmp/transcoded", codec="libx265")
        assert t.codec == "libx265"

    def test_renditions_dict(self):
        assert "360p" in settings.renditions
        assert "480p" in settings.renditions
        assert "720p" in settings.renditions
        assert "1080p" in settings.renditions
        for key in settings.renditions:
            assert "width" in settings.renditions[key]
            assert "height" in settings.renditions[key]
            assert "bitrate" in settings.renditions[key]

    @patch("app.media_service.transcoder.os.makedirs")
    @patch("builtins.open", new_callable=MagicMock)
    @patch("app.media_service.transcoder.get_video_framerate", return_value=30.0)
    @patch("app.media_service.transcoder.subprocess.Popen")
    def test_run_transcoder_all_renditions(self, mock_popen, mock_get_framerate, mock_open, mock_makedirs):
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"video_bytes", b"")
        mock_process.returncode = 0
        mock_process.stdout = MagicMock()
        mock_popen.return_value = mock_process

        t = MediaTranscoder("/tmp/downloads/vid1/video.mp4", "/tmp/transcoded")
        paths = t.run_transcoder()

        assert isinstance(paths, list)
        assert len(paths) == 5
        assert all(p.endswith("video.m4s") for p in paths)
        assert mock_get_framerate.called

    @patch("app.media_service.transcoder.os.makedirs")
    @patch("builtins.open", new_callable=MagicMock)
    @patch("app.media_service.transcoder.get_video_framerate", return_value=30.0)
    @patch("app.media_service.transcoder.subprocess.Popen")
    def test_audio_extraction_failure(self, mock_popen, mock_get_framerate, mock_open, mock_makedirs):
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"", b"error")
        mock_process.returncode = 1
        mock_process.stdout = MagicMock()
        mock_popen.return_value = mock_process

        t = MediaTranscoder("/tmp/downloads/vid1/video.mp4", "/tmp/transcoded")
        with pytest.raises(RuntimeError, match="Audio extraction failed"):
            t.run_transcoder()

    @patch("app.media_service.transcoder.os.makedirs")
    @patch("builtins.open", new_callable=MagicMock)
    def test_save_renditions(self, mock_open, mock_makedirs):
        mock_file = mock_open.return_value.__enter__.return_value
        t = MediaTranscoder("/tmp/downloads/abc/video.mp4", "/tmp/transcoded")
        results = {
            "360p": b"\x00" * 10,
            "720p": b"\x00" * 20,
            "audio": b"\x00" * 5,
        }

        paths = t.save_renditions(results, "vid123")

        assert len(paths) == 3
        assert all(p.endswith("video.m4s") for p in paths)
        assert any("vid123" in p and "360p" in p for p in paths)
        assert any("vid123" in p and "720p" in p for p in paths)
        assert any("vid123" in p and "audio" in p for p in paths)
        assert mock_makedirs.call_count == 3
        assert mock_file.write.call_count == 3


# ---------------------------------------------------------------------------
# MediaCleanup
# ---------------------------------------------------------------------------

class TestMediaCleanup:
    def test_singleton(self):
        a = MediaCleanup()
        b = MediaCleanup()
        assert a is b

    def test_cleanup_bucket_empty_list(self):
        cleanup = MediaCleanup()
        # Should return without error
        cleanup.cleanup_bucket([], "bucket")

    @patch("app.media_service.cleanup.delete_object")
    def test_cleanup_bucket_deletes_objects(self, mock_delete):
        cleanup = MediaCleanup()
        cleanup.cleanup_bucket(["key1", "key2"], "mybucket")
        assert mock_delete.call_count == 2
        mock_delete.assert_any_call("key1", "mybucket")
        mock_delete.assert_any_call("key2", "mybucket")

    @patch("app.media_service.cleanup.delete_object")
    def test_cleanup_bucket_propagates_error(self, mock_delete):
        mock_delete.side_effect = Exception("MinIO error")
        cleanup = MediaCleanup()
        with pytest.raises(Exception, match="MinIO error"):
            cleanup.cleanup_bucket(["key1"], "mybucket")

    @patch("app.media_service.cleanup.shutil.rmtree")
    @patch("app.media_service.cleanup.os.remove")
    @patch("app.media_service.cleanup.os.path.isdir")
    @patch("app.media_service.cleanup.os.listdir")
    @patch("app.media_service.cleanup.settings")
    def test_cleanup_temp_files(self, mock_settings, mock_listdir, mock_isdir, mock_remove, mock_rmtree):
        mock_settings.vid_download_dir = "/tmp/downloads"
        mock_settings.vid_segment_dir = "/tmp/segments"
        mock_settings.vid_transcode_dir = "/tmp/transcoded"
        mock_settings.vid_thumbnail_dir = "/tmp/thumbnails"

        # Each base_dir is a directory; listdir returns mixed entries
        base_dirs = {
            "/tmp/downloads", "/tmp/segments", "/tmp/transcoded", "/tmp/thumbnails",
        }
        # video123 under segments is a directory; others are files
        dir_entries = {os.path.join("/tmp/segments", "video123")}
        mock_isdir.side_effect = lambda p: p in base_dirs or p in dir_entries

        def _listdir(path):
            return {
                "/tmp/downloads": ["video123_meta", "other_video123", "unrelated"],
                "/tmp/segments": ["video123", "other_video123_extra", "unrelated"],
                "/tmp/transcoded": ["video123"],
                "/tmp/thumbnails": ["video123_thumb.jpg", "unrelated"],
            }[path]

        mock_listdir.side_effect = _listdir

        cleanup = MediaCleanup()
        cleanup.cleanup_temp_files("video123")

        # video123 is a dir under segments → rmtree
        assert mock_rmtree.call_count == 1
        mock_rmtree.assert_any_call(os.path.join("/tmp/segments", "video123"))
        # video123_meta (downloads), video123 (transcoded), video123_thumb.jpg (thumbnails) → os.remove
        assert mock_remove.call_count == 3
        mock_remove.assert_any_call(os.path.join("/tmp/downloads", "video123_meta"))
        mock_remove.assert_any_call(os.path.join("/tmp/transcoded", "video123"))
        mock_remove.assert_any_call(os.path.join("/tmp/thumbnails", "video123_thumb.jpg"))

        # Ensure non-matching entries were NOT deleted
        all_removed = [c.args[0] if c.args else "" for c in mock_remove.call_args_list]
        all_rmtreed = [c.args[0] if c.args else "" for c in mock_rmtree.call_args_list]
        for name in ["other_video123", "other_video123_extra", "unrelated"]:
            assert not any(name in p for p in all_removed + all_rmtreed)

    @patch("app.media_service.cleanup.shutil.rmtree")
    @patch("app.media_service.cleanup.os.listdir")
    @patch("app.media_service.cleanup.os.path.isdir")
    @patch("app.media_service.cleanup.settings")
    def test_cleanup_temp_files_missing_base_dir(self, mock_settings, mock_isdir, mock_listdir, mock_rmtree):
        mock_settings.vid_download_dir = "/tmp/downloads"
        mock_settings.vid_segment_dir = "/tmp/segments"
        mock_settings.vid_transcode_dir = "/tmp/transcoded"
        mock_settings.vid_thumbnail_dir = "/tmp/thumbnails"
        # All base dirs missing
        mock_isdir.return_value = False

        cleanup = MediaCleanup()
        cleanup.cleanup_temp_files("video123")

        mock_rmtree.assert_not_called()
        mock_listdir.assert_not_called()


# ---------------------------------------------------------------------------
# GenerateInit
# ---------------------------------------------------------------------------

class TestGenerateInit:
    def test_init(self):
        g = GenerateInit("/input.mp4", "/out", ["360p", "720p"])
        assert g.input_file == "/input.mp4"
        assert g.output_dir == "/out"
        assert g.representation == ["360p", "720p"]

    @patch("app.media_service.generate_init.get_video_framerate", return_value=30.0)
    @patch("app.media_service.generate_init.subprocess.Popen")
    def test_generate_init_file(self, mock_popen, mock_get_framerate):
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"", b"")
        mock_process.returncode = 0
        mock_popen.return_value = mock_process

        g = GenerateInit("/input.mp4", "/out", ["360p", "480p"])
        paths = g.generate_init_file()

        assert len(paths) == 3
        assert paths[0].endswith(os.path.join("360p", "init.mp4"))
        assert paths[1].endswith(os.path.join("480p", "init.mp4"))
        assert paths[2].endswith(os.path.join("audio", "init.mp4"))
        assert mock_popen.call_count == 3
        assert mock_get_framerate.called

    @patch("app.media_service.generate_init.get_video_framerate", return_value=30.0)
    @patch("app.media_service.generate_init.subprocess.Popen")
    def test_generate_init_failure(self, mock_popen, mock_get_framerate):
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"", b"error")
        mock_process.returncode = 1
        mock_popen.return_value = mock_process

        g = GenerateInit("/input.mp4", "/out", ["360p"])
        with pytest.raises(RuntimeError, match="Init segment failed"):
            g.generate_init_file()
