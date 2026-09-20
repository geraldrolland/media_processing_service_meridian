"""Media transcoding module using ffmpeg piped processes."""

import logging
import os
import subprocess
import tempfile
from typing import Any

import ffmpeg

logger = logging.getLogger(__name__)

RENDITIONS: dict[str, dict[str, Any]] = {
    "360p": {"width": 640, "height": 360, "bitrate": "800k"},
    "480p": {"width": 854, "height": 480, "bitrate": "1400k"},
    "720p": {"width": 1280, "height": 720, "bitrate": "2500k"},
    "1080p": {"width": 1920, "height": 1080, "bitrate": "4500k"},
}


class MediaTranscoder:
    """Transcodes a video file into multiple renditions using ffmpeg.

    Each rendition runs two piped ffmpeg processes:
      Process 1 — reads the input file frame-by-frame (raw RGB24).
      Process 2 — transcodes the raw frames to H.264 and outputs MP4 bytes.

    The transcoded video is then merged with the extracted audio stream.
    """

    def __init__(self, input_file: str, codec: str = "libx264"):
        self.input_file = input_file
        self.codec = codec

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def __get_input_fps(self) -> float:
        """Detect the frame rate of the input video via ffprobe."""
        probe = ffmpeg.probe(self.input_file)
        video_stream = next(
            s for s in probe["streams"] if s["codec_type"] == "video"
        )
        r_frame_rate: str = video_stream["r_frame_rate"]
        num, den = map(int, r_frame_rate.split("/"))
        return num / den

    def __extract_audio(self) -> bytes:
        """Extract the audio stream from the input file (copy, no re-encode)."""
        process = subprocess.Popen(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-i", self.input_file,
                "-vn", "-acodec", "copy",
                "-f", "adts", "pipe:1",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        audio_bytes, stderr = process.communicate()
        if process.returncode != 0:
            raise RuntimeError(
                f"Audio extraction failed (rc={process.returncode}): "
                f"{stderr.decode()}"
            )
        return audio_bytes

    def __transcode_rendition(
        self, width: int, height: int, bitrate: str
    ) -> bytes:
        """Run the two-process ffmpeg pipeline for a single rendition.

        Process 1 reads the input file frame-by-frame and pipes raw RGB24
        frames to Process 2, which transcodes them to H.264 and outputs
        MP4 bytes.
        """
        fps = self.__get_input_fps()

        # Process 1: frame reader — input file → raw RGB24 on stdout
        process1 = subprocess.Popen(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-i", self.input_file,
                "-f", "rawvideo", "-pix_fmt", "rgb24",
                "pipe:1",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Process 2: transcoder — raw RGB24 on stdin → H.264 MP4 on stdout
        process2 = subprocess.Popen(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-f", "rawvideo", "-pix_fmt", "rgb24",
                "-s", f"{width}x{height}", "-r", str(fps),
                "-i", "pipe:0",
                "-c:v", self.codec, "-b:v", bitrate,
                "-pix_fmt", "yuv420p",
                "-movflags", "+frag_keyframe+empty_moov",
                "-f", "mp4", "pipe:1",
            ],
            stdin=process1.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Close process1 stdout so it receives SIGPIPE when process2 exits
        process1.stdout.close()

        video_bytes, stderr = process2.communicate()
        process1.wait()

        if process1.returncode != 0:
            raise RuntimeError(
                f"Frame reader failed (rc={process1.returncode})"
            )
        if process2.returncode != 0:
            raise RuntimeError(
                f"Transcoder failed (rc={process2.returncode}): "
                f"{stderr.decode()}"
            )
        return video_bytes

    def __merge_video_audio(
        self, video_bytes: bytes, audio_bytes: bytes
    ) -> bytes:
        """Merge transcoded video bytes with extracted audio bytes.

        Uses temporary files because ffmpeg cannot read two separate
        byte streams from a single stdin.
        """
        video_tmp_path: str | None = None
        audio_tmp_path: str | None = None
        output_path: str | None = None

        try:
            with tempfile.NamedTemporaryFile(
                suffix=".mp4", delete=False
            ) as tmp:
                tmp.write(video_bytes)
                video_tmp_path = tmp.name

            with tempfile.NamedTemporaryFile(
                suffix=".aac", delete=False
            ) as tmp:
                tmp.write(audio_bytes)
                audio_tmp_path = tmp.name

            output_path = video_tmp_path.replace(".mp4", "_merged.mp4")

            process = subprocess.Popen(
                [
                    "ffmpeg", "-hide_banner", "-loglevel", "error",
                    "-i", video_tmp_path,
                    "-i", audio_tmp_path,
                    "-c", "copy",
                    "-f", "mp4", output_path,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            _, stderr = process.communicate()
            if process.returncode != 0:
                raise RuntimeError(
                    f"Merge failed (rc={process.returncode}): "
                    f"{stderr.decode()}"
                )

            with open(output_path, "rb") as f:
                return f.read()
        finally:
            for path in (video_tmp_path, audio_tmp_path, output_path):
                if path is not None and os.path.exists(path):
                    os.unlink(path)

    # ------------------------------------------------------------------
    # Private rendition methods
    # ------------------------------------------------------------------

    def __transcode_360p(self) -> bytes:
        r = RENDITIONS["360p"]
        return self.__transcode_rendition(r["width"], r["height"], r["bitrate"])

    def __transcode_480p(self) -> bytes:
        r = RENDITIONS["480p"]
        return self.__transcode_rendition(r["width"], r["height"], r["bitrate"])

    def __transcode_720p(self) -> bytes:
        r = RENDITIONS["720p"]
        return self.__transcode_rendition(r["width"], r["height"], r["bitrate"])

    def __transcode_1080p(self) -> bytes:
        r = RENDITIONS["1080p"]
        return self.__transcode_rendition(r["width"], r["height"], r["bitrate"])

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_transcoder(self) -> dict[str, bytes]:
        """Transcode the input file into all renditions.

        Extracts audio once, then for each rendition:
          1. Runs the two-process piped transcode.
          2. Merges the result with the audio stream.

        Returns:
            Dict mapping rendition name (e.g. "720p") to merged MP4 bytes.
        """
        audio_bytes = self.__extract_audio()

        results: dict[str, bytes] = {}
        for name, method in [
            ("360p", self.__transcode_360p),
            ("480p", self.__transcode_480p),
            ("720p", self.__transcode_720p),
            ("1080p", self.__transcode_1080p),
        ]:
            video_bytes = method()
            merged = self.__merge_video_audio(video_bytes, audio_bytes)
            results[name] = merged
            logger.info("Transcoded %s (%d bytes)", name, len(merged))

        return results
