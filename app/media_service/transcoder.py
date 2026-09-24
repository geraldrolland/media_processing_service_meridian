"""Media transcoding module using ffmpeg piped processes."""

import logging
import os
import subprocess

from app.config import settings
from app.utils import get_video_framerate

logger = logging.getLogger(__name__)


class MediaTranscoder:
    """Transcodes a video file into multiple CMAF .m4s renditions using ffmpeg.

    Each rendition runs two piped ffmpeg processes:
      Process 1 — reads the input file frame-by-frame (raw RGB24).
      Process 2 — transcodes the raw frames to H.264 and outputs .m4s bytes.

    Audio is extracted as a separate .m4s stream.
    """

    def __init__(self, input_file: str, output_dir: str, codec: str = "libx264"):
        self.input_file = input_file
        self.codec = codec
        self.output_dir = output_dir

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def __extract_audio(self) -> bytes:
        """Extract the audio stream from the input file (copy, no re-encode)."""
        process = subprocess.Popen(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-i", self.input_file,
                "-vn", "-acodec", "copy",
                "-f", "mp4", "-movflags",
                "+cmaf+dash+frag_keyframe+empty_moov",
                "pipe:1",
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
        fps = get_video_framerate(self.input_file)

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
                "-movflags", "+cmaf+dash+frag_keyframe+empty_moov",
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

    # ------------------------------------------------------------------
    # Private rendition methods
    # ------------------------------------------------------------------

    def __transcode_360p(self) -> bytes:
        r = settings.renditions["360p"]
        return self.__transcode_rendition(r["width"], r["height"], r["bitrate"])

    def __transcode_480p(self) -> bytes:
        r = settings.renditions["480p"]
        return self.__transcode_rendition(r["width"], r["height"], r["bitrate"])

    def __transcode_720p(self) -> bytes:
        r = settings.renditions["720p"]
        return self.__transcode_rendition(r["width"], r["height"], r["bitrate"])

    def __transcode_1080p(self) -> bytes:
        r = settings.renditions["1080p"]
        return self.__transcode_rendition(r["width"], r["height"], r["bitrate"])

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_transcoder(self) -> list[str]:
        """Transcode the input file into all renditions and save to disk.

        Transcodes each video rendition and extracts audio, then saves
        all results to:
            <output_dir>/<video_id>/<rendition>/<segment_stem>.m4s

        video_id is extracted from the input file's parent directory.

        Returns:
            List of saved file paths.
        """
        audio_bytes = self.__extract_audio()

        results: dict[str, bytes] = {}
        for name, method in [
            ("360p", self.__transcode_360p),
            ("480p", self.__transcode_480p),
            ("720p", self.__transcode_720p),
            ("1080p", self.__transcode_1080p),
        ]:
            results[name] = method()
            logger.info("Transcoded %s (%d bytes)", name, len(results[name]))

        results["audio"] = audio_bytes
        logger.info("Extracted audio (%d bytes)", len(audio_bytes))

        video_id = os.path.basename(os.path.dirname(self.input_file))
        return self.save_renditions(results, video_id)

    def save_renditions(
        self, results: dict[str, bytes], video_id: str
    ) -> list[str]:
        """Save transcoded rendition bytes to disk.

        Writes each entry to:
            <output_dir>/<video_id>/<rendition>/<segment_stem>.m4s

        Args:
            results: Dict mapping rendition name to .m4s bytes.
            video_id: The video ID used as the top-level directory.

        Returns:
            List of saved file paths.
        """
        segment_stem = os.path.splitext(
            os.path.basename(self.input_file)
        )[0]
        saved_files: list[str] = []

        for rendition, data in results.items():
            out_dir = os.path.join(
                self.output_dir, video_id, rendition
            )
            os.makedirs(out_dir, exist_ok=True)

            out_path = os.path.join(out_dir, f"{segment_stem}.m4s")
            with open(out_path, "wb") as f:
                f.write(data)
            saved_files.append(out_path)
            logger.info(
                "Saved %s → %s (%d bytes)",
                rendition, out_path, len(data),
            )

        return saved_files
