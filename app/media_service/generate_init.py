"""CMAF init segment generation module using ffmpeg."""

import logging
import os
import subprocess

from app.config import settings
from app.utils import get_video_framerate

logger = logging.getLogger(__name__)


class GenerateInit:
    """Generates CMAF init segments for each representation.

    Produces <representation>-init.mp4 files containing the ftyp + moov
    boxes required by DASH/HLS players before media segments can be consumed.
    """

    def __init__(self, input_file: str, output_dir: str, representation: list[str]):
        self.input_file = input_file
        self.output_dir = output_dir
        self.representation = representation

    def __generate_audio_init(self, out_path: str) -> None:
        """Generate a CMAF init segment for the audio stream."""
        process = subprocess.Popen(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-i", self.input_file,
                "-map", "0:a",
                "-c:a", "aac",
                "-movflags", "+cmaf+dash+frag_keyframe+empty_moov",
                "-f", "mp4", "-t", "0",
                out_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _, stderr = process.communicate()
        if process.returncode != 0:
            raise RuntimeError(
                f"Audio init segment failed "
                f"(rc={process.returncode}): {stderr.decode()}"
            )

    def generate_init_file(self) -> list[str]:
        """Generate CMAF init segments for each representation and audio.

        Uses the same encoding settings as MediaTranscoder (libx264, yuv420p,
        CMAF movflags) and outputs -t 0 to produce only the init segment.

        Returns:
            List of paths to the generated init files (video renditions + audio).
        """
        fps = get_video_framerate(self.input_file)
        os.makedirs(self.output_dir, exist_ok=True)

        init_paths: list[str] = []
        for rep in self.representation:
            r = settings.renditions[rep]
            rep_dir = os.path.join(self.output_dir, rep)
            os.makedirs(rep_dir, exist_ok=True)
            out_path = os.path.join(rep_dir, "init.mp4")

            process = subprocess.Popen(
                [
                    "ffmpeg", "-hide_banner", "-loglevel", "error",
                    "-i", self.input_file,
                    "-map", "0:v",
                    "-c:v", "libx264", "-b:v", r["bitrate"],
                    "-s", f"{r['width']}x{r['height']}",
                    "-r", str(fps),
                    "-pix_fmt", "yuv420p",
                    "-movflags", "+cmaf+dash+frag_keyframe+empty_moov",
                    "-f", "mp4", "-t", "0",
                    out_path,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            _, stderr = process.communicate()
            if process.returncode != 0:
                raise RuntimeError(
                    f"Init segment failed for {rep} "
                    f"(rc={process.returncode}): {stderr.decode()}"
                )
            init_paths.append(out_path)
            logger.info("Generated init segment → %s", out_path)

        # Generate audio init segment
        audio_dir = os.path.join(self.output_dir, "audio")
        os.makedirs(audio_dir, exist_ok=True)
        audio_path = os.path.join(audio_dir, "init.mp4")
        self.__generate_audio_init(audio_path)
        init_paths.append(audio_path)
        logger.info("Generated audio init segment → %s", audio_path)

        return init_paths
