import subprocess
import os

class GenerateThumbnail:
    def __init__(self, video_path: str, output_dir: str, thumbnail_prefix: str):
        self.video_path = video_path
        self.output_dir = output_dir
        self.thumbnail_prefix = thumbnail_prefix

    def generate_thumbnail(self) -> str:
        """Generate a thumbnail image from the video using ffmpeg.

        Returns:
            Path to the generated thumbnail image.
        """
        thumbnail_path = os.path.join(self.output_dir, f"{self.thumbnail_prefix}.jpg")
        command = [
            "ffmpeg",
            "-i", self.video_path,
            "-ss", "00:00:01.000",
            "-vframes", "1",
            thumbnail_path
        ]
        subprocess.run(command, check=True)

        return thumbnail_path
