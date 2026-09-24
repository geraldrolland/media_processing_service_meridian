import os
import subprocess
import glob

class Segmentation:
    def __init__(self, video_path: str, output_dir: str, seg_prefix: str, seg_duration: int):
        self.video_path = video_path
        self.output_dir = output_dir
        self.seg_prefix = seg_prefix
        self.seg_duration = seg_duration

    def generate_segments(self) -> list[str]:
        """Generate video segments using ffmpeg.

        Returns:
            List of segment file paths.
        """
        segment_pattern = os.path.join(self.output_dir, f"{self.seg_prefix}%d.mp4")
        command = [
            "ffmpeg",
            "-i", self.video_path,
            "-c:v", "libx264",
            "-c:a", "aac",
            "-f", "segment",
            "-segment_time", str(self.seg_duration),
            "-segment_start_number", "1",
            "-reset_timestamps", "1",            
            segment_pattern
        ]
        subprocess.run(command, check=True)

        # Return list of generated segment files
        return sorted(glob.glob(os.path.join(self.output_dir, f"{self.seg_prefix}*.mp4")))
