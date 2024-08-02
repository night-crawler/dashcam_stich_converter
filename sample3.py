import enum
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import cv2
import ffmpeg
from vidgear.gears import WriteGear
import moviepy.editor as mp


class VideoType(enum.Enum):
    NORMAL = 'N'
    EVENT = 'E'
    PARKING = 'P'


class Position(enum.Enum):
    FRONT = 'A'
    INTERNAL = 'B'
    REAR = 'C'


@dataclass
class VideoFileInfo:
    ts: datetime
    id: int
    video_type: VideoType
    position: Position
    file: Path

    @classmethod
    def from_path(cls, path: Path):
        # 20240703_131044_0417_N_A.MP4
        date, time, pk, video_type, position = path.stem.split('_')
        pk = int(pk)

        date = datetime.strptime(date, '%Y%m%d')
        time = datetime.strptime(time, '%H%M%S').time()
        ts = datetime.combine(date, time)

        video_type = VideoType(video_type)
        position = Position(position)

        return cls(ts, pk, video_type, position, path)


@dataclass
class FileGroup:
    ts: datetime
    pk: int
    files: list[VideoFileInfo]

    def get_by_position(self, position: Position):
        return next(file for file in self.files if file.position == position)


def parse_files(source: Path):
    files = source.glob('*.mp4', case_sensitive=False)

    groups = defaultdict(list)

    for file in sorted(files):
        vf = VideoFileInfo.from_path(file)
        groups[vf.id].append(vf)

    file_groups = []
    for pk, files in groups.items():
        fg = FileGroup(files[0].ts, pk, files)
        file_groups.append(fg)

    file_groups.sort(key=lambda fg: fg.pk)

    return file_groups


def stitch_group(group: FileGroup):
    front = str(group.get_by_position(Position.FRONT).file)
    internal = str(group.get_by_position(Position.INTERNAL).file)
    rear = str(group.get_by_position(Position.REAR).file)

    if not Path('./out').exists():
        Path('./out').mkdir()

    output_file = f"./out/output_{group.pk}.mp4"

    # Read video streams
    front_stream = cv2.VideoCapture(front)
    internal_stream = cv2.VideoCapture(internal)
    rear_stream = cv2.VideoCapture(rear)

    fps = front_stream.get(cv2.CAP_PROP_FPS)

    # Prepare output video writer
    output_params = {"-input_framerate": fps}
    writer = WriteGear(output=output_file, compression_mode=True, logging=True, **output_params)

    while True:
        ret1, frame1 = front_stream.read()
        ret2, frame2 = internal_stream.read()
        ret3, frame3 = rear_stream.read()

        if not (ret1 and ret2 and ret3):
            break

        # Resize frames
        # frame1_resized = cv2.resize(frame1, (3840, 2160))
        # frame2_resized = cv2.resize(frame2, (1920, 1080))
        # frame3_resized = cv2.resize(frame3, (1920, 1080))

        # Create the final stitched frame
        top_right = frame2
        bottom_right = frame3
        right_combined = cv2.vconcat([top_right, bottom_right])
        final_frame = cv2.hconcat([frame1, right_combined])

        # Write the frame to the output video
        writer.write(final_frame)

    # Release everything
    front_stream.release()
    internal_stream.release()
    rear_stream.release()
    writer.close()

    # Combine audio from all input videos
    front_clip = mp.VideoFileClip(front)
    # internal_clip = mp.VideoFileClip(internal)
    # rear_clip = mp.VideoFileClip(rear)

    # final_audio = mp.CompositeAudioClip([front_clip.audio, internal_clip.audio, rear_clip.audio])

    # Attach the audio to the output video
    final_video = mp.VideoFileClip(output_file)
    final_video = final_video.set_audio(front_clip.audio)
    final_video.write_videofile(f"final_{output_file}", codec="libx264")


if __name__ == "__main__":
    file_groups = parse_files(Path('/work/dashcam/source/'))
    args = []
    for group in file_groups:
        stitch_group(group)

