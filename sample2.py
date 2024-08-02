import enum
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import ffmpeg


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
    front = group.get_by_position(Position.FRONT).file
    internal = group.get_by_position(Position.INTERNAL).file
    rear = group.get_by_position(Position.REAR).file

    if not Path('./out').exists():
        Path('./out').mkdir()

    front_input = ffmpeg.input(str(front))

    vstacked = ffmpeg.filter([ffmpeg.input(str(internal)), ffmpeg.input(str(rear))], 'vstack')
    hstacked = ffmpeg.filter([front_input, vstacked], 'hstack')
    audio = front_input.audio.filter('loudnorm')

    return hstacked, audio


if __name__ == "__main__":
    file_groups = parse_files(Path('/work/dashcam/source/'))
    args = []
    for group in file_groups:
        video, audio = stitch_group(group)
        args.append(video)
        args.append(audio)

    # ffmpeg.output(*args, './bla.mp4').run(overwrite_output=True)
    q = ffmpeg.output(*args, './bla.mp4').compile()
    print(q)
