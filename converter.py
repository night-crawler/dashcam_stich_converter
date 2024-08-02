#! /usr/bin/env python3

import enum
import subprocess
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import click
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
    front = str(group.get_by_position(Position.FRONT).file)
    internal = str(group.get_by_position(Position.INTERNAL).file)
    rear = str(group.get_by_position(Position.REAR).file)

    if not Path('./out').exists():
        Path('./out').mkdir()

    output_file = Path(f"./out/output_{group.pk}.mp4")

    vstacked = ffmpeg.filter([ffmpeg.input(internal), ffmpeg.input(rear)], 'vstack')
    hstacked = ffmpeg.filter([ffmpeg.input(front), vstacked], 'hstack')
    audio = ffmpeg.input(front).audio.filter('loudnorm')

    ffmpeg.output(hstacked, audio, str(output_file)).run(overwrite_output=True)

    return output_file


def stitch_all(groups: list[FileGroup], max_workers=5):
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for group in groups:
            future = executor.submit(stitch_group, group)
            futures.append((group, future))

    for group, future in futures:
        try:
            yield group, future.result()
        except Exception as e:
            print(f'Error stitching group {group}: {e}')
            raise e


def create_file_list(file_paths, list_filename: Path):
    with list_filename.open('w') as f:
        for path in file_paths:
            f.write(f"file '{path.absolute()}'\n")


def combine_clips(output_files: list[Path], final_output_file: Path = Path('out.mp4')):
    temp_file_list = Path('file_list.txt')
    create_file_list(output_files, temp_file_list)
    try:
        command = [
            'ffmpeg', '-y', '-f', 'concat', '-safe', '0', '-i', str(temp_file_list),
            '-c', 'copy', final_output_file
        ]
        subprocess.run(command, check=True)
    except Exception as e:
        print(f'Error combining clips: {e}')
        raise e
    finally:
        temp_file_list.unlink()


@click.group()
def cli():
    pass


@cli.command()
@click.option("-p", "--parallelism", default=2, help="Number of parallel ffmpeg processes to run.")
@click.option("-s", "--src", prompt="Source path", help="Path to the source directory.")
@click.option("-d", "--dst", prompt="Destination path", help="Path to the destination directory.")
def stitch(parallelism, src, dst):
    src = Path(src)
    assert src.exists(), f"Source path {src} does not exist."
    assert src.is_dir(), f"Source path {src} is not a directory."

    dst = Path(dst)
    if not dst.exists():
        dst.mkdir()

    file_groups = parse_files(src)
    stitched_files = []
    for group, stitched_file in stitch_all(file_groups, max_workers=parallelism):
        print(f'Stitched group {group}: {stitched_file}')
        stitched_files.append(stitched_file)


@cli.command()
@click.option("-s", "--src", prompt="Source path", help="Path to the source directory.")
@click.option("-d", "--dst", prompt="Destination path", help="Path to the destination directory.")
def combine(src, dst):
    src = Path(src)
    assert src.exists(), f"Source path {src} does not exist."
    assert src.is_dir(), f"Source path {src} is not a directory."

    files = src.glob('*.mp4')
    combine_clips(sorted(files), dst)


if __name__ == "__main__":
    cli()
