import argparse
import ast
import cv2
import ffmpeg
import logging
import numpy as np
import os
import pyinotify
import signal
import sys
from libreeye.algorithms import motion

_logger = logging.getLogger(__name__)


class Listener:
    class _EventHandler(pyinotify.ProcessEvent):
        def __init__(self, handler):
            super().__init__()
            self.handler = handler

        def process_IN_CLOSE_WRITE(self, event):
            if os.path.isfile(event.pathname):
                self.handler(event.pathname)

    def __init__(self, handler):
        self._wm = pyinotify.WatchManager()
        self._mask = pyinotify.IN_CLOSE_WRITE
        self._notifier = pyinotify.ThreadedNotifier(
            self._wm, Listener._EventHandler(handler)
        )

    def add_path(self, path: str):
        self._wm.add_watch(path, self._mask, rec=False)

    def start(self):
        self._notifier.start()

    def stop(self):
        self._notifier.stop()
        self._notifier.join()
        _logger.debug('Notifier thread finished')


def build_iterator(path: str, scale=1.0):
    video_attr = ffmpeg.probe(path)['streams'][0]
    width = int(video_attr['width'] * scale)
    height = int(video_attr['height'] * scale)
    f_rate = ast.parse(video_attr['r_frame_rate'], mode='eval').body
    fps = f_rate.left.n // f_rate.right.n
    # Open video with ffmpeg
    ffmpeg_pipeline = ffmpeg.input(path, v='quiet')
    # Apply framestep filter to reduce fps down to 1
    ffmpeg_pipeline = ffmpeg_pipeline.filter('framestep', step=fps)
    # Apply scale filter if necessary
    if scale < 1:
        ffmpeg_pipeline = ffmpeg_pipeline.filter(
            'scale',
            width=width,
            height=height
        )
    # Create piped output
    ffmpeg_pipeline = ffmpeg_pipeline.output(
        'pipe:',
        format='rawvideo',
        pix_fmt='gray8'
    )
    # Loop
    process = ffmpeg_pipeline.run_async(pipe_stdout=True)
    while True:
        in_bytes = process.stdout.read(height * width)
        if not in_bytes:
            break
        frame = (
            np
            .frombuffer(in_bytes, np.uint8)
            .reshape([height, width])
        )
        yield frame


def run_motion_algorithm(path: str, scale: float, threshold: float,
                         min_area: float, cooldown: int):
    _logger.debug(path)
    # Run the motion detection algorithm
    events = motion.basic_motion(
        lambda: build_iterator(path, scale),
        threshold,
        min_area,
        cooldown,
        False
    )
    # Write detected frames with motion as pictures
    events_path = os.path.join(os.path.dirname(path), 'motion')
    if not os.path.isdir(events_path):
        os.makedirs(events_path, 0o755)
    for (n, frame) in events:
        image_path = os.path.join(
            events_path,
            f'{os.path.basename(os.path.splitext(path)[0])}-{n}.jpg'
        )
        _logger.debug('Writting image %s', image_path)
        cv2.imwrite(image_path, frame)
    _logger.debug('Completed execution for file %s', path)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--scale', type=float)
    parser.add_argument('--threshold', type=float)
    parser.add_argument('--min-area', type=float)
    parser.add_argument('--cooldown', type=int)
    parser.add_argument('paths', metavar='PATH', type=str, nargs='+')
    return parser.parse_args()


if __name__ == '__main__':
    # Configure logger
    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(asctime)s] %(filename)s:%(lineno)d %(message)s',
        datefmt='%d/%m %H:%M:%S',
        stream=sys.stderr
    )
    # Parse arguments
    args = parse_args()
    # Listen for new videos inside the paths provided
    l = Listener(
        lambda p: run_motion_algorithm(
            p,
            args.scale,
            args.threshold,
            args.min_area,
            args.cooldown
        )
    )
    for p in args.paths:
        l.add_path(p)
    # Configure signal handler to exit execution
    signal.signal(signal.SIGTERM, lambda *args: l.stop())
    # Start loop until interrupted by signal
    l.start()
