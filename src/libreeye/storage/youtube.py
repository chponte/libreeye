# This file is part of Libreeye.
# Copyright (C) 2019 by Christian Ponte
#
# Libreeye is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Libreeye is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Libreeye. If not, see <http://www.gnu.org/licenses/>.

from datetime import datetime, timedelta, timezone
import grp
import json
import logging
import os
import pprint
import pwd
import stat
import threading
import time

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
import ffmpeg
import googleapiclient.discovery
import googleapiclient.errors

from libreeye.storage.base import Storage, Item, Writer
from libreeye.utils.config import YoutubeStorageConfig

_logger = logging.getLogger(__name__)
_scopes = ['https://www.googleapis.com/auth/youtube.force-ssl']
_api_service_name = 'youtube'
_api_version = 'v3'


class YoutubeStorage(Storage):
    def __init__(self, config: YoutubeStorageConfig):
        self._segment_length = config.segment_length()
        self._expiration = config.expiration()
        self._secrets_file = '/etc/libreeye/secrets/youtube_api.json'
        self._credentials_file = '/etc/libreeye/secrets/youtube_creds.json'

    def oauth_login(self):
        # Check secrets file permissions
        st = os.stat(self._secrets_file)
        if (st.st_mode & (stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)):
            raise PermissionError(
                f'File {self._secrets_file} can be accessed by everyone')
        # Auth user
        flow = InstalledAppFlow.from_client_secrets_file(
            self._secrets_file, _scopes)
        credentials = flow.run_console()
        # Save credentials
        with open(self._credentials_file, 'w') as f:
            f.write(credentials.to_json())
        # Change file ownership
        uid = pwd.getpwnam('root').pw_uid
        gid = grp.getgrnam('libreeye').gr_gid
        os.chown(self._credentials_file, uid, gid)
        # Change file permissions
        os.chmod(self._credentials_file, stat.S_IRUSR | stat.S_IRGRP)

    def _build_credentials(self):
        # Check credentials file permissions
        st = os.stat(self._credentials_file)
        if (st.st_mode & (stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)):
            raise PermissionError(
                f'File {self._credentials_file} can be accessed by everyone')
        # Build Credentials object
        with open(self._credentials_file, 'r') as f:
            return Credentials(**json.loads(f.read()))

    def list_expired(self):
        if self._expiration <= 0:
            return []
        expired = []
        credentials = self._build_credentials()
        youtube = googleapiclient.discovery.build(
            _api_service_name,
            _api_version,
            credentials=credentials,
            cache_discovery=False)
        more_pages = True
        page_token = None
        due_date = datetime.utcnow() - timedelta(days=self._expiration)
        while more_pages:
            response = youtube.liveBroadcasts().list(
                part='snippet',
                broadcastStatus='completed',
                broadcastType='all',
                pageToken=page_token
            ).execute()
            for br in response['items']:
                date = datetime.fromisoformat(
                    br['snippet']['actualEndTime'][:-1])
                if (date <= due_date):
                    delete_request = youtube.liveBroadcasts().delete(
                        id=br['id']
                    )
                    expired.append(YoutubeItem(br, delete_request))
            if 'nextPageToken' in response:
                page_token = response['nextPageToken']
            else:
                more_pages = False
        return expired

    def create_writer(self, name, camera_config, probe, error_queue):
        return YoutubeWriter(
            name,
            self._segment_length,
            camera_config.output().youtube_ffmpeg_options(),
            self._build_credentials(),
            probe,
            error_queue
        )


class YoutubeItem(Item):
    def __init__(self, item, delete_request):
        self._item = item
        self._delete_request = delete_request

    def get_path(self):
        return self._item['snippet']['title']

    def remove(self):
        _logger.info('Removing broadcast %s', self._item['snippet']['title'])
        self._delete_request.execute()


class YoutubeWriter(Writer):
    def __init__(self, name, segment_length, ffmpeg_opts, credentials, probe,
                 error_queue):
        super().__init__()
        self._name = name
        self._segment_length = segment_length
        self._ffmpeg_output_opts = ffmpeg_opts
        self._credentials = credentials
        self._ffmpeg_input_format = probe['codec_name']
        self._error_queue = error_queue
        self._bc = None
        self._ls = None
        self._ffmpeg = None
        self._ffmpeg_stdin = None
        self._thread = None
        self._segment_start = 0

    def _init_stream(self):
        _logger.debug('_init_stream called')
        retry = True
        while retry:
            try:
                self._create_broadcast()
                self._ffmpeg_open()
                retry = False
            except googleapiclient.errors.HttpError as e:
                _logger.error(str(e))
                self._error_queue.put(e)
            except OSError as e:
                time.sleep(10)
        self._thread = None

    def _swap_streams(self):
        _logger.debug('_swap_streams called')
        retry = True
        while retry:
            try:
                self._ffmpeg_close()
                self._end_broadcast()
                self._create_broadcast()
                self._ffmpeg_open()
                retry = False
            except googleapiclient.errors.HttpError as e:
                _logger.error(str(e))
                self._error_queue.put(e)
            except OSError as e:
                time.sleep(10)
        self._thread = None

    def _fix_stream(self):
        _logger.debug('_fix_stream called')
        if self._ffmpeg is not None:
            self._ffmpeg.kill()
            self._ffmpeg = None
        self._ffmpeg_open()

    def _create_broadcast(self):
        _logger.debug('_create_broadcast called')
        youtube = googleapiclient.discovery.build(
            _api_service_name,
            _api_version,
            credentials=self._credentials,
            cache_discovery=False)
        if (self._bc is None or
            self._bc['status']['lifeCycleStatus'] in
                ['complete', 'revoked']):
            start_time = datetime.now(timezone.utc).isoformat()
            self._bc = youtube.liveBroadcasts().insert(
                part='snippet,status,contentDetails',
                body={
                    'snippet': {
                        'title': f'{self._name} - {start_time}',
                        'scheduledStartTime': start_time
                    },
                    'status': {
                        'privacyStatus': 'private'
                    },
                    'contentDetails': {
                        'enableAutoStart': True
                    }
                }
            ).execute()
        if self._ls is None:
            self._ls = youtube.liveStreams().insert(
                part='snippet,cdn',
                body={
                    'snippet': {
                        'title': f'{self._name}'
                    },
                    'cdn': {
                        'frameRate': '30fps',
                        'ingestionType': 'rtmp',
                        'resolution': '1080p'
                    }
                }
            ).execute()
        if 'boundStreamId' not in self._bc['contentDetails']:
            self._bc = youtube.liveBroadcasts().bind(
                id=self._bc['id'],
                part='snippet,status,contentDetails',
                streamId=self._ls['id']
            ).execute()
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug('new broadcast information:\n%s',
                          pprint.pformat(self._bc, indent=4))

    def _end_broadcast(self):
        _logger.debug('_end_broadcast called')
        youtube = googleapiclient.discovery.build(
            _api_service_name,
            _api_version,
            credentials=self._credentials,
            cache_discovery=False)
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug('ending broadcast:\n%s',
                          pprint.pformat(self._bc, indent=4))
        if (self._bc is not None and
            self._bc['status']['lifeCycleStatus'] not in
                ['complete', 'revoked']):
            self._bc = youtube.liveBroadcasts().transition(
                id=self._bc['id'],
                broadcastStatus='complete',
                part='snippet,status,contentDetails'
            ).execute()

    def _ffmpeg_open(self):
        _logger.debug('_ffmpeg_open called')
        stream_address = (
            f'{self._ls["cdn"]["ingestionInfo"]["rtmpsIngestionAddress"]}/'
            f'{self._ls["cdn"]["ingestionInfo"]["streamName"]}'
        )
        input_video = ffmpeg.input(
            'pipe:', f=self._ffmpeg_input_format, v='warning'
        ).video
        input_audio = ffmpeg.input(
            'anullsrc=channel_layout=stereo:sample_rate=44100', f='lavfi'
        ).audio
        self._ffmpeg = (
            ffmpeg.output(input_video, input_audio, stream_address,
                          **self._ffmpeg_output_opts)
            .run_async(pipe_stdin=True)
        )
        self._ffmpeg_stdin = self._ffmpeg.stdin
        self._segment_start = time.time()

    def _ffmpeg_close(self):
        _logger.debug('_ffmpeg_close called')
        self._ffmpeg.kill()
        self._ffmpeg_stdin = None
        self._ffmpeg = None

    def write(self, frame) -> None:
        # Check if there is an available ffmpeg input stream
        if self._ffmpeg_stdin is None:
            # Check if there is a thread already working on it
            if self._thread is None:
                self._thread = threading.Thread(target=self._init_stream)
                self._thread.start()
            return
        # Check if segment has ended
        if time.time() - self._segment_start > self._segment_length:
            self._ffmpeg_stdin = None
            self._thread = threading.Thread(target=self._swap_streams)
            self._thread.start()
            return
        # Write frame
        try:
            self._ffmpeg_stdin.write(frame)
        except ConnectionError as e:
            _logger.warning(e.strerror)
            self._ffmpeg_stdin = None
            self._thread = threading.Thread(target=self._fix_stream)
            self._thread.start()

    def close(self):
        if self._ffmpeg is not None:
            self._ffmpeg_close()
            self._end_broadcast()
