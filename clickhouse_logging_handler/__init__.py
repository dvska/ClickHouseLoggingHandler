# -*- coding: utf-8 -*-
"""

    Python logging tuned to Я.ЩёлкнитеДом

"""
import json
import logging
import os
import socket
import sys
import time
import traceback
from datetime import datetime

RESERVED = frozenset(
    ('stack', 'name', 'module', 'funcName', 'args', 'msg', 'levelno', 'exc_text', 'exc_info', 'data', 'created', 'levelname', 'msecs',
     'relativeCreated', 'tags', 'message'))

CONTEXTUAL = frozenset(('user', 'culprit', 'server_name', 'fingerprint'))


def extract_extra(record, reserved=RESERVED, contextual=CONTEXTUAL):
    data = {}

    extra = getattr(record, 'data', None)
    if not isinstance(extra, dict):
        if extra:
            extra = {'data': extra}
        else:
            extra = {}
    else:
        # record.data may be something we don't want to mutate to not cause unexpected side effects
        extra = dict(extra)

    for k, v in record.__dict__.items():
        if k in reserved:
            continue
        if k.startswith('_'):
            continue
        if '.' not in k and k not in contextual:
            extra[k] = v
        else:
            data[k] = v

    return data, extra


class ClickHouseLoggingHandler(logging.Handler):
    BUFFER_SIZE = 1000  # records  # TODO into config
    BUFFER_FLUSHTIME = 20  # secs

    last_flushed = time.time()

    def __init__(self, url, **kwargs):
        self.extra = kwargs.get('extra', {})
        self.url = url
        self.buffer = []
        logging.Handler.__init__(self, level=kwargs.get('level', logging.NOTSET))

    def emit(self, record):
        try:
            self.format(record)
            return self._emit(record)
        except Exception:
            print(f"Top level ClickHouse exception caught - failed creating log record @ {self.url}", file=sys.stderr)
            print(str(record.msg), file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
            time.sleep(5)

    def _emit(self, record):
        data, extra = extract_extra(record)

        date = str(datetime.utcfromtimestamp(record.created))

        data['ts'] = date[:-7]
        data['msec'] = date[-6:]
        data['level'] = record.levelname
        data['logger'] = record.name
        data['pid'] = extra['process']
        data['procname'] = extra['processName']
        data['file'] = f"{extra['filename']}:{extra['lineno']}"

        data['body'] = record.msg if isinstance(record.msg, str) else str(record.msg)
        data['jobid'] = os.environ.get('theapp_job_id', 0) or 0
        data['uid'] = os.environ.get('theapp_user_id', 0) or 0
        data['type'] = os.environ.get('theapp_job_type', '')
        data['plug'] = os.environ.get('theapp_plug_action', '')

        # requests.post(f'{self.url}?query=Insert+into+Log+FORMAT+JSONEachRow&input_format_skip_unknown_fields=1', json=data)

        #  instead, use sockets   1) for speed and  2) because requests use logging itself

        if extra.get('force_flush') or (time.time() - self.last_flushed > self.BUFFER_FLUSHTIME
                            or len(self.buffer) >= self.BUFFER_SIZE
                            or record.levelno > logging.ERROR):
            json_bulk_data = '\n'.join(self.buffer) + '\n'

            try:
                # requests.post(f'{self.url}?query=Insert+into+Log+FORMAT+JSONEachRow&input_format_skip_unknown_fields=1', json=data)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                url = self.url.split('//')[1]
                host, port = url.split(':')
                s.connect((host, int(port)))
                s.send(bytes(
                    "POST /?query=Insert+into+Log_buffer+FORMAT+JSONEachRow&input_format_skip_unknown_fields=1 HTTP/1.1\r\n"
                    f"Host: {host}\r\n"
                    f"Content-Length: {len(json_bulk_data)}\r\nConnection: keep-alive\r\nContent-Type: application/json\r\n\r\n{json_bulk_data}\r\n", 'utf8'))
                # response = s.recv(4096);
                # print(response.decode('utf8'))
                # if b'400 Bad Request' in response:
                #     print(json_data)
                # s.close()
            except:
                with open('log_records_unsent_to_clickhouse.JSONEachRow', 'a') as json_log:
                    json_log.write(json_bulk_data + '\n')
                    raise
            finally:
                self.buffer = []
                self.last_flushed = time.time()
