# -*- coding: utf-8 -*-
"""

    Python logging tuned to Я.ЩёлкнитеДом

"""
import logging
import os
import sys
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
    def __init__(self, url, **kwargs):
        self.extra = kwargs.get('extra', {})
        self.url = url
        logging.Handler.__init__(self, level=kwargs.get('level', logging.NOTSET))

    def emit(self, record):
        try:
            self.format(record)
            return self._emit(record)
        except Exception:
            print("Top level ClickHouse exception caught - failed creating log record", file=sys.stderr)
            print(str(record.msg), file=sys.stderr)
            print(str(traceback.format_exc()), file=sys.stderr)
            sys.exit(5)

    def _emit(self, record):
        data, extra = extract_extra(record)

        date = datetime.utcfromtimestamp(record.created)

        data['ts'] = str(date)[:-7]
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

        logging.getLogger('clickhouse_driver.connection').setLevel(logging.CRITICAL)

        logging.getLogger("requests").setLevel(logging.ERROR)
        logging.getLogger("urllib3").setLevel(logging.ERROR)

        # requests.post(f'{self.url}?query=Insert+into+Log+FORMAT+JSONEachRow&input_format_skip_unknown_fields=1', json=data)

        #  instead, use sockets   1) for speed and  2) because requests use logging itself
        import socket, json
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        url = self.url.split('//')[1]
        host, port = url.split(':')
        s.connect((host, int(port)))
        json_data = json.dumps(data, separators=(',', ':'))
        s.send(bytes(
            "POST /?query=Insert+into+Log+FORMAT+JSONEachRow&input_format_skip_unknown_fields=1 HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"Content-Length: {len(json_data)}\r\nConnection: keep-alive\r\nContent-Type: application/json\r\n\r\n{json_data}\r\n", 'utf8'))
        # response = s.recv(4096)
        # print(response)
