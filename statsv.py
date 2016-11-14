# -*- coding: utf-8 -*-
"""
  statsv
  ~~~~~~
  A simple web request -> kafka -> statsd gateway.

  Copyright 2014 Ori Livneh <ori@wikimedia.org>

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

"""
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import json
import logging
import multiprocessing
import os
import re
import socket
import urlparse

from kafka import KafkaConsumer

logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                    format='%(asctime)s %(message)s')

# Set kafka module logging level to INFO
logging.getLogger("kafka").setLevel(logging.INFO)


supported_metric_types = ('c', 'g', 'ms')
statsd_addr = ('statsd.eqiad.wmnet', 8125)

# TODO: make these configurable.
TIMEOUT_SECONDS = 60
kafka_topic = 'statsv'
kafka_consumer_group = 'statsv'
kafka_bootstrap_servers = (
    'kafka1012.eqiad.wmnet:9092',
    'kafka1013.eqiad.wmnet:9092',
    'kafka1014.eqiad.wmnet:9092',
    'kafka1018.eqiad.wmnet:9092',
    'kafka1020.eqiad.wmnet:9092',
    'kafka1022.eqiad.wmnet:9092',
)

SOCK_CLOEXEC = getattr(socket, 'SOCK_CLOEXEC', 0x80000)

class Watchdog:
    """
    Simple notifier for systemd's process watchdog.

    You can use this in message- or request-processing scripts that are
    managed by systemd and that are under constant load, where the
    absence of work is an abnormal condition.

    Make sure the unit file contains `WatchdogSec=1` (or some other
    value) and `Restart=always`. Then you can write something like:

        watchdog = Watchdog()
        while 1:
            handle_request()
            watchdog.notify()

    This way, if the script spends a full second without handling a
    request, systemd will restart it.

    See https://www.freedesktop.org/software/systemd/man/systemd.service.html#WatchdogSec=
    for more details about systemd's watchdog capabilities.
    """

    def __init__(self):
        # Get and clear NOTIFY_SOCKET from the environment to prevent
        # subprocesses from inheriting it.
        self.addr = os.environ.pop('NOTIFY_SOCKET', None)
        if not self.addr:
            self.sock = None
            return

        # If the first character of NOTIFY_SOCKET is "@", the string is
        # understood as an abstract socket address.
        if self.addr.startswith('@'):
            self.addr = '\0' + self.addr[1:]

        self.sock = socket.socket(
            socket.AF_UNIX, socket.SOCK_DGRAM | SOCK_CLOEXEC)

    def notify(self):
        if not self.sock:
            return
        self.sock.sendto(b'WATCHDOG=1', self.addr)


def process_queue(q):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while 1:
        raw_data = q.get()
        try:
            data = json.loads(raw_data)
        except:
            logging.exception(raw_data)
        try:
            query_string = data['uri_query'].lstrip('?')
            for metric_name, value in urlparse.parse_qsl(query_string):
                metric_value, metric_type = re.search(
                        '^(\d+)([a-z]+)$', value).groups()
                assert metric_type in supported_metric_types
                statsd_message = '%s:%s|%s' % (
                        metric_name, metric_value, metric_type)
                sock.sendto(statsd_message.encode('utf-8'), statsd_addr)
                logging.debug(statsd_message)
        except (AssertionError, AttributeError, KeyError):
            pass

queue = multiprocessing.Queue()

# Spawn either half as many workers as there are CPU cores.
# On single-core machines, spawn a single worker.
worker_count = max(1, multiprocessing.cpu_count() // 2)

for _ in range(worker_count):
    worker = multiprocessing.Process(target=process_queue, args=(queue,))
    worker.daemon = True
    worker.start()

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    group_id=kafka_consumer_group,
    auto_offset_reset='latest',
    # statsd metrics don't make sense if they lag,
    # so disable commits to avoid resuming at historical committed offset.
    enable_auto_commit=False,
    consumer_timeout_ms=TIMEOUT_SECONDS * 1000,
)

watchdog = Watchdog()

try:
    for message in consumer:
        if message is not None:
            queue.put(message.value)
            watchdog.notify()
    # If we reach this line, TIMEOUT_SECONDS elapsed with no events received.
    raise RuntimeError('No messages received in %d seconds.' % TIMEOUT_SECONDS)
except Exception as e:
    logging.exception("Caught exception, aborting.")
finally:
    queue.close()
    consumer.close()
