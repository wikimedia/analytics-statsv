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
import re
import socket
import urlparse

from kafka import KafkaClient, SimpleConsumer


logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                    format='%(asctime)s %(message)s')
supported_metric_types = ('c', 'ms')
statsd_addr = ('statsd.eqiad.wmnet', 8125)
statsd_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
kafka = KafkaClient((
    'kafka1012.eqiad.wmnet',
    'kafka1013.eqiad.wmnet',
    'kafka1014.eqiad.wmnet',
    'kafka1018.eqiad.wmnet',
    'kafka1020.eqiad.wmnet',
    'kafka1022.eqiad.wmnet',
))

consumer = SimpleConsumer(kafka, 'statsv', 'statsv')
for message in consumer:
    data = json.loads(message.message.value)
    try:
        query_string = data['uri_query'].lstrip('?')
        for metric_name, value in urlparse.parse_qsl(query_string):
            metric_value, metric_type = re.search('(\d+)(\D+)', value).groups()
            assert metric_type in supported_metric_types
            statsd_message = '%s:%s|%s' % (metric_name, metric_value, metric_type)
            statsd_sock.sendto(statsd_message.encode('utf-8'), statsd_addr)
            logging.debug(statsd_message)
    except (AssertionError, AttributeError, KeyError):
        pass
