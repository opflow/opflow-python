#!/usr/bin/env python

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/..')

import opflow

params = {
	'uri': 'amqp://master:zaq123edcx@192.168.56.56/',
	'exchange_name': 'direct_logs',
	'routing_key': 'routing_key'
}

engine = engine.Engine(params)

message = sys.argv[1] or 'Hello World!'

print(" [x] Sent %r" % (message))

engine.produce(message)

engine.close()
