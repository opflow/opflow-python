#!/usr/bin/env python

import json
import os
import signal
import sys
import time
import threading

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/..')

from fibonacci_generator import FibonacciGenerator
import opflow

subscriber = opflow.PubsubHandler(**{
    'uri': 'amqp://master:zaq123edcx@192.168.56.56/',
    'exchangeName': 'tdd-opflow-publisher',
    'routingKey': 'tdd-opflow-pubsub-public',
    'subscriberName': 'tdd-opflow-subscriber',
    'recyclebinName': 'tdd-opflow-recyclebin',
    'applicationId': 'FibonacciGenerator'
})

def listener(body, headers):
    data = json.loads(body)
    print("[x] input: %s" % (data))
    if (data['number'] == 30): raise Exception()
    fg = FibonacciGenerator(data['number'])
    print("[-] result: %s" % json.dumps(fg.finish()))

subscriber.subscribe(listener)

print(' [*] Waiting for message. To exit press CTRL+C')

def signal_term_handler(signal, frame):
    print 'SIGTERM/SIGINT'
    subscriber.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_term_handler)
signal.signal(signal.SIGTERM, signal_term_handler)

subscriber.retain()

print(' [*] Exit!')
