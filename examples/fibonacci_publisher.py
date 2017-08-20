#!/usr/bin/env python

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/..')

from fibonacci_generator import FibonacciGenerator
import opflow

publisher = opflow.PubsubHandler(**{
    'uri': 'amqp://master:zaq123edcx@192.168.56.56/',
    'exchangeName': 'tdd-opflow-publisher',
    'routingKey': 'tdd-opflow-pubsub-public',
    'applicationId': 'FibonacciGenerator'
})

if len(sys.argv) < 2:
    for i in range(20, 40):
        publisher.publish(json.dumps({ 'number': i }))
else:
    number = int(sys.argv[1])
    print '[+] number: %s' % number
    publisher.publish(json.dumps({ 'number': number }))

print(' [*] Exit!')
