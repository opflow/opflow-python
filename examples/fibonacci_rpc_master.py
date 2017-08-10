#!/usr/bin/env python

import json
import os
import sys
import time
import signal

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/..')

import opflow

master = opflow.RpcMaster({
	'uri': 'amqp://master:zaq123edcx@192.168.56.56/',
	'exchange_name': 'tdd-opflow-exchange',
	'routing_key': 'tdd-opflow-rpc',
	'responseName': 'tdd-opflow-feedback',
	'applicationId': 'FibonacciGenerator'
})

def signal_term_handler(signal, frame):
    print 'SIGTERM/SIGINT'
    master.close()
    sys.exit(0)
 
signal.signal(signal.SIGINT, signal_term_handler)
signal.signal(signal.SIGTERM, signal_term_handler)

message = json.dumps({ 'number': 25 })
print(" [x] Sent %r" % (message))

req = master.request('fibonacci', message)
while req.hasNext():
	print ' [-] message: %r' % req.next()

print(' [*] Waiting for message. To exit press CTRL+C')

master.close()

print(' [*] Exit!')