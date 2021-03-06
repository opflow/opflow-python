#!/usr/bin/env python

import json
import os
import sys
import time
import signal
import threading

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/..')

import opflow

master = opflow.RpcMaster(**{
	'uri': 'amqp://master:zaq123edcx@192.168.56.56/',
	'exchangeName': 'tdd-opflow-exchange',
	'routingKey': 'tdd-opflow-rpc',
	'responseName': 'tdd-opflow-feedback',
	'applicationId': 'FibonacciGenerator'
})

master.executor

def signal_term_handler(signal, frame):
    print 'SIGTERM/SIGINT'
    master.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_term_handler)
signal.signal(signal.SIGTERM, signal_term_handler)

message = json.dumps({ 'number': 25 })
print(" [x] Sent %r" % (message))

req = master.request('fibonacci', message, {
	'timeout': 10
});

time.sleep(6)

while req.hasNext():
	print ' [-] message: %r' % req.next()

time.sleep(8)

print(' [*] Waiting for message. To exit press CTRL+C')

for t in threading.enumerate():
	print "Thread[%s] is %s" % (t.name, t.is_alive())

master.close()

print(' [*] Exit!')

signal.pause()

# master.retain()