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

worker = opflow.RpcWorker({
	'uri': 'amqp://master:zaq123edcx@192.168.56.56/',
	'exchange_name': 'tdd-opflow-exchange',
	'routing_key': 'tdd-opflow-rpc',
	'operatorName': 'tdd-opflow-queue',
	'responseName': 'tdd-opflow-feedback',
	'applicationId': 'FibonacciGenerator'
})

def callback(body, headers, response):
	data = json.loads(body)
	print("[x] input: %s" % (data))

	response.emitStarted()
	print("[-] started")

	fg = FibonacciGenerator(data['number'])

	while(fg.next()):
		state = fg.result()
		response.emitProgress(state['step'], state['number'])
		print("[-] step: %s / %s" % (state['step'], state['number']))

	state = json.dumps(fg.result())
	response.emitCompleted(state)
	print("[-] result: %s" % state)

def signal_term_handler(signal, frame):
    print 'SIGTERM/SIGINT'
    worker.close()
    sys.exit(0)
 
signal.signal(signal.SIGINT, signal_term_handler)
signal.signal(signal.SIGTERM, signal_term_handler)

info = worker.process(None, callback)

print(' [*] Waiting for message. To exit press CTRL+C')

while threading.activeCount() > 0:
	time.sleep(1)
	print(' echo ...')
