#!/usr/bin/env python

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/..')

from fibonacci_generator import FibonacciGenerator
import opflow

params = {
	'uri': 'amqp://master:zaq123edcx@192.168.56.56/',
	'exchange_name': 'tdd-opflow-exchange',
	'routing_key': 'tdd-opflow-rpc',
	'operatorName': 'tdd-opflow-queue',
	'responseName': 'tdd-opflow-feedback'
}

worker = opflow.RpcWorker(params)

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

worker.process(None, callback)

print(' [*] Waiting for message. To exit press CTRL+C')
