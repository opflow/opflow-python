#!/usr/bin/env python

import unittest

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/../..')

import opflow

class ExecutorTest(unittest.TestCase):

    def setUp(self):
        self.engine = opflow.Engine({
            'uri': 'amqp://master:zaq123edcx@192.168.56.56/'
        })
        self.executor = opflow.Executor({ 'engine': self.engine })

    def testCreateExchangeIfNotExist(self):
        self.executor.deleteExchange(exchangeName='opflow-tdd-exchange')
        self.executor.declareExchange(exchangeName='opflow-tdd-exchange')
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()
