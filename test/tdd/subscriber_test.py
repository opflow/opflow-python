#!/usr/bin/env python

import unittest

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/../..')

import opflow

class PubsubHandlerTest(unittest.TestCase):

    def setUp(self):
        self.params = {
            'uri': 'amqp://master:zaq123edcx@192.168.56.56/'
        }

    def testSubscribeWithNullListener(self):
        try:
            subscriber = opflow.PubsubHandler(**dict(self.params,
                subscriberName='tdd-opflow-subscriber'))
            subscriber.subscribe(None)
        except opflow.ParameterError as error:
            self.assertTrue(True)
        except:
            self.assertTrue(False)
        finally:
            subscriber.close()

    def testSubscribeWithDifferentListeners(self):
        try:
            subscriber = opflow.PubsubHandler(**dict(self.params,
                subscriberName='tdd-opflow-subscriber'))
            def listener1(): pass
            def listener2(): pass
            subscriber.subscribe(listener1)
            subscriber.subscribe(listener2)
        except opflow.ParameterError as error:
            self.assertTrue(True)
        except:
            self.assertTrue(False)
        finally:
            subscriber.close()

    def test_multiple_subscribe_with_same_listeners(self):
        try:
            subscriber = opflow.PubsubHandler(**dict(self.params,
                subscriberName='tdd-opflow-subscriber'))
            def listener(): pass
            subscriber.subscribe(listener)
            subscriber.subscribe(listener)
            self.assertTrue(True)
        except opflow.ParameterError as error:
            self.assertTrue(False)
        except:
            self.assertTrue(False)
        finally:
            subscriber.close()

if __name__ == '__main__':
    unittest.main()
