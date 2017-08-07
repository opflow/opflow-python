#!/usr/bin/env python

import logging
import json
import pika
import sys

from util import Util

logger = Util.getLogger(__name__)

class Engine:
    def __init__(self, params):
        self.uri = params['uri']

        connection = pika.BlockingConnection(pika.URLParameters(self.uri))

        self.connection = connection
        self.channel = None

        if ('exchange_name' in params):
            self.exchange_name = params['exchange_name']
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('exchange_name value: %s' % self.exchange_name)
        else:
            self.exchange_name = None
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('exchange_name is empty')

        if ('exchange_type' in params):
            self.exchange_type = params['exchange_type']
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('exchange_type value: %s' % self.exchange_type)
        else:
            self.exchange_type = 'direct'
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('exchange_type is empty, use default')

        if (self.exchange_name != None and self.exchange_type != None):
            channel = self.__getChannel()
            channel.exchange_declare(exchange=self.exchange_name, type=self.exchange_type, durable=True)
        
        if ('routing_key' in params):
            self.routing_key = params['routing_key']

    def produce(self, message):
        self.channel.basic_publish(body=message,
                exchange=self.exchange_name,
                routing_key=self.routing_key)

    def consume(self, callback, options):
        _channel = None
        if ('forceNewChannel' in options and options['forceNewChannel']):
            _channel = self.connection.channel()
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('forceNewChannel is True, create new channel')
        else:
            _channel = self.__getChannel()
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('forceNewChannel is False, use default channel')

        _queueName = None
        _fixedQueue = True
        _declareOk = None
        if ('queueName' in options and options['queueName'] != None):
            _declareOk = _channel.queue_declare(queue=options['queueName'],durable=True)
            _fixedQueue = True
        else:
            _declareOk = _channel.queue_declare()
            _fixedQueue = False
        _queueName = _declareOk.method.queue

        if (('binding' not in options or options['binding'] != False) and (self.exchange_name != None)):
            self.channel.queue_bind(exchange=self.exchange_name,
                routing_key=self.routing_key, queue=_queueName)

        _replyToName = None
        if ('replyTo' in options and options['replyTo'] is not None):
            _checkOk = _channel.queue_declare(queue=options['replyTo'],passive=True)
            _replyToName = _checkOk.method.queue

        def rpcCallback(channel, method, properties, body):
            requestID = Util.getRequestId(properties.headers)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Request[%s] / DeliveryTag[%s] / ConsumerTag[%s]' % 
                    (requestID, method.delivery_tag, method.consumer_tag))
            callback(channel, method, properties, body, _replyToName)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Request[%s] invoke Ack(%s, False)) / ConsumerTag[%s]' % 
                    (requestID, method.delivery_tag, method.consumer_tag))
            channel.basic_ack(method.delivery_tag, False)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Request[%s] has finished successfully' % (requestID))
        
        self.channel.basic_consume(rpcCallback, queue=_queueName, no_ack=False)
        self.channel.start_consuming()

    def close(self):
        self.connection.close()

    def __getChannel(self):
        if (self.channel == None):
            self.channel = self.connection.channel()
        return self.channel
