#!/usr/bin/env python

import logging
import json
import pika
import threading
import time

from exception import ConstructorError
from exception import OperationError
from util import Util

logger = Util.getLogger(__name__)

class Engine:
    def __init__(self, params):
        if 'uri' in params and type(params['uri']) is str and params['uri'] is not None:
            self.__uri = params['uri']
        else:
            raise ConstructorError('"uri" not found or not a string')

        try:
            self.__connection = pika.BlockingConnection(pika.URLParameters(self.__uri))
            self.__channel = None
            self.__thread = None

            if ('exchange_name' in params):
                self.__exchangeName = params['exchange_name']
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('exchange_name value: %s' % self.__exchangeName)
            else:
                self.__exchangeName = None
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('exchange_name is empty')

            if ('exchange_type' in params):
                self.__exchangeType = params['exchange_type']
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('exchange_type value: %s' % self.__exchangeType)
            else:
                self.__exchangeType = 'direct'
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('exchange_type is empty, use default')

            if (self.__exchangeName != None and self.__exchangeType != None):
                channel = self.__getChannel()
                channel.exchange_declare(exchange=self.__exchangeName, type=self.__exchangeType, durable=True)
            
            if ('routing_key' in params):
                self.__routingKey = params['routing_key']

            if ('applicationId' in params):
                self.__applicationId = params['applicationId']
            else:
                self.__applicationId = None
        except:
            raise ConstructorError('Error on connecting or exchange declaration')

    def produce(self, message, properties, override=None):
        if (self.__applicationId is not None):
            properties['app_id'] = self.__applicationId
        basicProperties = pika.spec.BasicProperties(**properties)
        self.__channel.basic_publish(body=message, properties=basicProperties,
                exchange=self.__exchangeName, routing_key=self.__routingKey)

    def consume(self, callback, options):
        _channel = None
        if ('forceNewChannel' in options and options['forceNewChannel']):
            _channel = self.__connection.channel()
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
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('_queueName after run queue_declare(): %s' % _queueName)

        if (('binding' not in options or options['binding'] != False) and (self.__exchangeName != None)):
            self.__channel.queue_bind(exchange=self.__exchangeName,
                routing_key=self.__routingKey, queue=_queueName)

        _replyToName = None
        if ('replyTo' in options and options['replyTo'] is not None):
            _checkOk = _channel.queue_declare(queue=options['replyTo'],passive=True)
            _replyToName = _checkOk.method.queue
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('_replyToName after check: %s' % _replyToName)

        def rpcCallback(channel, method, properties, body):
            requestID = Util.getRequestId(properties.headers, False)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Request[%s] / DeliveryTag[%s] / ConsumerTag[%s]' % 
                    (requestID, method.delivery_tag, method.consumer_tag))
            try:
                if self.__applicationId is not None and self.__applicationId != properties.app_id:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug('Request[%s] received app_id:%s, but accepted app_id:%s, rejected' % 
                            (requestID, properties.app_id, self.__applicationId))
                    channel.basic_nack(delivery_tag=method.delivery_tag,multiple=False,requeue=True)
                    return

                callback(channel, method, properties, body, _replyToName)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('Request[%s] invoke Ack(%s, False)) / ConsumerTag[%s]' % 
                        (requestID, method.delivery_tag, method.consumer_tag))
                channel.basic_ack(delivery_tag=method.delivery_tag,multiple=False)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('Request[%s] has finished successfully' % (requestID))
            except:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('Request[%s] has failed. Rejected but service still alive' % (requestID))

        _consumerTag = _channel.basic_consume(rpcCallback, queue=_queueName, no_ack=False)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('_consumerTag after run basic_consume(): %s' % _consumerTag)

        self.__start_consuming()

        _consumerInfo = { 'channel': _channel,'queueName': _queueName,
            'fixedQueue': _fixedQueue,'consumerTag': _consumerTag }

        return _consumerInfo

    def cancelConsumer(self, consumerInfo=None):
        if consumerInfo is None: return
        if 'channel' not in consumerInfo or 'consumerTag' not in consumerInfo: return
        consumerInfo['channel'].basic_cancel(consumerInfo['consumerTag'])

    def close(self):
        self.__stop_consuming()
        if self.__connection is not None and self.__connection.is_open:
            self.__connection.close()

    @property
    def consumingLoop(self):
        return self.__thread

    def __getChannel(self):
        if self.__channel is None:
            self.__channel = self.__connection.channel()
        return self.__channel

    def __start_consuming(self):
        def startConsumer():
            # if logger.isEnabledFor(logging.DEBUG):
            #    logger.debug('invoke connection.process_data_events()')
            self.__connection.process_data_events(0.01)

        if self.__thread is None:
            self.__thread = StoppableThread(target=startConsumer,name='ConsumingThread')
            self.__thread.start()

    def __stop_consuming(self):
        if self.__thread is not None:
            self.__thread.stop()
            time.sleep(0.5)


class StoppableThread(threading.Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped condition."""

    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}):
        super(StoppableThread, self).__init__(group, target, name, args, kwargs)
        self.__target = target
        self.__args = args
        self.__kwargs = kwargs
        self.__stop_event = threading.Event()

    def run(self):
        while not self.__stop_event.is_set():
            self.__target(*self.__args, **self.__kwargs)

    def stop(self):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('StoppableThread enable stop_event')
        self.__stop_event.set()
