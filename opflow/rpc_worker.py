#!/usr/bin/env python

import logging
import pika
import sys

from engine import Engine
from util import Util

logger = Util.getLogger(__name__)

class RpcWorker:
    def __init__(self, params):
        if logger.isEnabledFor(logging.DEBUG): logger.debug('Constructor begin ...')
        self.__engine = Engine(params)

        if ('operatorName' in params):
            self.__operatorName = params['operatorName']
        else:
            self.__operatorName = None

        if ('responseName' in params):
            self.__responseName = params['responseName']
        else:
            self.__responseName = None

        self.__middlewares = []
        self.__consumerInfo = None

        if logger.isEnabledFor(logging.DEBUG): logger.debug('Constructor end!')

    def process(self, routineId, callback):
        checkId = None
        if (routineId is None):
            checkId = lambda (varId): True
        elif (type(routineId) is str):
            checkId = lambda (varId): (varId == routineId)

        if (checkId is not None):
            self.__middlewares.append({
                'checker': checkId,
                'listener': callback
            })
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Middlewares length: %s' % (len(self.__middlewares)))
        else:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Invalid checker, skipped')

        if (self.__consumerInfo is not None):
            return self.__consumerInfo

        def workerCallback(channel, method, properties, body, replyToName):
            response = RpcResponse(channel, properties, method.consumer_tag, body, replyToName)
            routineId = Util.getRoutineId(properties.headers)
            for middleware in self.__middlewares:
                if (middleware['checker'](routineId)):
                    middleware['listener'](body, properties.headers, response)
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug('middleware matched, invoke callback')
                else:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug('middleware not matched, skipped')

        self.__consumerInfo = self.__engine.consume(workerCallback, {
            'binding': True,
            'queueName': self.__operatorName,
            'replyTo': self.__responseName
        })

        return self.__consumerInfo

class RpcResponse:
    def __init__(self, channel, properties, workerTag, body, replyToName):
        if logger.isEnabledFor(logging.DEBUG): logger.debug('Constructor begin ...')
        self.__channel = channel
        self.__properties = properties
        self.__workerTag = workerTag
        
        if (properties.reply_to is not None):
            self.__replyTo = properties.reply_to
        else:
            self.__replyTo = replyToName

        self.__requestId = Util.getRequestId(properties.headers)

        if logger.isEnabledFor(logging.DEBUG): logger.debug('Constructor end!')

    def emitStarted(self, info=None):
        if (info is None): info = "{}"
        properties = pika.spec.BasicProperties(
            correlation_id=self.__properties.correlation_id,
            headers=self.__createHeaders('started'))
        self.__basicPublish(info, properties)

    def emitProgress(self, completed, total=100, extra=None):
        properties = pika.spec.BasicProperties(
            correlation_id=self.__properties.correlation_id,
            headers=self.__createHeaders('progress'))
        percent = -1
        if (total > 0 and completed >= 0 and completed <= total):
            if (total == 100):
                percent = completed
            else:
                percent = (completed * 100) // total

        result = None
        if (extra is None):
            result = '{ "percent": %s }' % (percent)
        else:
            result = '{ "percent": %s, "data": %s }' % (percent, extra)

        self.__basicPublish(result, properties)

    def emitFailed(self, error=None):
        if (error is None): error = ''
        properties = pika.spec.BasicProperties(
            correlation_id=self.__properties.correlation_id,
            headers=self.__createHeaders('failed', True))
        self.__basicPublish(error, properties)

    def emitCompleted(self, value=None):
        if (value is None): value = ''
        properties = pika.spec.BasicProperties(
            correlation_id=self.__properties.correlation_id,
            headers=self.__createHeaders('completed', True))
        self.__basicPublish(value, properties)

    def __createHeaders(self, status, finished=False):
        headers = { 'status': status }
        if (self.__requestId is not None):
            headers['requestId'] = self.__requestId
        if (finished):
            headers['workerTag'] = self.__workerTag
        return headers


    def __basicPublish(self, data, properties):
        self.__channel.basic_publish(exchange='', routing_key=self.__replyTo,
            body=data, properties=properties)
