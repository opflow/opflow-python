#!/usr/bin/env python

import logging
import pika

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
        routineIdChecker = None
        if (routineId is None):
            routineIdChecker = lambda (varId): True
        elif (type(routineId) is str):
            routineIdChecker = lambda (varId): (varId == routineId)
        elif (type(routineId) is list):
            routineIdChecker = lambda (varId): (varId in routineId)

        if (callable(routineIdChecker)):
            self.__middlewares.append({
                'checker': routineIdChecker,
                'listener': callback
            })
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Middlewares length: %s' % (len(self.__middlewares)))
        else:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('Invalid checker (routineIdChecker is not callable), skipped')

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

    def close(self):
        if self.__consumerInfo is not None:
            self.__engine.cancelConsumer(self.__consumerInfo)
        self.__engine.close()

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

        self.__requestId = Util.getRequestId(properties.headers, False)

        if logger.isEnabledFor(logging.DEBUG): logger.debug('Constructor end!')

    def emitStarted(self, info=None):
        if (info is None): info = "{}"
        self.__basicPublish(info, self.__buildProperties('started'))

    def emitProgress(self, completed, total=100, extra=None):
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
        self.__basicPublish(result, self.__buildProperties('progress'))

    def emitFailed(self, error=None):
        if (error is None): error = ''
        self.__basicPublish(error, self.__buildProperties('failed', True))

    def emitCompleted(self, value=None):
        if (value is None): value = ''
        self.__basicPublish(value, self.__buildProperties('completed', True))

    def __buildProperties(self, status, finished=False):
        headers = self.__createHeaders(status, finished)
        properties = None
        if (self.__properties.app_id is None):
            properties = pika.spec.BasicProperties(
                correlation_id=self.__properties.correlation_id,
                headers=headers)
        else:
            properties = pika.spec.BasicProperties(
                app_id=self.__properties.app_id,
                correlation_id=self.__properties.correlation_id,
                headers=headers)
        return properties

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
