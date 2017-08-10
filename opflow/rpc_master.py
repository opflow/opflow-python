#!/usr/bin/env python

import logging
import pika
import Queue
import time
import threading
import uuid

from engine import Engine
from util import Util

logger = Util.getLogger(__name__)

class RpcMaster:
    def __init__(self, params):
        if logger.isEnabledFor(logging.DEBUG): logger.debug('Constructor begin ...')
        self.__lock = threading.RLock()
        self.__idle = threading.Condition(self.__lock)
        self.__engine = Engine(params)
        self.__tasks = {}
        self.__responseConsumer = None
        self.__responseName = params['responseName']

    def request(self, routineId, content, options=None):
        if logger.isEnabledFor(logging.DEBUG): logger.debug('request() is invoked')

        if (options is None): options = {}

        if (self.__responseConsumer is None):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('request() init new ResponseConsumer')
            self.__responseConsumer = self.__initResponseConsumer(False)
        
        consumerInfo = self.__responseConsumer

        taskId = Util.getUUID()

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('request() - new taskId: %s' % (taskId))

        def completeListener():
            self.__lock.acquire()
            try:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('completeListener will be invoked')
                del self.__tasks[taskId]
                if len(self.__tasks) == 0:
                    if logger.isEnabledFor(logging.DEBUG): logger.debug('tasks is empty')
                    self.__idle.notify()
            finally:
                self.__lock.release()

        if (routineId is not None):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('request() - routineId: %s' % (routineId))
            options['routineId'] = routineId

        task = RpcRequest(options, completeListener)
        self.__tasks[taskId] = task

        headers = { 'routineId': task.routineId, 'requestId': task.requestId }
        properties = { 'headers': headers, 'correlation_id': taskId }
        
        if not consumerInfo['fixedQueue']:
            properties['reply_to'] = consumerInfo['queueName']

        self.__engine.produce(message=content, properties=properties)

        return task

    def __initResponseConsumer(self, forked=False):
        if logger.isEnabledFor(logging.DEBUG):
                logger.debug('__initResponseConsumer() - invoked with forked: %s' % forked)
        def callback(channel, method, properties, body, replyToName):
            taskId = properties.correlation_id
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('responseConsumer - task[%s] received data' % (taskId))

            if (taskId not in self.__tasks):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('responseConsumer - task[%s] not found, skipped' % (taskId))
                return
            task = self.__tasks[taskId]

            task.push({ 'content': body, 'headers': properties.headers })
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('responseConsumer - task[%s] message enqueued' % (taskId))
        
        options = { 'binding': False, 'prefetch': 1 }
        if (not forked):
            options['queueName'] = self.__responseName
            options['consumerLimit'] = 1
            options['forceNewChannel'] = False

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('__initResponseConsumer() - options: %s' % options)
        
        return self.__engine.consume(callback, options)

    def close(self):
        self.__lock.acquire()
        try:
            while len(self.__tasks) > 0: self.__idle.wait()
            if self.__responseConsumer is not None:
                self.__engine.cancelConsumer(self.__responseConsumer)
            self.__engine.close()
        finally:
            self.__lock.release()

class RpcRequest:
    EMPTY = { 'status': 'EMPTY' }
    ERROR = { 'status': 'ERROR' }

    def __init__(self, options, callback):
        if logger.isEnabledFor(logging.DEBUG): logger.debug('RpcRequest constructor begin')
        self.__requestId = Util.getRequestId(options, True)
        self.__routineId = Util.getRoutineId(options, True)
        self.__timeout = None
        self.__timestamp = time.time()
        self.__completeListener = callback
        self.__list = Queue.Queue()

    @property
    def requestId(self):
        return self.__requestId

    @property
    def routineId(self):
        return self.__routineId

    @property
    def timestamp(self):
        return self.__timestamp

    def hasNext(self):
        self.__current = self.__list.get()
        self.__list.task_done()
        if (self.__current == self.EMPTY): return False
        if (self.__current == self.ERROR): return False
        return True

    def next(self):
        _result = self.__current
        self.__current = None
        return _result

    def push(self, message):
        self.__list.put(item=message, block=True)
        if (self.__isDone(message)):
            self.__list.put(self.EMPTY, True)
            if (callable(self.__completeListener)): self.__completeListener()
        self.__list.join()

    def exit(self):
        self.__list.put(self.ERROR, True)
        self.__list.join()

    def __isDone(self, message):
        status = None
        if ('headers' in message and message['headers'] is not None):
            headers = message['headers']
            if ('status' in headers):
                status = headers['status']
        if (status in ['failed', 'completed']):
            return True
        else:
            return False