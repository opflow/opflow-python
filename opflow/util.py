#!/usr/bin/env python

import logging
import uuid

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s'))

class Util(object):
    @staticmethod
    def getRoutineId(headers):
        if (headers is None or 'routineId' not in headers):
            return None
        return headers['routineId']

    @staticmethod
    def getRequestId(headers):
        if (headers is None or 'requestId' not in headers):
            return str(uuid.uuid4())
        return headers['requestId']

    @staticmethod
    def getLogger(name):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(ch)
        return logger
