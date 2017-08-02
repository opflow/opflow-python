#!/usr/bin/env python
import pika
import sys
import logging

#logging.basicConfig(level=logging.DEBUG)

connection = pika.BlockingConnection(pika.URLParameters('amqp://master:zaq123edcx@192.168.56.56?frameMax=0x1000'))
channel = connection.channel()

channel.exchange_declare(exchange='direct_logs',
                         type='direct')

severity = sys.argv[1] if len(sys.argv) > 2 else 'info'
message = ' '.join(sys.argv[2:]) or 'Hello World!'
channel.basic_publish(exchange='direct_logs',
                      routing_key=severity,
                      body=message)
print(" [x] Sent %r:%r" % (severity, message))
connection.close()