from twisted.internet import defer
from datetime import datetime
from txamqp.content import Content

# amqp_context.py
errorLogger = None

def setErrorLogger(broker):
    global errorLogger
    errorLogger = broker

def getErrorLogger():
    return errorLogger

class amqpErrorLogger():
    
    def __init__(self, amqpBroker):
        self.amqpBroker = amqpBroker

    @defer.inlineCallbacks
    def errorLogger(self, log, content):
        log.info('sending to amqp')

        # Log locally (optional)
        log.info("Publishing error to AMQP: %s", content)

        # Publish
        yield self.amqpBroker.publish(exchange='messaging', routing_key='submit.error.*', content=content)