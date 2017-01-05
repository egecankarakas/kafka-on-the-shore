'''
Created on Jan 3, 2017

@author: egecan
'''

from kafka.consumer.group import KafkaConsumer

class BaseConsumer(KafkaConsumer):
    def __init__(self,*topics,**configs):
        """Base Consumer Constructor, adapts from KafkaConsumer"""
        KafkaConsumer.__init__(self,*topics,**configs)
        
    def getMessages(self,timeout_ms=0,max_records=None):
        """return messages from producer system"""
        return self.poll(timeout_ms, max_records)
        
    def _initialize(self):
        """initalize connection object"""
        raise NotImplementedError("BaseConsumer __init__  is not implemented")

    def next(self):
        return next(self)

    def _followOffset(self):
        """Manage consume offest"""
        raise NotImplementedError("BaseConsumer _followOffset is not implemented")
