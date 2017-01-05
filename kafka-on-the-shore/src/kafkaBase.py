'''
Created on Jan 2, 2017

@author: kafka
'''
from kafka.consumer.group import KafkaConsumer
from kafka import TopicPartition
import time

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

class SinglePartitionConsumer(BaseConsumer):
    def __init__(self,topic='test',partition=0,**configs):
        super(self.__class__,self).__init__(**configs)
        topicP=[TopicPartition(topic,partition)]
        self.assign(topicP)
        self.seek(topicP[0],self._followOffset())
        
    def _followOffset(self):
        
        return 0
        
    def _initialize(self):
        raise NotImplementedError
        

if __name__=='__main__':
    consumer=SinglePartitionConsumer(topic='test',bootstrap_servers='localhost')
    i=0
    while True:
        time.sleep(1)
        #print i
        #print type(consumer.getMessages())
        items=consumer.getMessages()
        if items!={}:
            for item in items[TopicPartition('test',0)]:
                print item
        i+=1
    consumer.close()
    
class BaseDecoder(object):
    def __init__(self):
        self._type = None # JSON XML CSV 
    
    def _initialize(self):
        #loads library 
        pass

    def load(self,data):
        self._type.load

    def returnData(self):
        return self.data
