'''
Created on Jan 3, 2017

@author: egecan
'''

from kafka import TopicPartition
from egecan.parser.Utf8Parser import Utf8Parser
from egecan.kafka.BaseConsumer import BaseConsumer
import logging


class SinglePartitionConsumer(BaseConsumer):
    _parser=None
    
    def __init__(self,topic='test',dataFormat='',partition=0,**configs):
        if dataFormat!=None:
            self._parser=Utf8Parser(dataFormat=dataFormat)
        
        BaseConsumer.__init__(self,value_deserializer=self._parser.getParserFunction(),**configs)
        
        topicP=[TopicPartition(topic,partition)]
        self.assign(topicP)
        self.seek(topicP[0],self._followOffset())
        
    def _followOffset(self):
        
        return 0
        
    def _initialize(self):
        raise NotImplementedError