'''
Created on Jan 3, 2017

@author: egecan
'''

from kafka import TopicPartition
from egecan.parser.Utf8Parser import Utf8Parser
from egecan.kafka.BaseConsumer import BaseConsumer
from egecan.logger.kafkaLogger import KafkaLogging
import inspect

class SinglePartitionConsumer(BaseConsumer):
    _parser=None
    offset=0
    logger=None
    logConfig=None
    topicP=None
    
    def __init__(self,topic='test',dataFormat='',partition=0,**configs):
        if dataFormat!=None:
            self._parser=Utf8Parser(dataFormat=dataFormat)

        BaseConsumer.__init__(self,value_deserializer=self._parser.getParserFunction(),**configs)

        configs.update({'topic':topic,'partition':partition})
        self._configureLogger(**configs)
        

        self.topicP=TopicPartition(topic,partition)
        self.assign([self.topicP])
        self.seek(self.topicP,self._followOffset())
        
        
    def _followOffset(self,**configs):
        self.offset=0
        try:
            with open(self.logConfig['handlers']['kafkaOffsetHandler']['filename'],'r') as f:
                lines=f.readlines()
                self.offset=int(lines[len(lines)-1])+1
        except:
            print 'Couldnt read from file : '+self.logConfig['handlers']['kafkaOffsetHandler']['filename']
            pass
        return self.offset
    
    def __next__(self):
        msgs=BaseConsumer.__next__(self)
        if self.topicP in msgs:
            for msg in msgs[self.topicP]:
                self.logger.info(msg=msg)
                self.offsetLogger.info(msg=msg.offset)
        return msgs
    
    def getMessages(self, timeout_ms=0, max_records=None):
        msgs=BaseConsumer.getMessages(self, timeout_ms=timeout_ms, max_records=max_records)
        if self.topicP in msgs:
            for msg in msgs[self.topicP]:
                self.logger.info(msg=msg)
                self.offsetLogger.info(msg=str(msg.offset))
                
        return msgs
    
    def _configureLogger(self,**configs):
        loggerArgs={}
        for key in configs:
            if key in KafkaLogging.editables:
                loggerArgs[key]=configs[key]
        
        kafkaLogging=KafkaLogging(**loggerArgs)
        print kafkaLogging.DEFAULT_LOGGING
        self.logConfig=kafkaLogging.DEFAULT_LOGGING
        self.logger=kafkaLogging.logger
        self.offsetLogger=kafkaLogging.offsetLogger
        
    def _initialize(self):
        raise NotImplementedError