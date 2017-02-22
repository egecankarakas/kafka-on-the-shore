'''
Created on Jan 3, 2017

@author: egecan
'''

from kafka import TopicPartition
from egecan.parser.Utf8Parser import Utf8Parser
from egecan.kafka.BaseConsumer import BaseConsumer
from egecan.logger.kafkaLogger import KafkaLogging

class SinglePartitionConsumer(BaseConsumer):
    _parser=None
    offset=0
    logger=None
    logConfig=None
    topicP=None
    
    def __init__(self,topic='test',dataFormat='',when='M',interval=1,backupCount=3,partition=0,literalType='xml',schemaFile='',**configs):
        if dataFormat!=None:
            self._parser=Utf8Parser(dataFormat=dataFormat,literalType=literalType,schemaFile=schemaFile)
        BaseConsumer.__init__(self,value_deserializer=self._parser.getParserFunction(),**configs)

        configs.update({'topic':topic,'partition':partition,
                             'when':when,'interval':interval,'backupCount':backupCount})
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
        parsedMsgs=[]
        if self.topicP in msgs:
            for msg in msgs[self.topicP]:
                parsedMessage="Consumer Record("
                for i in range(len(msg)):
                    self._parser.parseRecord(msg._fields[i])+'='+self._parser.parseRecord(msg.__getnewargs__()[i])+','
                parsedMessage=parsedMessage[:-1]+')'
                parsedMsgs.append(parsedMessage)
                self.logger.info(parsedMessage)
                self.offsetLogger.info(msg=msg.offset)
        return parsedMsgs
    
    def getMessages(self, timeout_ms=0, max_records=None):
        msgs=BaseConsumer.getMessages(self, timeout_ms=timeout_ms, max_records=max_records)
        if self.topicP in msgs:
            for msg in msgs[self.topicP]:
                self.logger.info(self._parser.parseRecord(msg.value))
                self.offsetLogger.info(msg=msg.offset)
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
    
