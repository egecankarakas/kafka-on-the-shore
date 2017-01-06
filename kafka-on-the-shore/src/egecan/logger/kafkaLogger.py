'''
Created on Jan 6, 2017

@author: egecan
'''
import logging

class KafkaLogging(object):
    '''
    classdocs
    '''

    DEFAULT_LOGGING={
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simpleFormatter':{
                'format':'%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            },
            'kafkaOffsetFormatter':{
                'format':'%(message)s'
            }
        },
        'handlers': {
            'kafkaHandler':{
                'class':'logging.handlers.TimedRotatingFileHandler',
                'level':'INFO',
                'formatter':'simpleFormatter',
                'filename':'logs/kafka-clientId-topic-partition.log',
                'when':'D',
                'interval':1,
                'backupCount':7,
                'encoding':'utf-8',
            },
            'kafkaOffsetHandler': {
                'class':'logging.FileHandler',
                'level':'INFO',
                'formatter':'kafkaOffsetFormatter',
                'filename':'logs/offset-clientId-topic-partition.log',
                'mode':'w',
                'encoding':'utf-8',
            }
            
        },
        'loggers': {
            'kafkaLogger': {
                'level': 'INFO',
                'handlers':['kafkaHandler'],
                'propogate':False,
            },
            'kafkaOffsetLogger': {
                'level': 'INFO',
                'handlers':['kafkaOffsetHandler']
            },
        }
    }
    
    editables=['when','clientId','topic','partition','interval','backupCount']

    def __init__(self, **configs):
        self._configureLogging(**configs)
        #print self.DEFAULT_LOGGING
        
    def _configureLogging(self,**configs):
        dictConf=self.DEFAULT_LOGGING
        for key in configs:
            if key in self.editables:
                if key=='when' or key=='interval' or key=='backupCount':
                    dictConf['handlers']['kafkaHandler'][key]=configs[key]
                else:
                    dictConf['handlers']['kafkaOffsetHandler']['filename']=str(configs[key]).join(dictConf['handlers']['kafkaOffsetHandler']['filename'].rsplit(key, 1))
                    dictConf['handlers']['kafkaHandler']['filename']=str(configs[key]).join(dictConf['handlers']['kafkaHandler']['filename'].rsplit(key, 1))

