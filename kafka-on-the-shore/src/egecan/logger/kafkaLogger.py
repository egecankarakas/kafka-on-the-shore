'''
Created on Jan 6, 2017

@author: egecan
'''
import logging
import logging.config
import inspect

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
                'filename':'logs/kafka-client_id-topic-partition.log',
                'when':'D',
                'interval':1,
                'backupCount':7,
                'encoding':'utf-8',
            },
            'kafkaOffsetHandler': {
                'class':'logging.handlers.RotatingFileHandler',
                'level':'INFO',
                'formatter':'kafkaOffsetFormatter',
                'filename':'logs/offset-client_id-topic-partition.log',
                'mode':'r+',
                'maxBytes':1,
                'backupCount':1,
                'encoding':'utf-8',
            }
            
        },
        'loggers': {
            'kafkaLogger': {
                'level': 'INFO',
                'handlers':['kafkaHandler'],
                'propogate':False,
            },
            'offsetLogger': {
                'level': 'INFO',
                'handlers':['kafkaOffsetHandler'],
                'propogate':False,
            }
        }
    }
    
    editables=['when','client_id','topic','partition','interval','backupCount']

    def __init__(self, **configs):
        self._configureLogging(**configs)
        print self.DEFAULT_LOGGING
        logging.config.dictConfig(self.DEFAULT_LOGGING)
        self.logger=logging.getLogger('kafkaLogger')
        self.offsetLogger=logging.getLogger('offsetLogger')
        #return self.logger
        #print self.DEFAULT_LOGGING
        
    def _configureLogging(self,**configs):
        dictConf=self.DEFAULT_LOGGING
        absoluteBasePath="/".join(inspect.getfile(self.__class__).split('/')[:-1])
        for key in configs:
            if key in self.editables:
                if key=='when' or key=='interval' or key=='backupCount':
                    dictConf['handlers']['kafkaHandler'][key]=configs[key]
                else:
                    dictConf['handlers']['kafkaOffsetHandler']['filename']=str(configs[key]).join(dictConf['handlers']['kafkaOffsetHandler']['filename'].rsplit(key, 1))
                    dictConf['handlers']['kafkaHandler']['filename']=str(configs[key]).join(dictConf['handlers']['kafkaHandler']['filename'].rsplit(key, 1))
        
        dictConf['handlers']['kafkaOffsetHandler']['filename']=absoluteBasePath+'/'+dictConf['handlers']['kafkaOffsetHandler']['filename']
        print dictConf['handlers']['kafkaOffsetHandler']['filename']
        dictConf['handlers']['kafkaHandler']['filename']=absoluteBasePath+'/'+dictConf['handlers']['kafkaHandler']['filename']
        print dictConf['handlers']['kafkaHandler']['filename']
    