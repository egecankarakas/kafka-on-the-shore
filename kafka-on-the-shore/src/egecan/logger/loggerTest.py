'''
Created on Jan 6, 2017

@author: egecan
'''

import logging
import logging.config
from kafkaLogger import KafkaLogging


if __name__ == '__main__':

    DEFAULT_LOGGING = {
        'version': 1,
        'disable_existing_loggers': False,
        'loggers': {
            '': {
                'level': 'INFO',
            },
            'another.module': {
                'level': 'DEBUG',
            },
        }
    }
    dictConf=KafkaLogging(clientId='myClient',topic='test',partition=0,when='D',interval=1).DEFAULT_LOGGING
    print dictConf
#===============================================================================
#     
# 
#     kafkaHandlerFilename='logs/kafka-ClientId-Topic-Partition.log'
#     kafkaOffsetHandlerFilename='logs/offset-ClientId-Topic-Partition.log'
#     
#     DEFAULT_LOGGING['handlers']['kafkaHandler']['filename']='newTopic bla bla'
#     
#     DEFAULT_LOGGING['handlers']['kafkaHandler']['filename']='newTopic bla bla'
#     DEFAULT_LOGGING['handlers']['kafkaOffsetHandler']['filename']='newTopic bla bla'
#     
#     
#     logging.config.dictConfig(DEFAULT_LOGGING)
#===============================================================================
    
    #logging.info('Hello, log')