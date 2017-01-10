'''
Created on Jan 6, 2017

@author: egecan
'''

import logging
import logging.config
import inspect
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
    
    kafkaLogging=KafkaLogging(clientId='myClient',topic='test',partition=0,when='D',interval=1,backupCount=7)
    dictConf=kafkaLogging.DEFAULT_LOGGING
    logger=kafkaLogging.logger
    
    for i in range(1):
        logger.info('Merhaba '+str(i))
        
    print dictConf
    
    print inspect.getfile(kafkaLogging.__class__)