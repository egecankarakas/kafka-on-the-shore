# -*- coding: utf-8 -*-
'''
Created on Jan 4, 2017

@author: egecan
'''

from egecan.kafka.SinglePartitionConsumer import SinglePartitionConsumer
from kafka import TopicPartition
import time

if __name__ == '__main__':
    consumer=SinglePartitionConsumer(client_id='myClient',topic='test',bootstrap_servers='localhost:9092')
    i=0
    while True:
        #print i
        time.sleep(1)
        #print i
        #print type(consumer.getMessages())
        items=consumer.getMessages()
        if items!={}:
            for item in items[TopicPartition(topic='test', partition=0)]:
                print item.value
        i+=1
        if i==50:
            break
    consumer.close()