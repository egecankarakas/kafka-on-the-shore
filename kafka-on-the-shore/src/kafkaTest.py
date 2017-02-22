# -*- coding: UTF-8 -*-
'''
Created on Jan 4, 2017

@author: egecan
'''

from egecan.kafka.SinglePartitionConsumer import SinglePartitionConsumer
from kafka import TopicPartition
import time
from lxml import etree
import re
from io import BytesIO

if __name__ == '__main__':
    consumer=SinglePartitionConsumer(client_id='xmltestClient',topic='test',dataFormat='',bootstrap_servers='localhost:9092,localhost:9094,localhost:9096')
    i=0
    
    
    while True:
        #print i
        time.sleep(1)
        #print i
        #print type(consumer.getMessages())
        items=consumer.getMessages()
        
        if items!={}:
            print items
            for item in items[TopicPartition('test',0)]:
                print item.value
            #        for col in item.value:
            #            print col
                    #root=etree.fromstring(item.value.replace("&#","&#x"))
                    #print item.value
                    #if type(item.value)==type(etree.HTML('<a/>')):
                    #    print etree.tostring(item.value,encoding='utf-8')
                    #print etree.tostring(item.value)
                    #root=etree.HTML(item.value)
                    #print etree.tostring(root,encoding='utf-8')
                    #print'****'
        else:
            print '{{}}'
        i+=1
        if i==50:
            break
    consumer.close()