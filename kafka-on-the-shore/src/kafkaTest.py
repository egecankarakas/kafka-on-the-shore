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

def parseRecord(item):
    if type(item)==type([]):
        items=[parseRecord(x) for x in item]
        #print 'list return : '+'['+','.join(items)+']'
        return '['+','.join(items)+']'
    elif type(item)==type({}):
        result='{'
        for k,v in item.items():
            result+='"'+str(k)+'"'+':'+parseRecord(v)+','
        #print 'dict return : '+result[:-1]+'}'
        return result[:-1]+'}'
    elif type(item)==unicode:
        #print "Unicode item : "+item
        return '"'+item+'"'
    elif type(item)==tuple:
        items=[parseRecord(x) for x in item]
        print 'tuple return : '+'['+','.join(items)+']'
        return '['+','.join(items)+']'
    else:
        #print 'item return : '+unicode(str(item),'utf-8')
        return '"'+unicode(str(item),'utf-8')+'"'
    
if __name__ == '__main__':
    consumer=SinglePartitionConsumer(client_id='xmltestClient',topic='test4',when='m',interval=1,backupCount=3,dataFormat='avro',literalType='xml',bootstrap_servers='localhost:9092')
    i=0
    
    import avro.schema  
    import avro.io  
    import io
    from io import BytesIO
    schema = avro.schema.parse(open('/opt/Striim-3.6.7/Samples/SwarmApp/AvroTestRaw.avsc').read())
    
    while True:
        #print i
        time.sleep(1)
        #print i
        #print type(consumer.getMessages())
        items=consumer.getMessages()
        
        if items!={}:
            print items
            for item in items[TopicPartition('test4',0)]:
                print item.value['username']
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