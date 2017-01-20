'''
Created on Jan 3, 2017

@author: egecan
'''
from egecan.parser.BaseParser import BaseParser
from egecan.errors.UnsuppportedTypeError import UnsuppportedTypeError
from egecan.errors.UnsupportedLiteralTypeError import UnsuppportedLiteralTypeError
import avro.schema  
import avro.io  
import io
from io import BytesIO

class Utf8Parser(BaseParser):
    """This class parses a string for the given format"""
    def __init__(self,dataFormat=None,delimeter=',',literalType='HTML',**configs):
        if dataFormat!=None:
            dataFormat=dataFormat.lower()
        self._type = dataFormat # JSON XML CSV 
        if dataFormat=='json':
            import json
            self._type=json
        elif dataFormat=='xml':
            import lxml
            self.literalType=literalType
            self._type=lxml
        elif dataFormat=='csv':
            """csv dataFormat is not a fully supported dataFormat. However a basic support for it is provided"""
            import csv
            self._delimeter=delimeter
            self._type=csv
        elif dataFormat=='':
            self._type=object
        elif dataFormat=='avro':
            import avro
            self._type=avro
            self._schema=avro.schema.parse(open('/opt/Striim-3.6.7/Samples/SwarmApp/AvroTestRaw.avsc').read())
            self._reader=avro.io.DatumReader(self._schema)
        else:
            raise UnsuppportedTypeError("\nThe given dataFormat: "
                                        +str(dataFormat)+
                                        " is not supported by Utf8Parser."+
                                        "\n Please consider using one of the following types: "+
                                        "json, xml, csv, avro")
    
    def getParserFunction(self):
        if self._type.__name__=='json':
            print "\n***Using json parser!***\n"
            return lambda m : self._type.loads(m.decode('utf-8'),'utf-8')
        elif self._type.__name__=='lxml':
            from lxml import etree
            if self.literalType=='xml':
                return lambda m : etree.XML(m.decode('utf-8').encode('utf-8'))
            elif self.literalType=='html':
                return lambda m : etree.HTML(m.decode('utf-8').encode('utf-8'))
            else:
                raise UnsuppportedLiteralTypeError("The given literalType: "
                                                   +str(self.literalType)+
                                                   " is not supported by Utf8Parser."+
                                                   "\n Please consider using one of the following types: "+
                                                   "json, xml, csv, avro")
        elif self._type.__name__=='avro':
            return lambda m : self._reader.read(avro.io.BinaryDecoder(BytesIO(m)))
        elif self._type.__name__=='csv':
            return lambda m : m.decode('utf-8').split(self._delimeter)
        elif self._type.__name__ == 'object':
            return lambda m : m.decode('utf-8')#unicode(m,"utf-8")
        else:
            raise UnsuppportedTypeError("\nThe given type: "+str(self._type.__name__)+" is not supported by Utf8Parser."+
                                        "\n Please consider using one of the following types: json, xml, csv, avro")
    
    def _initialize(self):
        #loads library 
        pass

    def load(self,data):
        if self._type.__name__=='json':
            self.data = self._type.loads(data,encoding='utf-8')
        elif self._type.__name__=='xml':
            self.data = self._type.etree.ElementTree.fromstring(data.decode('utf-8'))
        elif self._type.__name__=='csv':
            self.data = data.decode('utf-8').split(self._delimeter)
        elif self._type.__name__ == 'object':
            self.data = data.decode('utf-8')
        else:
            raise UnsuppportedTypeError("\nThe given type: "+str(self._type.__name__)+" is not supported by Utf8Parser."+
                                        "\n Please consider using one of the following types: json, xml, csv")
        return self.data
    
    
    
    def parseRecord(self,item):
        if type(item)==type([]):
            items=[self.parseRecord(x) for x in item]
            #print 'list return : '+'['+','.join(items)+']'
            return '['+','.join(items)+']'
        elif type(item)==type({}):
            result='{'
            for k,v in item.items():
                result+='"'+str(k)+'"'+':'+self.parseRecord(v)+','
            #print 'dict return : '+result[:-1]+'}'
            return result[:-1]+'}'
        elif type(item)==unicode:
            #print "Unicode item : "+item
            return '"'+item+'"'
        elif type(item)==tuple:
            items=[self.parseRecord(x) for x in item]
            #print 'tuple return : '+'['+','.join(items)+']'
            return '['+','.join(items)+']'
        else:
            #print 'item return : '+unicode(str(item),'utf-8')
            return '"'+unicode(str(item),'utf-8')+'"'
