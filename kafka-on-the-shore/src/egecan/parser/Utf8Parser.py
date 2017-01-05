'''
Created on Jan 3, 2017

@author: egecan
'''
from egecan.parser.BaseParser import BaseParser
from egecan.errors.UnsuppportedTypeError import UnsuppportedTypeError

class Utf8Parser(BaseParser):
    """This class parses a string for the given format"""
    def __init__(self,dataFormat=None,delimeter=',',**configs):
        self._type = dataFormat # JSON XML CSV 
        if dataFormat=='json':
            import json
            self._type=json
        elif dataFormat=='xml':
            import xml
            self._type=xml
        elif dataFormat=='csv':
            """csv dataFormat is not a fully supported dataFormat. However a basic support for it is provided"""
            self._delimeter=delimeter
        elif dataFormat=='':
            self._type=object
        else:
            raise UnsuppportedTypeError("\nThe given dataFormat: "+str(dataFormat)+" is not supported by Utf8Parser."+"\n Please consider using one of the following types: json, xml, csv")
    
    def getParserFunction(self):
        if self._type.__name__=='json':
            return lambda m : self._type.loads(m.decode('utf-8'))
        elif self._type.__name__=='xml':
            return lambda m : self._type.etree.ElementTree.fromstring(m.decode('utf-8'))
        elif self._type.__name__=='csv':
            return lambda m : m.decode('utf-8').split(self._delimeter)
        elif self._type.__name__ == 'object':
            return lambda m : m.decode('utf-8')#unicode(m,"utf-8")
        else:
            raise UnsuppportedTypeError("\nThe given type: "+str(self._type.__name__)+" is not supported by Utf8Parser."+
                                        "\n Please consider using one of the following types: json, xml, csv")
    
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
            self.data = data.encode('utf-8').decode('utf-8')
        else:
            raise UnsuppportedTypeError("\nThe given type: "+str(self._type.__name__)+" is not supported by Utf8Parser."+
                                        "\n Please consider using one of the following types: json, xml, csv")
        return self.data
