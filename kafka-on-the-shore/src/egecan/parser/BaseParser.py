'''
Created on Jan 3, 2017

@author: egecan
'''

class BaseParser(object):
    def __init__(self, *args, **kwargs):
        """Parser constructor for a given dataFormat"""
        raise NotImplementedError
    
    def load(self,data):
        """load data method that parses a string with format given to the constructor"""
        raise NotImplementedError
    
    def getParserFunction(self):
        """Returns the lambda function for the parser"""
        raise NotImplementedError

    def returnData(self):
        """Simply return the data in parser"""
        return self.data

