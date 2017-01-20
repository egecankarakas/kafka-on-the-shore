'''
Created on Jan 20, 2017

@author: egecan
'''

class UnsuppportedLiteralTypeError(Exception):
    def __init__(self, message, errors):

        # Call the base class constructor with the parameters it needs
        super(UnsuppportedLiteralTypeError, self).__init__(message)

        # Now for your custom code...
        self.errors = errors