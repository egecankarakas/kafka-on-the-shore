'''
Created on Jan 3, 2017

@author: egecan
'''

class UnsuppportedTypeError(Exception):
    def __init__(self, message, errors):

        # Call the base class constructor with the parameters it needs
        super(UnsuppportedTypeError, self).__init__(message)

        # Now for your custom code...
        self.errors = errors