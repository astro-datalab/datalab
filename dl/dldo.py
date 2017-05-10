#!/usr/bin/env python
#
# DLDO.PY -- Task routines for the 'datalab' notebook interface
#

#from __future__ import print_function

__authors__ = 'David Nidever <dnidever@noao.edu>, Robert Nikutta <rnikutta@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20170509'  # yyyymmdd


"""
    Task routines for the 'datalab' command-line client.

Import via

.. code-block:: python

    from dl import datalab
"""


# Data Lab Client interfaces
from dl import authClient, storeClient, queryClient


class Dldo:
    '''
       dldo super-class
    '''
    def __init__(self):
        pass
        
    def login(self, username):
        '''
        Login to datalab
        '''
        if username == 'anonymous':
            token = authClient.login('anonymous','')
        else:
#            print "Enter password:"
            token = authClient.login(username,getpass.getpass(prompt='Enter password:'))

        if not authClient.isValidToken(token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        else:
            print "Authentication successful."
            self.token = token
            return token

        
    def ls(self, file, format='ascii'):
        '''
        The list command method
        '''
        # Check that we have a good token
        if not authClient.isValidToken(self.token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        # Run the LS command
        return storeClient.ls (token, name=file, format=format)

    
    def mv(self, token, source, destination, verbose=True):
        '''
        The move command method
        '''
        # Check that we have a good token
        if not authClient.isValidToken(self.token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        # Run the MV command
        storeClient.mv (token, fr=source, to=destination,
                        verbose=verbose)

        
