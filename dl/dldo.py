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

# std lib imports
import getpass

# Data Lab Client interfaces
from dl import authClient, storeClient, queryClient


class Dldo:
    '''
       dldo super-class
    '''
    def __init__(self):
        self.token = ""
        self.user = ""
        self.loginstatus = ""
        self.unmount = ""
        pass
        
    def login(self, user):
        '''
        Login to datalab
        '''
        if user == 'anonymous':
            token = authClient.login('anonymous','')
        else:
            token = authClient.login(user,getpass.getpass(prompt='Enter password:'))

        if not authClient.isValidToken(token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        else:
            print "Authentication successful."
            self.user = user
            self.token = token
            self.loginstatus = "loggedin"
            return

    def logout(self):
        '''
        Logout out of the Data Lab
        '''
        if self.loginstatus == 'loggedout':
            print ("No user is currently logged into the Data Lab")
            return
        else:
            user, uid, gid, hash = self.token.strip().split('.', 3)

            res = authClient.logout (self.token)
            if res != "OK":
                print ("Error: %s" % res)
                return
            if self.unmount != "":
                print ("Unmounting remote space")
                cmd = "umount %s" % self.unmount
                pipe = Popen(cmd, shell=True, stdout=PIPE)
                output = pipe.stdout.read()
                self.mount = ""
                
            print ("'%s' is now logged out of the Data Lab" % user)
            self.loginstatus = "loggedout"
            self.user = ""
            self.token = ""


    def status(self):
        ''' 
        Status of the Data Lab connection
        '''
        if self.loginstatus == "loggedout":
            print ("No user is currently logged into the Data Lab")
        else:
            print ("User %s is logged into the Data Lab" % self.user)
        if self.mount != "":
            if status != "loggedout":
                print ("The user's Virtual Storage is mounted at %s" % self.mount)
            else:
                print ("The last user's Virtual Storage is still mounted at %s" % \
                    self.mount)
            
        
    def ls(self, file='vos://', format='ascii'):
        '''
        The list command method
        '''
        # Check that we have a good token
        if not authClient.isValidToken(self.token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        # Run the LS command
        return storeClient.ls (self.token, name=file, format=format)

    
    def mv(self, token, source, destination, verbose=True):
        '''
        The move command method
        '''
        # Check that we have a good token
        if not authClient.isValidToken(self.token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        # Run the MV command
        storeClient.mv (self.token, fr=source, to=destination,
                        verbose=verbose)

        
