#!/usr/bin/env python
#
# STORECLIENT -- Client routines for the Data Lab Store Manager service
#

from __future__ import print_function

__authors__ = 'Matthew Graham <graham@noao.edu>, Mike Fitzpatrick <fitz@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20180320'  # yyyymmdd


"""
    Client routines for the DataLab Storage Manager service.

Import via

.. code-block:: python

    from dl import storeClient

"""

import os
import sys
import fnmatch
import requests
import glob
import socket
import json

if os.path.isfile ('./Util.py'):                # use local dev copy
    from Util import multifunc
    from Util import multimethod
    from Util import def_token
else:                                           # use distribution copy
    from dl.Util import multifunc
    from dl.Util import multimethod
    from dl.Util import def_token

# Turn off some annoying astropy warnings
import warnings
from astropy.utils.exceptions import AstropyWarning
warnings.simplefilter('ignore', AstropyWarning)



#####################################
#  Storage Manager Configuration
#####################################

# The URL of the Storage Manager service to contact.  This may be changed by
# passing a new URL into the set_svc_url() method before beginning.

DEF_SERVICE_URL = 'https://datalab.noao.edu/storage'


# Allow the service URL for dev/test systems to override the default.
THIS_HOST = socket.gethostname()                        # host name
THIS_IP   = socket.gethostbyname(socket.gethostname())  # host IP address

if THIS_HOST[:5] == 'dldev':
    DEF_SERVICE_URL  = 'http://dldev.datalab.noao.edu/storage'
elif THIS_HOST[:6] == 'dltest':
    DEF_SERVICE_URL  = 'http://dltest.datalab.noao.edu/storage'

# The requested query 'profile'.  A profile refers to the specific
# machines and services used by the Storage Manager on the server.
DEF_PROFILE     = 'default'

# Use a /tmp/SM_DEBUG file as a way to turn on debugging in the client code.
DEBUG           = os.path.isfile ('/tmp/SM_DEBUG')



# ####################################################################
#  Store Client error class
# ####################################################################

class storeClientError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message



# ####################################################################
#  Module Functions
# ####################################################################

# --------------------------------------------------------------------
# ISALIVE -- Ping the Storage Manager service to see if it responds.
#
def isAlive (svc_url=DEF_SERVICE_URL):
    return client.isAlive (svc_url=svc_url)
    
# --------------------------------------------------------------------
# SET_SVC_URL -- Set the service url to use.
#
def set_svc_url (svc_url=DEF_SERVICE_URL):
    return client.set_svc_url (svc_url=svc_url)

# --------------------------------------------------------------------
# GET_SVC_URL -- Get the service url being used.
#
def get_svc_url ():
    return client.get_svc_url ()

# --------------------------------------------------------------------
# SET_PROFILE -- Set the profile to be used
#
def set_profile (profile=DEF_PROFILE):
    return client.set_profile (profile=profile)

# --------------------------------------------------------------------
# GET_PROFILE -- Get the profile currently being used.
#
def get_profile ():
    return client.get_profile ()
    
# --------------------------------------------------------------------
# LIST_PROFILES -- List the profiles supported by the storage manager service
#
@multifunc('sc',1)
def list_profiles  (token, profile=None, format='text'):
    '''  Usage:  storeClient.list_profiles (token)
    '''
    return client._list_profiles (token=def_token(token), profile=profile,
                                  format=format)
    
@multifunc('sc',0)
def list_profiles  (token=None, profile=None, format='text'):
    '''  Usage:  storeClient.list_profiles ()
    '''
    return client._list_profiles (token=def_token(token), profile=profile,
                                  format=format)
    

        
# -----------------------------
#  Utility Functions
# -----------------------------

# --------------------------------------------------------------------
# ACCESS -- Determine whether the file can be accessed with the given node.
#           Modes are 'r' (read access), 'w' (write access), or '' or None
#           for an existence test.
#
@multifunc('sc',3)
def access (token, path, mode, verbose=True):
    '''  Usage:  storeClient.access (token, path, mode)
    '''
    return client._access (path=path, mode=mode, token=def_token(token), 
                          verbose=verbose)

@multifunc('sc',2)
def access (path, mode, token=None, verbose=True):
    '''  Usage:  storeClient.access (path, mode)
    '''
    return client._access (path=path, mode=mode, token=def_token(token), 
                          verbose=verbose)

@multifunc('sc',1)
def access (path, mode=None, token=None, verbose=True):
    '''  Usage:  storeClient.access (path)
    '''
    return client._access (path=path, mode=mode, token=def_token(token), 
                          verbose=verbose)


# --------------------------------------------------------------------
# STAT -- Get file status. Values are returned as a dictionary of the
#         requested node.
#
@multifunc('sc',2)
def stat (token, path, verbose=True):
    '''  Usage:  storeClient.stat (token, path)
    '''
    return client._stat (path=path, token=def_token(token), verbose=verbose)

@multifunc('sc',1)
def stat (path, token=None, verbose=True):
    '''  Usage:  storeClient.stat (path)
    '''
    return client._stat (path=path, token=def_token(token), verbose=verbose)



# --------------------------------------------------------------------
# GET -- Retrieve a file (or files) from the Store Manager service
#
@multifunc('sc',3)
def get  (token, fr, to, verbose=True, debug=False):
    '''  Usage:  storeClient.get (token, fr, to)
    '''
    return client._get (fr=fr, to=to, token=def_token(token),
                        verbose=verbose, debug=debug)

@multifunc('sc',2)
def get  (opt1, opt2, fr='', to='', token=None, verbose=True, debug=False):
    '''  Usage:  storeClient.get (fr, to)
    '''
    if opt1 is not None and len(opt1.split('.')) >= 4:
        # opt1 looks like a token
        return client._get (fr=opt2, to=to, token=def_token(opt1),
                            verbose=verbose, debug=debug)
    else:
        # opt1 is the 'fr' value, opt2 is the 'to' value
        return client._get (fr=opt1, to=opt2, token=def_token(token),
                            verbose=verbose, debug=debug)

@multifunc('sc',1)
def get (optval, fr='', to='', token=None, verbose=True, debug=False):
    '''  Usage:  storeClient.get (fr)
    '''
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return client._get (fr=fr, to=to, token=def_token(optval), 
                            verbose=verbose, debug=debug)
    else:
        # optval is the 'fr' value
        return client._get (fr=optval, to=to, token=def_token(token), 
                            verbose=verbose, debug=debug)

@multifunc('sc',0)
def get  (token=None, fr='', to='', verbose=True, debug=False):
    '''  Usage:  storeClient.get (token, fr, to)
    '''
    return client._get (fr=fr, to=to, token=def_token(token),
                        verbose=verbose, debug=debug)


# --------------------------------------------------------------------
# PUT -- Upload a file (or files) to the Store Manager service
#
@multifunc('sc',3)
def put  (token, fr, to, verbose=True, debug=False):
    '''  Usage:  storeClient.put (token, fr, to)
    '''
    return client._put (fr=fr, to=to, token=def_token(token),
                        verbose=verbose, debug=debug)

@multifunc('sc',2)
def put  (fr, to, token=None, verbose=True, debug=False):
    '''  Usage:  storeClient.put (fr, to)
    '''
    return client._put (fr=fr, to=to, token=def_token(token),
                        verbose=verbose, debug=debug)

@multifunc('sc',1)
def put  (optval, fr='', to='vos://', token=None, verbose=True, debug=False):
    '''  Usage:  storeClient.put (fr)
    '''
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return client._put (fr=fr, to=to, token=def_token(optval),
                            verbose=verbose, debug=debug)
    else:
        # optval looks like source name
        return client._put (fr=optval, to=to, token=def_token(token),
                            verbose=verbose, debug=debug)

@multifunc('sc',0)
def put  (fr='', to='vos://', token=None, verbose=True, debug=False):
    '''  Usage:  storeClient.put (fr='',to='')
    '''
    return client._put (fr=fr, to=to, token=def_token(token),
                        verbose=verbose, debug=debug)


# --------------------------------------------------------------------
# CP -- Copy a file/directory within the store manager service
#
@multifunc('sc',3)
def cp  (token, fr, to, verbose=False):
    '''  Usage:  storeClient.cp (token, fr, to)
    '''
    return client._cp (fr=fr, to=to, token=def_token(token), verbose=verbose)

@multifunc('sc',2)
def cp  (fr, to, token=None, verbose=False):
    '''  Usage:  storeClient.cp (fr, to)
    '''
    return client._cp (fr=fr, to=to, token=def_token(token), verbose=verbose)

@multifunc('sc',1)
def cp  (token, fr='', to='', verbose=False):
    '''  Usage:  storeClient.cp (fr, to)
    '''
    return client._cp (fr=fr, to=to, token=def_token(token), verbose=verbose)

@multifunc('sc',0)
def cp  (token=None, fr='', to='', verbose=False):
    '''  Usage:  storeClient.cp (fr, to)
    '''
    return client._cp (fr=fr, to=to, token=def_token(token), verbose=verbose)


# --------------------------------------------------------------------
# LN -- Create a link to a file/directory in the store manager service
#
@multifunc('sc',3)
def ln  (token, fr, target, verbose=False):
    '''  Usage:  storeClient.ln (token, fr, target)
    '''
    return client._ln (fr=fr, target=target, token=def_token(token), 
                       verbose=verbose)

@multifunc('sc',2)
def ln  (fr, target, token=None, verbose=False):
    '''  Usage:  storeClient.ln (fr, target)
    '''
    return client._ln (fr=fr, target=target, token=def_token(token), 
                       verbose=verbose)

@multifunc('sc',1)
def ln  (token, fr='', target='', verbose=False):
    '''  Usage:  storeClient.ln (fr, target)
    '''
    return client._ln (fr=fr, target=target, token=def_token(token),
                       verbose=verbose)


# --------------------------------------------------------------------
# LS -- Get a file/directory listing from the store manager service
#
@multifunc('sc',2)
def ls  (token, name, format='csv', verbose=False):
    '''  Usage:  storeClient.ls (token, name)
    '''
    return client._ls (name=name, format=format, token=def_token(token),
                       verbose=verbose)

@multifunc('sc',1)
def ls  (optval, name='vos://', token=None, format='csv', verbose=False):
    '''  Usage:  storeClient.ls (name)
    '''
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return client._ls(name='vos://', format=format, token=def_token(optval),
                          verbose=verbose)
    else:
        return client._ls(name=optval, format=format, token=def_token(None),
                          verbose=verbose)

@multifunc('sc',0)
def ls  (name='vos://', token=None, format='csv', verbose=False):
    '''  Usage:  storeClient.ls ()
    '''
    return client._ls (name=name, format=format, token=def_token(token),
                       verbose=verbose)


# --------------------------------------------------------------------
# MKDIR -- Create a directory in the store manager service
#
@multifunc('sc',2)
def mkdir  (token, name):
    '''  Usage:  storeClient.mkdir (token, name)
    '''
    return client._mkdir (name=name, token=def_token(token))

@multifunc('sc',1)
def mkdir  (optval, name='', token=None):
    '''  Usage:  storeClient.mkdir (name)
    '''
    if optval is not None and len(optval.split('.')) >= 4:
        return client._mkdir (name=name, token=def_token(optval))
    else:
        return client._mkdir (name=optval, token=def_token(token))


# --------------------------------------------------------------------
# MV -- Move/rename a file/directory within the store manager service
#
@multifunc('sc',3)
def mv  (token, fr, to, verbose=False):
    '''  Usage:  storeClient.mv (token, fr, to)
    '''
    return client._mv (fr=fr, to=to, token=def_token(token), verbose=verbose)

@multifunc('sc',2)
def mv  (fr, to, token=None, verbose=False):
    '''  Usage:  storeClient.mv (fr, to)
    '''
    return client._mv (fr=fr, to=to, token=def_token(token), verbose=verbose)

@multifunc('sc',1)
def mv  (token, fr='', to='', verbose=False):
    '''  Usage:  storeClient.mv (fr, to)
    '''
    return client._mv (fr=fr, to=to, token=def_token(token), verbose=verbose)

@multifunc('sc',0)
def mv  (token=None, fr='', to='', verbose=False):
    '''  Usage:  storeClient.mv (fr, to)
    '''
    return client._mv (fr=fr, to=to, token=def_token(token), verbose=verbose)


# --------------------------------------------------------------------
# RM -- Delete a file from the store manager service
#
@multifunc('sc',2)
def rm  (token, name, verbose=False):
    '''  Usage:  storeClient.rm (token, name)
    '''
    return client._rm (name=name, token=def_token(token), verbose=verbose)

@multifunc('sc',1)
def rm  (optval, name='', token=None, verbose=False):
    '''  Usage:  storeClient.rm (name)
    '''
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return client._rm (name=name, token=def_token(optval), verbose=verbose)
    else:
        # optval is the name to be removed
        return client._rm (name=optval, token=def_token(token), verbose=verbose)

@multifunc('sc',0)
def rm  (name='', token=None, verbose=False):
    '''  Usage:  storeClient.rm (name)
    '''
    return client._rm (name=name, token=def_token(token), verbose=verbose)


# --------------------------------------------------------------------
# RMDIR -- Delete a directory from the store manager service
#
@multifunc('sc',2)
def rmdir  (token, name, verbose=False):
    '''  Usage:  storeClient.rmdir (token, name)
    '''
    return client._rmdir (name=name, token=def_token(token), verbose=verbose)

@multifunc('sc',1)
def rmdir  (optval, name='', token=None, verbose=False):
    '''  Usage:  storeClient.rmdir (name)
    '''
    return client._rmdir (name=name, token=def_token(token), verbose=verbose)
    if optval is not None and len(optval.split('.')) >= 4:
        return client._rmdir (name=name, token=def_token(optval),
                            verbose=verbose)
    else:
        return client._rmdir (name=optval, token=def_token(token),
                            verbose=verbose)

@multifunc('sc',0)
def rmdir  (name='', token=None, verbose=False):
    '''  Usage:  storeClient.rm (name)
    '''
    return client._rmdir (name=name, token=def_token(token), verbose=verbose)


# --------------------------------------------------------------------
# SAVEAS -- Save the string representation of a data object as a file.
#
@multifunc('sc',3)
def saveAs  (token, data, name):
    '''  Usage:  storeClient.saveAs (token, data, name)
    '''
    return client._saveAs (data=data, name=name, token=def_token(token))

@multifunc('sc',2)
def saveAs  (data, name, token=None):
    '''  Usage:  storeClient.saveAs (data, name)
    '''
    return client._saveAs (data=data, name=name, token=def_token(token))


# --------------------------------------------------------------------
# TAG -- Annotate a file/directory in the store manager service
#
@multifunc('sc',3)
def tag  (token, name, tag):
    '''  Usage:  storeClient.tag (token, name, tag)
    '''
    return client._tag (name=name, tag=tag, token=def_token(token))

@multifunc('sc',2)
def tag  (name, tag, token=None):
    '''  Usage:  storeClient.tag (name, tag)
    '''
    return client._tag (name=name, tag=tag, token=def_token(token))

@multifunc('sc',1)
def tag  (token, name='', tag=''):
    '''  Usage:  storeClient.tag (token, name='foo', tag='bar')
    '''
    return client._tag (name=name, tag=tag, token=def_token(token))


# --------------------------------------------------------------------
# LOAD/PULL -- Load a file from a remote endpoint to the store manager service
#
@multifunc('sc',3)
def load  (token, name, endpoint):
    '''  Usage:  storeClient.load (token, name, endpoint)
    '''
    return client._load (name=name, endpoint=endpoint, token=def_token(token))

@multifunc('sc',2)
def load  (name, endpoint, token=None):
    '''  Usage:  storeClient.load (name, endpoint)
    '''
    return client._load (name=name, endpoint=endpoint, token=def_token(token))

# Aliases for load() calls.

@multifunc('sc',3)
def pull  (token, name, endpoint):
    '''  Usage:  storeClient.pull (token, name, endpoint)
    '''
    return client._load (name=name, endpoint=endpoint, token=def_token(token))

@multifunc('sc',2)
def pull  (name, endpoint, token=None):
    '''  Usage:  storeClient.pull (name, endpoint)
    '''
    return client._load (name=name, endpoint=endpoint, token=def_token(token))



# ####################################################################
#  Module Functions
# ####################################################################

class storeClient (object):
    """
         STORECLIENT -- Client-side methods to access the Data Lab
                        Storage Manager Service.
    """
    def __init__ (self, profile=DEF_PROFILE, svc_url=DEF_SERVICE_URL):
        """ Initialize the store client object. 
        """
        self.svc_url = svc_url                  # StoreMgr service URL
        self.svc_profile = profile              # StoreMgr service profile

        self.hostip = THIS_IP
        self.hostname = THIS_HOST
        self.async_wait = False

        # Get the $HOME/.datalab directory.
        self.home = '%s/.datalab' % os.path.expanduser('~')

        self.debug = DEBUG                      # interface debug flag


    # --------------------------------------------------------------------
    # ISALIVE -- Ping the Query Manager service to see if it responds.
    #
    def isAlive (self, svc_url=None, timeout=2):
        """ Check whether the StorageManager service at the given URL is
            alive and responding.  This is a simple call to the root 
            service URL or ping() method.
    
        Parameters
        ----------
        svc_url : str
            The service URL of the storage manager to use 
        
        Returns
        -------
    
        Example
        -------
        .. code-block:: python
    
            # Check if service is responding
            if storeClient.isAlive():
               .....stuff 
        """
        if svc_url is None:
            svc_url = self.svc_url

        try:
            r = requests.get (svc_url, timeout=timeout)
            resp = r.content
            if r.status_code != 200:
                return False
            elif resp is not None and r.text.lower()[:11] != "hello world":
                return False
        except Exception:
            return False

        return True
    
    def set_svc_url (self, svc_url):
        """ Set the Storage Manager service URL.
    
        Parameters
        ----------
        svc_url : str
            The service URL of the Storage Manager to use 
        
        Returns
        -------
        Nothing
    
        Example
        -------
        .. code-block:: python
    
            storeClient.set_scv_url("http://dldemo.sdm.noao.edu:7003")
        """
        self.svc_url = svc_url
    
    def get_svc_url (self):
        """ Return the currently-used Storage Manager service URL.
    
        Parameters
        ----------
            None
    
        Returns
        -------
            Current Query Manager service URL
    
        Example
        -------
        .. code-block:: python
    
            print (storeClient.get_scv_url())
        """
        return self.svc_url
    
    def set_profile (self, profile):
        """ Set the service profile to be used.
    
        Parameters
        ----------
        profile : str
            The name of the profile to use. The list of available ones can
            be retrieved from the service (see function 
            :func:`storeClient.list_profiles()`)
    
        Returns
        -------
        Nothing
        
        Example
        -------
        .. code-block:: python
    
            storeClient.set_profile('test')
        """
        
        self.svc_profile = profile
            
    def get_profile (self):
        """ Get the profile
    
        Parameters
        ----------
        None
    
        Returns
        -------
        profile : str
            The name of the current profile used with the Storage Manager
            
        Example
        -------
        .. code-block:: python
    
            print ('Store Service profile = ' + storeClient.get_profile())
        """
        return self.svc_profile
            
        
    @multimethod('sc',1)
    def list_profiles  (self, token, profile=None, format='text'):
        '''  Usage:  storeClient.list_profiles (token, ....)
        '''
        return self._list_profiles (token=def_token(token), profile=profile,
                                      format=format)
    
    @multimethod('sc',0)
    def list_profiles  (self, token=None, profile=None, format='text'):
        '''  Usage:  storeClient.list_profiles (....)
        '''
        return self._list_profiles (token=def_token(token), profile=profile,
                                      format=format)
    
    def _list_profiles (self, token=None, profile=None, format='text'):
        """ Retrieve the profiles supported by the storage manager service

        Parameters
        ----------
        token : str
            Authentication token (see function :func:`authClient.login()`)
    
        profile : str
            A specific profile to list
            
        Returns
        -------
        profiles : list/dict
            A list of the names of the supported profiles or a dictionary of the
            specific profile
        
        Example
        -------
        .. code-block:: python
    
            # get the list of profiles
            profiles = storeClient.list_profiles (token)
        """

        dburl = '/profiles?' 
        if profile != None and profile != 'None' and profile != '':
            dburl += "profile=%s&" % profile
        dburl += "format=%s" % format
        
        r = getFromURL(self.svc_url, dburl, def_token(token))
        profiles = r.content
        if '{' in profiles:
            profiles = json.loads(profiles)

        return profiles
        


    # -----------------------------
    #  Utility Methods
    # -----------------------------

    # --------------------------------------------------------------------
    # ACCESS -- Determine whether the file can be accessed with the given node.
    #
    @multimethod('sc',3)
    def access (self, token, path, mode, verbose=True):
        '''  Usage:  storeClient.access (token, path, mode)
        '''
        return self._access (path=path, mode=mode, token=def_token(token), 
                             verbose=verbose)

    @multimethod('sc',2)
    def access (self, path, mode, token=None, verbose=True):
        '''  Usage:  storeClient.access (path, mode)
        '''
        return self._access (path=path, mode=mode, token=def_token(token), 
                             verbose=verbose)

    @multimethod('sc',1)
    def access (self, path, mode=None, token=None, verbose=True):
        '''  Usage:  storeClient.access (path, mode)
        '''
        return self._access (path=path, mode=mode, token=def_token(token), 
                             verbose=verbose)

    def _access (self, path='', mode='', token=None, verbose=True):
        """ Determine whether the file can be accessed with the given node.

        Parameters
        ----------
        path : str
            A name or file template of the file status to retrieve.
            
        mode : str
            Requested access mode.  Modes are 'r' (read access), 'w' (write
            access), or 'rw' to test for both read/write access.  If mode
            is None a simple existence check is made.
            
        token : str
            Authentication token (see function :func:`authClient.login()`)
    
        verbose : bool
            Verbose output flag.
            
        Returns
        -------
        result : bool
            True if the node can be access with the requested mode.

        Example
        -------
        .. code-block:: python
    
            if storeClient.access ('/mydata.csv')
                print ('File exists')
            elif storeClient.access ('/mydata.csv','rw')
                print ('File is both readable and writable')
        """
    
        url = self.svc_url + ("/access?name=%s&mode=%s&verbose=%s" % \
                         (path,mode,verbose))
        r = requests.get(url, headers={'X-DL-AuthToken': def_token(token)})
        if r.status_code != 200:
            return False
        else:
            return (True if r.content.lower() == 'true' else False)
        pass


    # --------------------------------------------------------------------
    # STAT -- Get file status. Values are returned as a dictionary of the
    #         requested node.
    #
    @multimethod('sc',2)
    def stat (self, token, path, verbose=True):
        '''  Usage:  storeClient.stat (token, path)
        '''
        return self._stat (path=path, token=def_token(token), verbose=verbose)

    @multimethod('sc',1)
    def stat (self, path, token=None, verbose=True):
        '''  Usage:  storeClient.stat (path)
        '''
        return self._stat (path=path, token=def_token(token), verbose=verbose)

    def _stat (self, path='', token=None, verbose=True):
        """ Get file status information, similar to stat().

        Parameters
        ----------
        path : str
            A name or file template of the file status to retrieve.
            
        token : str
            Authentication token (see function :func:`authClient.login()`)
    
        verbose : bool
            Verbose output flag.
            
        Returns
        -------
        stat : dictionary
            A dictionary of node status values.  Returned fields include:

                name		Name of node
                groupread	List of group/owner names w/ read access
                groupwrite	List of group/owner names w/ write access
                publicread	Publicly readable (0=False, 1=True)
                owner		Owner name
                perms		Formatted unix-like permission string
                target		Node target if LinkNode
                size		Size of file node (bytes)
                type		Node type (container|data|link)

        Example
        -------
        .. code-block:: python
    
            # get status information for a specific node
            stat = storeClient.stat ('vos://mydata.csv')
    
            if stat['type'] == 'container':
                print ('This is a directory')
            else:
                print ('File size is: ' + stat['size'])
        """
    
        url = self.svc_url + ("/stat?name=%s&verbose=%s" % (path,verbose))
        r = requests.get(url, headers={'X-DL-AuthToken': def_token(token)})
        if r.status_code != 200:
            return {}
        else:
            return json.loads(r.content)


    # --------------------------------------------------------------------
    # GET -- Retrieve a file from the store manager service
    # --------------------------------------------------------------------
    @multimethod('sc',3)
    def get (self, token, fr, to, verbose=True, debug=False):
        '''  Usage:  storeClient.get (token, fr, to)
        '''
        return self._get (fr=fr, to=to, token=def_token(token), 
                          verbose=verbose, debug=debug)

    @multimethod('sc',2)
    def get (self, opt1, opt2, fr='', to='', token=None, verbose=True,
             debug=False):
        '''  Usage:  storeClient.get (fr, to)
        '''
        if opt1 is not None and len(opt1.split('.')) >= 4:
            # opt1 looks like a token
            return self._get (fr=opt2, to=to, token=def_token(opt1),
                              verbose=verbose, debug=debug)
        else:
            # opt1 is the 'fr' value, opt2 is the 'to' value
            return self._get (fr=opt1, to=opt2, token=def_token(token),
                              verbose=verbose, debug=debug)

    @multimethod('sc',1)
    def get (self, optval, fr='', to='', token=None, verbose=True, debug=False):
        '''  Usage:  storeClient.get (fr)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self._get (fr=fr, to=to, token=def_token(optval), 
                              verbose=verbose, debug=debug)
        else:
            # optval is the 'fr' value
            return self._get (fr=optval, to=to, token=def_token(token), 
                              verbose=verbose, debug=debug)

    @multimethod('sc',0)
    def get (self, fr='', to='', token=None, verbose=True, debug=False):
        '''  Usage:  storeClient.get (token, fr, to)
        '''
        return self._get (fr=fr, to=to, token=def_token(token), 
                          verbose=verbose, debug=debug)

    def _get (self, token=None, fr='', to='', verbose=True, debug=False):
        """ Retrieve a file from the store manager service

        Parameters
        ----------
        token : str
            Authentication token (see function :func:`authClient.login()`)
    
        fr : str
            A name or file template of the file(s) to retrieve.
            
        to : str
            Name of the file(s) to locally.  If not specified, the contents
            of the file are returned to the caller.
            
        Returns
        -------
        result : str
            A list of the names of the files retrieved, or the contents of 
            a single file.
        
        Example
        -------
        .. code-block:: python
    
            # get a single file to a local file of a different name
            data = storeClient.get ('vos://mydata.csv', 'data.csv')
    
            # get the contents of a single file to a local variable
            data = storeClient.get ('vos://mydata.csv')
    
            # get a list of remote files to a local directory
            flist = storeClient.get ('vos://*.fits', './data/')
            flist = storeClient.get ('*.fits', './data/')
        """
    
        def sizeof_fmt(num):
            ''' Local pretty-printer for file sizes.
            '''
            for unit in ['B','K','M','G','T','P','E','Z']:
                if abs(num) < 1024.0:
                    if unit == 'B':
                        return "%5d%s" % (num, unit)
                    else:
                        return "%3.1f%s" % (num, unit)
                num /= 1024.0
            return "%.1f%s" % (num, 'Y')
    

        headers = {'X-DL-AuthToken': def_token(token)}

        # Patch the names with the default URI prefix if needed.
        nm = (fr if fr.count("://") > 0 else ("vos://" + fr))
        nm = nm.replace('///','//')
    
        if debug:
            print ("get(): nm = %s" % nm)
        if hasmeta(fr):
            if not os.path.exists(to):
                raise storeClientError ( "Download directory does not exist")
            if not os.path.isdir(to):
                raise storeClientError (
                          "Location must be specified as a directory")
            if to == '' or to is None:
                raise storeClientError(
                          "Multi-file requests require a download location")
    
        if to != '' and to is not None:
            # Expand metacharacters to create a file list for download.
            flist = expandFileList (self.svc_url, token, nm, "csv", full=True)
            if debug: 
                print ("get: flist = %s" % flist)

            nfiles = len(flist)
            fnum = 1
            resp = []
            for f in flist:
                # Generate the download file path.
                junk, fn = os.path.split(f)
                if to.endswith("/"):
                    dlname = ((to + fn) if hasmeta(fr) else to)
                else:
                    dlname = ((to + "/" + fn) if hasmeta(fr) else to)
    
                # Get a single file.
                res = requests.get(self.svc_url + "/get?name=%s" % f,
                                   headers=headers)
    
                if res.status_code != 200:
                    resp.append ("Error: " + res.text)
                else:
                    r = requests.get(res.text, stream=True)
                    clen = r.headers.get('content-length')
                    total_length = (0 if clen is None else int(clen))
    
                    # Download the file in chunks so we can have a progress
                    # indicator on each.
                    dl = 0
                    done = 0
                    with open(dlname, 'wb', 0) as fd:
                        for chunk in r.iter_content(chunk_size=1024):
                            dl += len (chunk)
                            if chunk:
                                fd.write(chunk)
                            if total_length > 0:
                                done = int(20 * dl / total_length)
    
                            if verbose:
                                # Print a progress indicator
                                sys.stdout.write("\r(%d/%d) [%s%s] [%7s] %s" % \
                                    (fnum, nfiles, '=' * done, ' ' * (20-done),
                                    sizeof_fmt(dl), f[6:]))
                                sys.stdout.flush()
    
                        # Handle a zero-length file download.
                        if verbose:
                            if dl == 0:
                                print ("\r(%d/%d) [%s] [%7s] %s" % \
                                    (fnum, nfiles, '=' * 20, "0 B", f[6:]))
                            else:
                                print('')
                    fd.close()
                    resp.append ("OK")
                fnum += 1
    
            return resp
    
        else:
            # Get a single file, return the raw contents to the caller.
            url = requests.get(self.svc_url + "/get?name=%s" % nm,
                               headers=headers)
            r = requests.get(url.text, stream=False, headers=headers)
            return r.content
        
        
    # --------------------------------------------------------------------
    # PUT -- Upload a file to the store manager service
    # --------------------------------------------------------------------
    @multimethod('sc',3)
    def put (self, token, fr, to, verbose=True, debug=False):
        '''  Usage:  storeClient.put (token, fr, to)
        '''
        return self._put (fr=fr, to=to, token=def_token(token), 
                          verbose=verbose, debug=False)

    @multimethod('sc',2)
    def put (self, fr, to, token=None, verbose=True, debug=False):
        '''  Usage:  storeClient.put (fr, to)
        '''
        return self._put (fr=fr, to=to, token=def_token(token), 
                          verbose=verbose, debug=False)

    @multimethod('sc',1)
    def put (self, optval, fr='', to='vos://', token=None, verbose=True,
             debug=False):
        '''  Usage:  storeClient.put (fr)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self._put (fr=fr, to=to, token=def_token(optval), 
                              verbose=verbose, debug=False)
        else:
            # optval looks like a source name
            return self._put (fr=optval, to=to, token=def_token(token), 
                              verbose=verbose, debug=False)

    @multimethod('sc',0)
    def put  (self,fr='', to='vos://', token=None, verbose=True, debug=False):
        '''  Usage:  storeClient.put (fr='',to='')
        '''
        return self._put (fr=fr, to=to, token=def_token(token),
                          verbose=verbose, debug=debug)

    def _put (self, token=None, fr='', to='vos://', verbose=True, debug=False):
        """ Upload a file to the store manager service

        Parameters
        ----------
        token : str
            Authentication token (see function :func:`authClient.login()`)
    
        fr : str
            Name or template of local files to upload.
            
        to : str
            Name of the file for destination directory on remote VOSpace.
            
        Returns
        -------
        result : str
            An 'OK' message or error string for each uploaded file.
        
        Example
        -------
        .. code-block:: python
    
            # get the contents of a single file
        """
        headers = {'X-DL-AuthToken': def_token(token)}
    
        # If the 'fr' is a directory, create it first and then transfer the
        # contents.
        if os.path.isdir (fr):
            if fr.endswith("/"):
                dname = (to if to.count("://") > 0 else to[:-1])
                self._mkdir  (token=token, name=dname)
            flist = glob.glob(fr+"/*")
        else:
            dname = ''
            flist = glob.glob(fr)

        if debug:
            print ("fr=%s  to=%s  dname=%s" % (fr, to, dname))
            print (flist)

        nfiles = len(flist)
        fnum = 1
        resp = []
        for f in flist:
            if debug:
                print ("put: f=%s" % (f))
            fr_dir, fr_name = os.path.split(f)
    
            # Patch the names with the URI prefix if needed.
            nm = (to if to.count("://") > 0 else ("vos://" + to))
            if to.endswith("/"):
                nm = nm + fr_name
            if is_vosDir(self.svc_url, token, nm):
                nm = nm + '/' + fr_name
            nm = nm.replace('///','//')      # fix extra path indicators
    
            if debug:
                print ("put: f=%s  nm=%s" % (f,nm))
    
            if not os.path.exists(f):
                # Skip files that don't exist
                if verbose:
                    print ("Error: Local file '%s' does not exist" % f)
                continue
    
            r = requests.get (self.svc_url + "/put?name=%s" % nm,
                              headers=headers)

            # Cannot upload directly to a container
            # if r.status_code == 500 and \
            #    r.content == "Data cannot be uploaded to a container":
            if r.status_code == 500:
                file = fr[fr.rfind('/') + 1:]
                nm += '/%s' % f
                r = requests.get(self.svc_url + "/put?name=%s" % nm, 
                                    headers=headers)
            try:
                if verbose:
                    sys.stdout.write ("(%d / %d) %s -> " % (fnum, nfiles, f))

                # This *should* work for large data files - MJG 05/24/17
                with open(f, 'rb') as file:
                    requests.put(r.content, data=file,
                         headers={'Content-type': 'application/octet-stream',
                                  'X-DL-AuthToken': token})
                if verbose:
                    sys.stdout.write ("%s\n" % nm)
    
            except Exception as e:
                resp.append (e.message)
            else:
                resp.append ("OK")
    
            fnum += 1

        return (str(resp) if len(resp) > 1 else resp[0])
            
    
    # --------------------------------------------------------------------
    # LOAD -- Load a file from a remote endpoint to the store manager service
    # --------------------------------------------------------------------
    @multimethod('sc',3)
    def load  (self, token, name, endpoint):
        '''  Usage:  storeClient.load (token, name, endpoint)
        '''
        return self._load (name=name, endpoint=endpoint, token=def_token(token))

    @multimethod('sc',2)
    def load  (self, name, endpoint, token=None):
        '''  Usage:  storeClient.load (name, endpoint)
        '''
        return self._load (name=name, endpoint=endpoint, token=def_token(token))

    # Aliases for load() calls.

    @multimethod('sc',3)
    def pull  (self, token, name, endpoint):
        '''  Usage:  storeClient.pull (token, name, endpoint)
        '''
        return self._load (name=name, endpoint=endpoint, token=def_token(token))

    @multimethod('sc',2)
    def pull  (self, name, endpoint, token=None):
        '''  Usage:  storeClient.pull (name, endpoint)
        '''
        return self._load (name=name, endpoint=endpoint, token=def_token(token))

    def _load (self, token=None, name='', endpoint=''):
        """ Load a file from a remote endpoint to the Store Manager service

        Parameters
        ----------
        token : str
            Authentication token (see function :func:`authClient.login()`)
    
        name : str
            Name or template of local files to upload.
            
        endpoint : str
            Name of the file for destination directory on remote VOSpace.
            
        Returns
        -------
        result : str
            An 'OK' message or error string
        
        Example
        -------
        .. code-block:: python
    
            # Load a file from a remote URL
            storeClient.load ('mydata.vot', 'http://example.com/data.vot')
        """
        r = getFromURL(self.svc_url, "/load?name=%s&endpoint=%s" % \
                       (name, endpoint), def_token(token))
        return r
    
        
    # --------------------------------------------------------------------
    # CP -- Copy a file/directory within the store manager service
    # --------------------------------------------------------------------
    @multimethod('sc',3)
    def cp  (self, token, fr, to, verbose=False):
        '''  Usage:  storeClient.cp (token, fr, to)
        '''
        return self._cp (fr=fr, to=to, token=def_token(token), verbose=verbose)

    @multimethod('sc',2)
    def cp  (self, fr, to, token=None, verbose=False):
        '''  Usage:  storeClient.cp (fr, to)
        '''
        return self._cp (fr=fr, to=to, token=def_token(token), verbose=verbose)

    @multimethod('sc',1)
    def cp  (self, token, fr='', to='', verbose=False):
        '''  Usage:  storeClient.cp (fr, to)
        '''
        return self._cp (fr=fr, to=to, token=def_token(token), verbose=verbose)

    @multimethod('sc',0)
    def cp  (self, token=None, fr='', to='', verbose=False):
        '''  Usage:  storeClient.cp (fr, to)
        '''
        return self._cp (fr=fr, to=to, token=def_token(token), verbose=verbose)

    def _cp (self, token=None, fr='', to='', verbose=False):
        """ Copy a file/directory within the store manager service

        Parameters
        ----------
        token : str
            Authentication token (see function :func:`authClient.login()`)
    
        fr : str
            Name of the file to be copied (may not be a directory).
            
        to : str
            Name of the file to be created
            
        Returns
        -------
        result : str
            An 'OK' message or error string for each uploaded file.
        
        Example
        -------
        .. code-block:: python
    
            # Copy a file in vospace
            storeClient.cp ('foo', 'bar')
            storeClient.cp ('vos://foo', 'vos:///new/bar')
        """
        # Patch the names with the URI prefix if needed.
        src = (fr if fr.count("://") > 0 else ("vos://" + fr))
        dest = (to if to.count("://") > 0 else ("vos://" + to))
    
        # If the 'from' string has no metacharacters we're copying a single file,
        # otherwise expand the file list and process the matches individually.
        if not hasmeta(fr):
            r = getFromURL(self.svc_url, "/cp?from=%s&to=%s" % (src, dest),
                           def_token(token))
            return r
        else:
            flist = expandFileList (self.svc_url, token, src, "csv", full=True)
            nfiles = len(flist)
            fnum = 1
            resp = []
            for f in flist:
                junk, fn = os.path.split (f)
                to_fname = dest + ('/%s' % fn)
                to_fname = to_fname.replace('///','//')
                if verbose:
                    print ("(%d / %d) %s -> %s" % (fnum, nfiles, f, to_fname))
                r = getFromURL(self.svc_url, "/cp?from=%s&to=%s" % \
                               (f, to_fname), def_token(token))
                fnum += 1
                resp.append(r)
            return resp
    
    
    # --------------------------------------------------------------------
    # LN -- Create a link to a file/directory in the store manager service
    # --------------------------------------------------------------------
    @multimethod('sc',3)
    def ln  (self, token, fr, target, verbose=False):
        '''  Usage:  storeClient.ln (token, fr, target)
        '''
        return self._ln (fr=fr, target=target, token=def_token(token),
                           verbose=verbose)

    @multimethod('sc',2)
    def ln  (self, fr, target, token=None, verbose=False):
        '''  Usage:  storeClient.ln (fr, target)
        '''
        return self._ln (fr=fr, target=target, token=def_token(token),
                           verbose=verbose)

    @multimethod('sc',1)
    def ln  (self, token, fr='', target='', verbose=False):
        '''  Usage:  storeClient.ln (fr, target)
        '''
        return self._ln (fr=fr, target=target, token=def_token(token),
                           verbose=verbose)

    def _ln (self, token=None, fr='', target='', verbose=True):
        """ Create a link to a file/directory in the store manager service

        Parameters
        ----------
        token : str
            Authentication token (see function :func:`authClient.login()`)
    
        fr : str
            Name of the file to be linked (may not be a directory).
            
        to : str
            Name of the link to be created
            
        Returns
        -------
        result : str
            An 'OK' message or error string for each uploaded file.
        
        Example
        -------
        .. code-block:: python
    
            # Link a file in vospace
            storeClient.ln ('foo', 'bar')
            storeClient.ln ('vos://foo', 'vos:///new/bar')
        """
        try:
            r = getFromURL(self.svc_url, "/ln?from=%s&to=%s" % (fr, target),
                           def_token(token))
        except Exception:
            raise storeClientError(r.content)
        else:
            return "OK"


    # --------------------------------------------------------------------
    # LS -- Get a file/directory listing from the store manager service
    # --------------------------------------------------------------------
    @multimethod('sc',2)
    def ls  (self, token, name, format='csv', verbose=False):
        '''  Usage:  storeClient.ls (token, name)
        '''
        return self._ls (name=name, format=format, token=def_token(token),
                         verbose=verbose)

    @multimethod('sc',1)
    def ls  (self, name, token=None, format='csv', verbose=False):
        '''  Usage:  storeClient.ls (name)
             Usage:  storeClient.ls (token, name='foo')
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self._ls (name='vos://', format=format, 
                             token=def_token(optval), verbose=verbose)
        else:
            return self._ls (name=optval, format=format, token=def_token(None),
                             verbose=verbose)

    @multimethod('sc',0)
    def ls  (self, name='vos://', token=None, format='csv', verbose=False):
        '''  Usage:  storeClient.ls ()
        '''
        return self._ls (name=name, format=format, token=def_token(token))

    def _ls (self, token=None, name='', format='csv', verbose=False):
        """
            Get a file/directory listing from the store manager service
    
        Parameters
        ----------
        token : str
            Secure token obtained via :func:`authClient.login`
    
        name : str
            Valid name of file or directory, e.g. ``vos://somedir``
            .. todo:: [20161110] currently doesn't seem to work.
    
        format : str
            Default ``str``.
    
        Example
        -------
        .. code-block:: python
    
            listing = storeClient.ls (token, name='vos://somedir')
            listing = storeClient.ls (token, 'vos://somedir')
            listing = storeClient.ls ('vos://somedir')
            print (listing)
    
        This prints for instance:
    
        .. code::
    
            bar2.fits,foo1.csv,fancyfile.dat
    
        """

        try:
            uri = (name if name.count('://') > 0 else 'vos://' + name)
            r = getFromURL(self.svc_url, 
                           "/ls?name=%s&format=%s&verbose=%s" % \
                           (uri, format, verbose), def_token(token))
        except Exception as e:
            raise Exception (r.content)
        return (r.content)


    # --------------------------------------------------------------------
    # MKDIR -- Create a directory in the store manager service
    # --------------------------------------------------------------------
    @multimethod('sc',2)
    def mkdir (self, token, name):
        '''  Usage:  storeClient.mkdir (token, name)
        '''
        return self._mkdir (name=name, token=def_token(token))

    @multimethod('sc',1)
    def mkdir (self, optval, name='', token=None):
        '''  Usage:  storeClient.mkdir (name)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            return self._mkdir (name=name, token=def_token(optval))
        else:
            return self._mkdir (name=optval, token=def_token(token))

    def _mkdir (self, token=None, name=''):
        """ Make a directory in the storage manager service

        Parameters
        ----------
        token : str
            Authentication token (see function :func:`authClient.login()`)
    
        name : str
            Name of the container (directory) to create.
            
        Returns
        -------
        result : str
            An 'OK' message or error string for each uploaded file.
        
        Example
        -------
        .. code-block:: python
    
            # Create a directory in vospace
            storeClient.mkdir ('foo')
        """
        nm = (name if name.count("://") > 0 else ("vos://" + name))
        
        try:
            r = getFromURL(self.svc_url, "/mkdir?dir=%s" % nm, def_token(token))
        except Exception:
            raise storeClientError(r.content)
        else:
            return "OK"
            
        
    # --------------------------------------------------------------------
    # MV -- Move/rename a file/directory within the store manager service
    # --------------------------------------------------------------------
    @multimethod('sc',3)
    def mv  (self, token, fr, to, verbose=False):
        '''  Usage:  storeClient.mv (token, fr, to)
        '''
        return self._mv (fr=fr, to=to, token=def_token(token), verbose=verbose)

    @multimethod('sc',2)
    def mv  (self, fr, to, token=None, verbose=False):
        '''  Usage:  storeClient.mv (fr, to)
        '''
        return self._mv (fr=fr, to=to, token=def_token(token), verbose=verbose)

    @multimethod('sc',1)
    def mv  (self, token, fr='', to='', verbose=False):
        '''  Usage:  storeClient.mv (fr, to)
        '''
        return self._mv (fr=fr, to=to, token=def_token(token), verbose=verbose)

    @multimethod('sc',0)
    def mv  (self, token=None, fr='', to='', verbose=False):
        '''  Usage:  storeClient.mv (fr, to)
        '''
        return self._mv (fr=fr, to=to, token=def_token(token), verbose=verbose)

    def _mv (self, token=None, fr='', to='', verbose=False):
        """ Move/rename a file/directory within the store manager service

        Parameters
        ----------
        token : str
            Authentication token (see function :func:`authClient.login()`)
    
        fr : str
            Name of the file to be moved
            
        to : str
            Name of the file or directory to move the 'fr' file.  If given
            as a directory the original filename is preserved.
            
        Returns
        -------
        result : str
            An 'OK' message or error string for each uploaded file.
        
        Example
        -------
        .. code-block:: python
    
            # Move a file in vospace
            storeClient.mv ('foo', 'bar')             # rename file
            storeClient.mv ('foo', 'vos://newdir/')   # move to new directory
            storeClient.mv ('foo', 'newdir')
        """
        # Patch the names with the URI prefix if needed.
        src = (fr if fr.count("://") > 0 else ("vos://" + fr))
        dest = (to if to.count("://") > 0 else ("vos://" + to))
    
        # If the 'from' string has no metacharacters we're copying a single file,
        # otherwise expand the file list on the and process the matches
        # individually.
        if not hasmeta(fr):
            r = getFromURL(self.svc_url, "/mv?from=%s&to=%s" % (src, dest),
                           def_token(token))
            return r
        else:
            flist = expandFileList (self.svc_url, token, src, "csv", full=True)
            nfiles = len(flist)
            fnum = 1
            resp = []
            for f in flist:
                junk, fn = os.path.split (f)
                to_fname = dest + ('/%s' % fn)
                if to_fname[:-1] != 'vos://':
                    to_fname = to_fname.replace('///','//')
                if verbose:
                    print ("(%d / %d) %s -> %s" % (fnum, nfiles, f, to_fname))
                r = getFromURL(self.svc_url, "/mv?from=%s&to=%s" % (f,to_fname),
                               def_token(token))
                fnum += 1
                resp.append(r)
            return resp    
    
        
    # --------------------------------------------------------------------
    # RM -- Delete a file from the store manager service
    # --------------------------------------------------------------------
    @multimethod('sc',2)
    def rm  (self, token, name, verbose=False):
        '''  Usage:  storeClient.rm (token, name)
        '''
        return self._rm (name=name, token=def_token(token), verbose=verbose)

    @multimethod('sc',1)
    def rm  (self, optval, name='', token=None, verbose=False):
        '''  Usage:  storeClient.rm (name)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self._rm (name=name,token=def_token(optval),verbose=verbose)
        else:
            return self._rm (name=optval,token=def_token(token),verbose=verbose)

    @multimethod('sc',0)
    def rm  (self, name='', token=None, verbose=False):
        '''  Usage:  storeClient.rm (name)
        '''
        return self._rm (name=name, token=def_token(token), verbose=verbose)

    def _rm (self, token=None, name='', verbose=False):
        """ Delete a file from the store manager service

        Parameters
        ----------
        token : str
            Authentication token (see function :func:`authClient.login()`)
    
        name : str
            Name of the file to delete
            
        Returns
        -------
        result : str
            An 'OK' message or error string for each uploaded file.
        
        Example
        -------
        .. code-block:: python
    
            # Remove a file from vospace
            storeClient.rm ('foo.csv')
            storeClient.rm ('vos://foo.csv')
        """
        
        # Patch the names with the URI prefix if needed.
        nm = (name if name.count("://") > 0 else ("vos://" + name))
        if nm == "vos://" or nm == "vos://tmp" or nm == "vos://public":
            return "Error: operation not permitted"
    
        # If the 'name' string has no metacharacters we're copying a single file,
        # otherwise expand the file list on the and process the matches
        # individually.
        if not hasmeta(nm):
            r = getFromURL(self.svc_url, "/rm?file=%s" % nm, def_token(token))
            return r
        else:
            flist = expandFileList (self.svc_url, token, nm, "csv", full=True)
            nfiles = len(flist)
            fnum = 1
            resp = []
            for f in flist:
                if verbose:
                    print ("(%d / %d) %s" % (fnum, nfiles, f))
                r = getFromURL(self.svc_url, "/rm?file=%s" % f, 
                               def_token(token))
                fnum += 1
                resp.append(r)
            return resp
    
        
    # --------------------------------------------------------------------
    # RMDIR -- Delete a directory from the store manager service
    # --------------------------------------------------------------------
    @multimethod('sc',2)
    def rmdir (self, token, name, verbose=False):
        '''  Usage:  storeClient.rmdir (token, name)
        '''
        return self._rmdir (name=name, token=def_token(token), verbose=verbose)

    @multimethod('sc',1)
    def rmdir (self, optval, name='', token=None, verbose=False):
        '''  Usage:  storeClient.rmdir (name)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            return self._rmdir (name=name, token=def_token(optval),
                                verbose=verbose)
        else:
            return self._rmdir (name=optval, token=def_token(token),
                                verbose=verbose)

    @multimethod('sc',0)
    def rmdir  (self, name='', token=None, verbose=False):
        '''  Usage:  storeClient.rm (name)
        '''
        return self._rmdir (name=name, token=def_token(token), verbose=verbose)

    def _rmdir (self, token=None, name='', verbose=False):
        """ Delete a directory from the store manager service

        Parameters
        ----------
        token : str
            Authentication token (see function :func:`authClient.login()`)
    
        name : str
            Name of the container (directory) to delete.
            
        Returns
        -------
        result : str
            An 'OK' message or error string for each uploaded file.
        
        Example
        -------
        .. code-block:: python
    
            # Remove an empty directory from VOSpace.
            storeClient.rmdir ('datadir')
            storeClient.rmdir ('vos://datadir')
        """

        # FIXME - Should handle file templates, return Response objects

        # Patch the names with the URI prefix if needed.
        nm = (name if name.count("://") > 0 else ("vos://" + name))
        if nm == "vos://" or nm == "vos://tmp" or nm == "vos://public":
            return "Error: operation not permitted"
    
        try:        
            self._saveAs(token=def_token(token), data="deleted",
                         name=(nm+"/.deleted"))
            r = getFromURL(self.svc_url, "/rmdir?dir=%s" % nm, def_token(token))
        except Exception as e:
            raise storeClientError(e.message)
        else:
            return "OK"
            #return r


    # --------------------------------------------------------------------
    # SAVEAS -- Save the string representation of a data object as a file.
    # --------------------------------------------------------------------
    @multimethod('sc',3)
    def saveAs (self, token, data, name):
        '''  Usage:  storeClient.saveAs (token, data, name)
        '''
        return self._saveAs (data=data, name=name, token=def_token(token))

    @multimethod('sc',2)
    def saveAs (self, data, name, token=None):
        '''  Usage:  storeClient.saveAs (data, name)
        '''
        return self._saveAs (data=data, name=name, token=def_token(token))

    def _saveAs (self, token=None, data='', name=''):
        """ Save the string representation of a data object as a file.

        Parameters
        ----------
        token : str
            Authentication token (see function :func:`authClient.login()`)
    
        data : str
            Data object to be saved.
            
        name : str
            Name of the file to create containing the string representation
            of the data.
            
        Returns
        -------
        result : str
            An 'OK' message or error string for each uploaded file.
        
        Example
        -------
        .. code-block:: python
    
            # Save a data object to VOSpace
            storeClient.saveAs (pandas_data, 'pandas.example')
            storeClient.saveAs (json_data, 'json.example')
            storeClient.saveAs (table_data, 'table.example')
        """
        import tempfile
    
        try:
            with tempfile.NamedTemporaryFile(mode='w',delete=False) as tfd:
                tfd.write(str(data))
                tfd.flush()
                tfd.close()
        except Exception as e:
            raise storeClientError(e.message)
    
        # Patch the names with the URI prefix if needed.
        nm = (name if name.count("://") > 0 else ("vos://" + name))
    
        # Put the temp file to the VOSpace.
        resp = self._put (token=token, fr=tfd.name, to=nm, verbose=False)
    
        os.unlink(tfd.name)                # Clean up
    
        return resp


    # --------------------------------------------------------------------
    # TAG -- Annotate a file/directory in the store manager service
    # --------------------------------------------------------------------
    @multimethod('sc',3)
    def tag (self, token, name, tag):
        '''  Usage:  storeClient.tag (token, name, tag)
        '''
        return self._tag (name=name, tag=tag, token=def_token(token))

    @multimethod('sc',2)
    def tag (self, name, tag, token=None):
        '''  Usage:  storeClient.tag (name, tag)
        '''
        return self._tag (name=name, tag=tag, token=def_token(token))

    @multimethod('sc',1)
    def tag  (self, token, name='', tag=''):
        '''  Usage:  storeClient.tag (token, name='foo', tag='bar')
        '''
        return self._tag (name=name, tag=tag, token=def_token(token))

    def _tag (self, token=None, name='', tag=''):
        """ Annotate a file/directory in the store manager service

        Parameters
        ----------
        token : str
            Authentication token (see function :func:`authClient.login()`)
    
        name : str
            Name of the file to tag
            
        tag : str
            Annotation string for file
            
        Returns
        -------
        result : str
            An 'OK' message or error string for each uploaded file.
        
        Example
        -------
        .. code-block:: python
    
            # Annotate a file in vospace
            storeClient.tag ('foo.csv', 'This is a test')
        """
        try:
            r = getFromURL(self.svc_url, "/tag?name=%s&tag=%s" % (name, tag),
                           def_token(token))
        except Exception:
            raise storeClientError (r.content)
        else:
            if r.status_code == 200:
                return "OK"
            else:
                return r.content





# -------------------------------------------------------
#  Utility Methods
# -------------------------------------------------------

def hasmeta(s):
    """ Determine whether a string contains filename meta-characters.
    """
    return (s.find('*') >= 0) or (s.find('[') >= 0) or (s.find('?') > 0)


def is_vosDir (svc_url, token, path):
    """ Determine whether 'path' is a ContainerNode in the VOSpace.
    """
    url = svc_url + ("/isdir?name=%s" % (path))
    r = requests.get(url, headers={'X-DL-AuthToken': def_token(token)})
    if r.status_code != 200:
        return False
    else:
        return (True if r.content.lower() == 'true' else False)

def expandFileList (svc_url, token, pattern, format, full=False):
    """ Expand a filename pattern in a VOSpace URI to a list of files.  We
        do this by getting a listing of the parent container contents from
        the service and then match the pattern on the client side.
    """
    debug = False

    # The URI prefix is constant whether it's included in the pattern string
    # or not.  The SM sm controls a specific instance of VOSpace so at the
    # moment the expansiom to the VOSpace URI is handled on the server.  We'll
    # prepend this to the service call as needed to ensure a correct argument
    # and give the calling routine the option of leaving it off.
    if pattern.count('://') > 0:
        str = pattern[pattern.index('://')+3:]
        uri = pattern[:pattern.index('://')+3]
    else:
        str = pattern
        uri = 'vos://'

    # Extract the directory and filename/pattern from the string.
    dir, name = os.path.split(str)
    if debug:
        print ("-----------------------------------------")
        print ("INPUT PATTERN = '" + str + "'")
        print ("PATTERN = '" + str + "'")
        print ('str = ' + str)
        print ("split: '%s' '%s'" % (dir, name))

    pstr = (name if (name is not None and hasmeta(name)) else "*")

    if dir is not None:
        if dir == "/" and name is not None:
            dir = dir + name
        else:
            if dir.endswith("/"):
                dir = dir[:-1]        # trim trailing '/'
            if not dir.startswith("/"):
                dir = "/" + dir                         # prepend '/'
    else:
        dir = '/'
        if name is not None:
            dir = dir + name
    if dir == "/":
        dir = ""
        pstr = name
    if not hasmeta(name) and name is not None:
        pstr = (name if name != '' else "*")

    # Make the service call to get a listing of the parent directory.
    url = svc_url + "/ls?name=%s%s&format=%s" % (uri, dir, "csv")
    r = requests.get(url, headers={'X-DL-AuthToken': def_token(token)})

    # Filter the directory contents list using the filename pattern.
    list = []
    flist = r.content.split(',')
    for f in flist:
        if fnmatch.fnmatch(f, pstr) or f == pstr:
            furi = (f if not full else (uri + dir + "/" + f))
            list.append(furi.replace("///", "//"))

    if debug:
        print (url)
        print ("%s --> '%s' '%s' '%s' => '%s'" % (pattern,uri,dir,name,pstr))

    return sorted(list)
        

# Get from a URL
def getFromURL (svc_url, path, token):
    debug = False
    try:
        if debug:
            print ("url: %s" % (("%s%s" % (svc_url, path))))
            print ("token -  %s %s" % (token,def_token(token)))
        resp = requests.get("%s%s" % (svc_url, path), 
                            headers = {"X-DL-AuthToken": def_token (token)})
    except Exception as e:
        raise storeClientError(e.message)
    return resp



# ###################################
#  Store Client Handles
# ###################################

# GET_CLIENT -- Get a new storeClient object
#
def getClient (profile=DEF_PROFILE, svc_url=DEF_SERVICE_URL):
    '''  Create a new storeClient object and set a default profile.
    '''
    return storeClient (profile=profile, svc_url=svc_url)

# The default client handle for the module.
client = getClient (profile=DEF_PROFILE, svc_url=DEF_SERVICE_URL)

