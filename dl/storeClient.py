#!/usr/bin/env python
#
# STORECLIENT -- Client routines for the Data Lab Store Manager service
#

from __future__ import print_function

__authors__ = 'Matthew Graham <graham@noao.edu>, Mike Fitzpatrick <fitz@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20180321'  # yyyymmdd


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
        
    
# -----------------------------
#  Utility Functions
# -----------------------------

# --------------------------------------------------------------------
# LIST_PROFILES -- List the profiles supported by the storage manager service
#
@multifunc(1)
def list_profiles  (token, profile=None, format='text'):
    '''  Usage:  storeClient.list_profiles (token)
    '''
    return client._list_profiles (token=def_token(token), profile=profile,
                                  format=format)
    
@multifunc(0)
def list_profiles  (token=None, profile=None, format='text'):
    '''  Usage:  storeClient.list_profiles ()
    '''
    return client._list_profiles (token=def_token(token), profile=profile,
                                  format=format)
    

# --------------------------------------------------------------------
# GET -- Retrieve a file (or files) from the Store Manager service
#
@multifunc(3)
def get  (token, fr, to, verbose=True, debug=False):
    '''  Usage:  storeClient.get (token, fr, to)
    '''
    return client._get (fr=fr, to=to, token=def_token(token),
                        verbose=verbose, debug=debug)

@multifunc(2)
def get  (fr, to, token=None, verbose=True, debug=False):
    '''  Usage:  storeClient.get (fr, to)
    '''
    return client._get (fr=fr, to=to, token=def_token(token),
                        verbose=verbose, debug=debug)

@multifunc(1)
def get (fr, to='', token=None, verbose=True, debug=False):
    '''  Usage:  storeClient.get (fr)
    '''
    return client._get (fr=fr, to=to, token=def_token(token), 
                        verbose=verbose, debug=debug)


# --------------------------------------------------------------------
# PUT -- Upload a file (or files) to the Store Manager service
#
@multifunc(3)
def put  (token, fr, to, verbose=True, debug=False):
    '''  Usage:  storeClient.put (token, fr, to)
    '''
    return client._put (fr=fr, to=to, token=def_token(token),
                        verbose=verbose, debug=debug)

@multifunc(2)
def put  (fr, to, token=None, verbose=True, debug=False):
    '''  Usage:  storeClient.put (fr, to)
    '''
    return client._put (fr=fr, to=to, token=def_token(token),
                        verbose=verbose, debug=debug)

@multifunc(1)
def put  (fr, to='vos://', token=None, verbose=True, debug=False):
    '''  Usage:  storeClient.put (fr)
    '''
    return client._put (fr=fr, to=to, token=def_token(token),
                        verbose=verbose, debug=debug)


# --------------------------------------------------------------------
# CP -- Copy a file/directory within the store manager service
#
@multifunc(3)
def cp  (token, fr, to, verbose=False):
    '''  Usage:  storeClient.cp (token, fr, to)
    '''
    return client._cp (fr=fr, to=to, token=def_token(token), verbose=verbose)

@multifunc(2)
def cp  (fr, to, token=None, verbose=False):
    '''  Usage:  storeClient.cp (fr, to)
    '''
    return client._cp (fr=fr, to=to, token=def_token(token), verbose=verbose)


# --------------------------------------------------------------------
# LN -- Create a link to a file/directory in the store manager service
#
@multifunc(3)
def ln  (token, fr, target, verbose=False):
    '''  Usage:  storeClient.ln (token, fr, target)
    '''
    return client._ln (fr=fr, target=target, token=def_token(token), 
                       verbose=verbose)

@multifunc(2)
def ln  (fr, target, token=None, verbose=False):
    '''  Usage:  storeClient.ln (fr, target)
    '''
    return client._ln (fr=fr, target=target, token=def_token(token), 
                       verbose=verbose)


# --------------------------------------------------------------------
# LS -- Get a file/directory listing from the store manager service
#
@multifunc(2)
def ls  (token, name, format='csv'):
    '''  Usage:  storeClient.ls (token, name)
    '''
    return client._ls (name=name, format=format, token=def_token(token))

@multifunc(1)
def ls  (name, token=None, format='csv'):
    '''  Usage:  storeClient.ls (name)
    '''
    return client._ls (name=name, format=format, token=def_token(token))

@multifunc(0)
def ls  (name='vos://', token=None, format='csv'):
    '''  Usage:  storeClient.ls ()
    '''
    return client._ls (name=name, format=format, token=def_token(token))


# --------------------------------------------------------------------
# MKDIR -- Create a directory in the store manager service
#
@multifunc(2)
def mkdir  (token, name):
    '''  Usage:  storeClient.mkdir (token, name)
    '''
    return client._mkdir (name=name, token=def_token(token))

@multifunc(1)
def mkdir  (name, token=None):
    '''  Usage:  storeClient.mkdir (name)
    '''
    return client._mkdir (name=name, token=def_token(token))


# --------------------------------------------------------------------
# MV -- Move/rename a file/directory within the store manager service
#
@multifunc(3)
def mv  (token, fr, to, verbose=False):
    '''  Usage:  storeClient.mv (token, fr, to)
    '''
    return client._mv (fr=fr, to=to, token=def_token(token), verbose=verbose)

@multifunc(2)
def mv  (fr, to, token=None, verbose=False):
    '''  Usage:  storeClient.mv (fr, to)
    '''
    return client._mv (fr=fr, to=to, token=def_token(token), verbose=verbose)


# --------------------------------------------------------------------
# RM -- Delete a file from the store manager service
#
@multifunc(2)
def rm  (token, name, verbose=False):
    '''  Usage:  storeClient.rm (token, name)
    '''
    return client._rm (name=name, token=def_token(token), verbose=verbose)

@multifunc(1)
def rm  (name, token=None, verbose=False):
    '''  Usage:  storeClient.rm (name)
    '''
    return client._rm (name=name, token=def_token(token), verbose=verbose)


# --------------------------------------------------------------------
# RMDIR -- Delete a directory from the store manager service
#
@multifunc(2)
def rmdir  (token, name, verbose=False):
    '''  Usage:  storeClient.rmdir (token, name)
    '''
    return client._rmdir (name=name, token=def_token(token), verbose=verbose)

@multifunc(1)
def rmdir  (name, token=None, verbose=False):
    '''  Usage:  storeClient.rmdir (name)
    '''
    return client._rmdir (name=name, token=def_token(token), verbose=verbose)


# --------------------------------------------------------------------
# SAVEAS -- Save the string representation of a data object as a file.
#
@multifunc(3)
def saveAs  (token, data, name):
    '''  Usage:  storeClient.saveAs (token, data, name)
    '''
    return client._saveAs (data=data, name=name, token=def_token(token))

@multifunc(2)
def saveAs  (name, data, token=None):
    '''  Usage:  storeClient.saveAs (data, name)
    '''
    return client._saveAs (data=data, name=name, token=def_token(token))


# --------------------------------------------------------------------
# TAG -- Annotate a file/directory in the store manager service
#
@multifunc(3)
def tag  (token, name, tag):
    '''  Usage:  storeClient.tag (token, name, tag)
    '''
    return client._tag (name=name, tag=tag, token=def_token(token))

@multifunc(2)
def tag  (name, tag, token=None):
    '''  Usage:  storeClient.tag (name, tag)
    '''
    return client._tag (name=name, tag=tag, token=def_token(token))


# --------------------------------------------------------------------
# LOAD/PULL -- Load a file from a remote endpoint to the store manager service
#
@multifunc(3)
def load  (token, name, endpoint):
    '''  Usage:  storeClient.load (token, name, endpoint)
    '''
    return client._load (name=name, endpoint=endpoint, token=def_token(token))

@multifunc(2)
def load  (name, endpoint, token=None):
    '''  Usage:  storeClient.load (name, endpoint)
    '''
    return client._load (name=name, endpoint=endpoint, token=def_token(token))

# Aliases for load() calls.

@multifunc(3)
def pull  (token, name, endpoint):
    '''  Usage:  storeClient.pull (token, name, endpoint)
    '''
    return client._load (name=name, endpoint=endpoint, token=def_token(token))

@multifunc(2)
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
            
        
    # -----------------------------
    #  Utility Methods
    # -----------------------------

    @multimethod(1)
    def list_profiles  (self, token, profile=None, format='text'):
        '''  Usage:  storeClient.list_profiles (token, ....)
        '''
        return self._list_profiles (token=def_token(token), profile=profile,
                                      format=format)
    
    @multimethod(0)
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
        
        r = getFromURL(dburl, def_token(token))
        profiles = r.content
        if '{' in profiles:
            profiles = json.loads(profiles)

        return profiles
        

    # --------------------------------------------------------------------
    # GET -- Retrieve a file from the store manager service
    # --------------------------------------------------------------------
    @multimethod(3)
    def get (self, token, fr, to, verbose=True, debug=False):
        '''  Usage:  storeClient.get (token, fr, to)
        '''
        return self._get (fr=fr, to=to, token=def_token(token), 
                          verbose=verbose, debug=debug)

    @multimethod(2)
    def get (self, fr, to, token=None, verbose=True, debug=False):
        '''  Usage:  storeClient.get (fr, to)
        '''
        return self._get (fr=fr, to=to, token=def_token(token), 
                          verbose=verbose, debug=debug)

    @multimethod(1)
    def get (self, fr, to='', token=None, verbose=True, debug=False):
        '''  Usage:  storeClient.get (fr)
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
            A name or file template of the file(s) to retrieve. Names may
            
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
        nm = (fr if fr.startswith("vos://") else ("vos://" + fr))
    
        if debug:
            print ("get(): nm = %s" % nm)
        if hasmeta(fr):
            if not os.path.isdir(to):
                raise storeClientError (
                          "Location must be specified as a directory")
            if to == '' or to is None:
                raise storeClientError(
                          "Multi-file requests require a download location")
    
        if to != '' and to is not None:
            # Expand metacharacters to create a file list for download.
            flist = expandFileList (token, nm, "csv", full=True)
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
                    resp.append (r)
                fnum += 1
    
            return str(resp)
    
        else:
            # Get a single file, return the contents to the caller.
            url = requests.get(self.svc_url + "/get?name=%s" % nm,
                               headers=headers)
            r = requests.get(url.text, stream=False, headers=headers)
            return r.content
        
        
    # --------------------------------------------------------------------
    # PUT -- Upload a file to the store manager service
    # --------------------------------------------------------------------
    @multimethod(3)
    def put (self, token, fr, to, verbose=True, debug=False):
        '''  Usage:  storeClient.put (token, fr, to)
        '''
        return self._put (fr=fr, to=to, token=def_token(token), 
                          verbose=verbose, debug=False)

    @multimethod(2)
    def put (self, fr, to, token=None, verbose=True, debug=False):
        '''  Usage:  storeClient.put (fr, to)
        '''
        return self._put (fr=fr, to=to, token=def_token(token), 
                          verbose=verbose, debug=False)

    @multimethod(1)
    def put (self, fr, to='vos://', token=None, verbose=True, debug=False):
        '''  Usage:  storeClient.put (fr)
        '''
        return self._put (fr=fr, to=to, token=def_token(token), 
                          verbose=verbose, debug=False)

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
    
        # If the 'to' is a directory, create it first and then transfer the
        # contents.
        if os.path.isdir (fr):
            if fr.endswith("/"):
                dname = (to if to.startswith("vos://") else to[:-1])
                self._mkdir  (token=token, name=dname)
            flist = glob.glob(fr+"/*")
        else:
            dname = ''
            flist = [fr]
    
        if debug:
            print ("fr=%s  to=%s  dname=%s" % (fr, to, dname))
            print (flist)
    
        nfiles = len(flist)
        fnum = 1
        resp = []
        for f in flist:
            fr_dir, fr_name = os.path.split(f)
    
            # Patch the names with the URI prefix if needed.
            nm = (to if to.startswith("vos://") else ("vos://" + to))
            if to.endswith("/"):
                nm = nm + fr_name
    
            if debug:
                print ("put: f=%s  nm=%s" % (f,nm))
    
            if not os.path.exists(f):
                # Skip files that don't exist
                if verbose:
                    print ("Error: file '%s' does not exist" % f)
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
                    print ("%s" % nm)
    
            except Exception as e:
                resp.append (str(e))
            else:
                resp.append ("OK")
    
            fnum += 1
            
    
    # --------------------------------------------------------------------
    # LOAD -- Load a file from a remote endpoint to the store manager service
    # --------------------------------------------------------------------
    @multimethod(3)
    def load  (self, token, name, endpoint):
        '''  Usage:  storeClient.load (token, name, endpoint)
        '''
        return self._load (name=name, endpoint=endpoint, token=def_token(token))

    @multimethod(2)
    def load  (self, name, endpoint, token=None):
        '''  Usage:  storeClient.load (name, endpoint)
        '''
        return self._load (name=name, endpoint=endpoint, token=def_token(token))

    # Aliases for load() calls.

    @multimethod(3)
    def pull  (self, token, name, endpoint):
        '''  Usage:  storeClient.pull (token, name, endpoint)
        '''
        return self._load (name=name, endpoint=endpoint, token=def_token(token))

    @multimethod(2)
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
        r = getFromURL("/load?name=%s&endpoint=%s" % (name, endpoint), token)
        return r
    
        
    # --------------------------------------------------------------------
    # CP -- Copy a file/directory within the store manager service
    # --------------------------------------------------------------------
    @multimethod(3)
    def cp  (self, token, fr, to, verbose=False):
        '''  Usage:  storeClient.cp (token, fr, to)
        '''
        return self._cp (fr=fr, to=to, token=def_token(token), verbose=verbose)

    @multimethod(2)
    def cp  (self, fr, to, token=None, verbose=False):
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
        src = (fr if fr.startswith("vos://") else ("vos://" + fr))
        dest = (to if to.startswith("vos://") else ("vos://" + to))
    
        # If the 'from' string has no metacharacters we're copying a single file,
        # otherwise expand the file list and process the matches individually.
        if not hasmeta(fr):
            r = getFromURL("/cp?from=%s&to=%s" % (src, dest), token)
            return r
        else:
            flist = expandFileList (token, src, "csv", full=True)
            nfiles = len(flist)
            fnum = 1
            resp = []
            for f in flist:
                junk, fn = os.path.split (f)
                to_fname = dest + ('/%s' % fn)
                if verbose:
                    print ("(%d / %d) %s -> %s" % (fnum, nfiles, f, to_fname))
                r = getFromURL("/cp?from=%s&to=%s" % (f, to_fname), token)
                fnum += 1
                resp.append(r)
            return resp
    
    
    # --------------------------------------------------------------------
    # LN -- Create a link to a file/directory in the store manager service
    # --------------------------------------------------------------------
    @multimethod(3)
    def ln  (self, token, fr, target, verbose=False):
        '''  Usage:  storeClient.ln (token, fr, target)
        '''
        return self._ln (fr=fr, target=target, token=def_token(token),
                           verbose=verbose)

    @multimethod(2)
    def ln  (self, fr, target, token=None, verbose=False):
        '''  Usage:  storeClient.ln (fr, target)
        '''
        return self._ln (fr=fr, target=target, token=def_token(token),
                           verbose=verbose)

    def _ln (self, token=None, fr='', target=''):
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
            r = getFromURL("/ln?from=%s&to=%s" % (fr, target), token)
        except Exception:
            raise storeClientError(r.content)
        else:
            return "OK"
    
        
    # --------------------------------------------------------------------
    # LS -- Get a file/directory listing from the store manager service
    # --------------------------------------------------------------------
    @multimethod(2)
    def ls  (self, token, name, format='csv'):
        '''  Usage:  storeClient.ls (token, name)
        '''
        return self._ls (name=name, format=format, token=def_token(token))

    @multimethod(1)
    def ls  (self, name, token=None, format='csv'):
        '''  Usage:  storeClient.ls (name)
        '''
        return self._ls (name=name, format=format, token=def_token(token))

    @multimethod(0)
    def ls  (self, name='vos://', token=None, format='csv'):
        '''  Usage:  storeClient.ls ()
        '''
        return self._ls (name=name, format=format, token=def_token(token))

    def _ls (self, token=None, name='', format='csv'):
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
        flist = expandFileList (token, name, format, full=False)
        if (format == 'csv'):
            result = ",".join(flist)
            return (result[1:] if result.startswith(",") else result)
    
        else:
            results = []
            for f in flist:
                url = self.svc_url + "/ls?name=vos://%s&format=%s" % (f, format)
                r = requests.get(url, headers={'X-DL-AuthToken': token})
                results.append(r.content)
    
            return "\n".join(results)
    
        
    # --------------------------------------------------------------------
    # MKDIR -- Create a directory in the store manager service
    # --------------------------------------------------------------------
    @multimethod(2)
    def mkdir (self, token, name):
        '''  Usage:  storeClient.mkdir (token, name)
        '''
        return self._mkdir (name=name, token=def_token(token))

    @multimethod(1)
    def mkdir (self, name, token=None):
        '''  Usage:  storeClient.mkdir (name)
        '''
        return self._mkdir (name=name, token=def_token(token))

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
        nm = (name if name.startswith("vos://") else ("vos://" + name))
        
        try:
            r = getFromURL("/mkdir?dir=%s" % nm, token)
        except Exception:
            raise storeClientError(r.content)
        else:
            return "OK"
            
        
    # --------------------------------------------------------------------
    # MV -- Move/rename a file/directory within the store manager service
    # --------------------------------------------------------------------
    @multimethod(3)
    def mv  (self, token, fr, to, verbose=False):
        '''  Usage:  storeClient.mv (token, fr, to)
        '''
        return self._mv (fr=fr, to=to, token=def_token(token), verbose=verbose)

    @multimethod(2)
    def mv  (self, fr, to, token=None, verbose=False):
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
        src = (fr if fr.startswith("vos://") else ("vos://" + fr))
        dest = (to if to.startswith("vos://") else ("vos://" + to))
    
        # If the 'from' string has no metacharacters we're copying a single file,
        # otherwise expand the file list on the and process the matches
        # individually.
        if not hasmeta(fr):
            r = getFromURL("/mv?from=%s&to=%s" % (src, dest), token)
            return r
        else:
            flist = expandFileList (token, src, "csv", full=True)
            nfiles = len(flist)
            fnum = 1
            resp = []
            for f in flist:
                junk, fn = os.path.split (f)
                to_fname = dest + ('/%s' % fn)
                if verbose:
                    print ("(%d / %d) %s -> %s" % (fnum, nfiles, f, to_fname))
                r = getFromURL("/mv?from=%s&to=%s" % (f, to_fname), token)
                fnum += 1
                resp.append(r)
            return resp    
    
        
    # --------------------------------------------------------------------
    # RM -- Delete a file from the store manager service
    # --------------------------------------------------------------------
    @multimethod(2)
    def rm  (self, token, name, verbose=False):
        '''  Usage:  storeClient.rm (token, name)
        '''
        return self._rm (name=name, token=def_token(token), verbose=verbose)

    @multimethod(1)
    def rm  (self, name, token=None, verbose=False):
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
        nm = (name if name.startswith("vos://") else ("vos://" + name))
        if nm == "vos://" or nm == "vos://tmp" or nm == "vos://public":
            return "Error: operation not permitted"
    
        # If the 'name' string has no metacharacters we're copying a single file,
        # otherwise expand the file list on the and process the matches
        # individually.
        if not hasmeta(nm):
            r = getFromURL("/rm?file=%s" % nm, token)
            return r
        else:
            flist = expandFileList (token, nm, "csv", full=True)
            nfiles = len(flist)
            fnum = 1
            resp = []
            for f in flist:
                if verbose:
                    print ("(%d / %d) %s" % (fnum, nfiles, f))
                r = getFromURL("/rm?file=%s" % f, token)
                fnum += 1
                resp.append(r)
            return resp
    
        
    # --------------------------------------------------------------------
    # RMDIR -- Delete a directory from the store manager service
    # --------------------------------------------------------------------
    @multimethod(2)
    def rmdir (self, token, name, verbose=False):
        '''  Usage:  storeClient.rmdir (token, name)
        '''
        return self._rmdir (name=name, token=def_token(token), verbose=verbose)

    @multimethod(1)
    def rmdir (self, name, token=None, verbose=False):
        '''  Usage:  storeClient.rmdir (name)
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
    
        # Patch the names with the URI prefix if needed.
        nm = (name if name.startswith("vos://") else ("vos://" + name))
        if nm == "vos://" or nm == "vos://tmp" or nm == "vos://public":
            return "Error: operation not permitted"
    
        try:        
            saveAs  (token, "deleted", nm+"/.deleted")
            r = getFromURL("/rmdir?dir=%s" % nm, token)
        except Exception:
            raise storeClientError(r.content)
        else:
            return "OK"
    
    
    # --------------------------------------------------------------------
    # SAVEAS -- Save the string representation of a data object as a file.
    # --------------------------------------------------------------------
    @multimethod(3)
    def saveAs (self, token, data, name):
        '''  Usage:  storeClient.saveAs (token, data, name)
        '''
        return self._saveAs (data=data, name=name, token=def_token(token))

    @multimethod(2)
    def saveAs (self, name, data, token=None):
        '''  Usage:  storeClient.saveAs (data, name)
        '''
        return self._saveAs (data=data, name=name, token=def_token(token))

    def _saveAs (token=None, data='', name=''):
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
            raise storeClientError(str(e))
    
        # Patch the names with the URI prefix if needed.
        nm = (name if name.startswith("vos://") else ("vos://" + name))
    
        # Put the temp file to the VOSpace.
        put (token, fr=tfd.name, to=nm, verbose=False)
    
        os.unlink(tfd.name)                # Clean up
    
        return "OK"
    
            
    # --------------------------------------------------------------------
    # TAG -- Annotate a file/directory in the store manager service
    # --------------------------------------------------------------------
    @multimethod(3)
    def tag (self, token, name, tag):
        '''  Usage:  storeClient.tag (token, name, tag)
        '''
        return self._tag (name=name, tag=tag, token=def_token(token))

    @multimethod(2)
    def tag (self, name, tag, token=None):
        '''  Usage:  storeClient.tag (name, tag)
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
            r = getFromURL("/tag?file=%s&tag=%s" % (name, tag), token)
        except Exception:
            raise storeClientError (r.content)
        else:
            return "OK"





# -------------------------------------------------------
#  Utility Methods
# -------------------------------------------------------

def hasmeta(s):
    """ Determine whether a string contains filename meta-characters.
    """
    return (s.find('*') >= 0) or (s.find('[') >= 0) or (s.find('?') > 0)


def expandFileList (token, pattern, format, full=False):
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
    uri = 'vos://'
    str = (pattern[6:] if pattern.startswith('vos://') else pattern)

    # Extract the directory and filename/pattern from the string.
    dir, name = os.path.split(str)
    if debug:
        print ("-----------------------------------------")
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
    url = self.svc_url + "/ls?name=vos://%s&format=%s" % (dir, "csv")
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
def getFromURL(path, token):
    try:
        resp = requests.get("%s%s" % (self.svc_url, path), 
                            headers = {"X-DL-AuthToken": def_token (token)})
    except Exception as e:
        raise storeClientError(str(e))
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

