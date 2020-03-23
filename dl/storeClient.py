#!/usr/bin/env python
#
# STORECLIENT -- Client routines for the Data Lab Store Manager service

from __future__ import print_function

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>, Matthew Graham <graham@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = 'v2.18.4'


'''
    Client routines for the DataLab Storage Manager service.

Storage Manager Client Interface
--------------------------------
                isAlive  (svc_url=DEF_SERVICE_URL)
            set_svc_url  (svc_url=DEF_SERVICE_URL)
            get_svc_url  ()
            set_profile  (profile=DEF_PROFILE)
            get_profile  ()
               services  (name=None, svc_type='vos', format=None,
                          profile='default')

          list_profiles  (optval, profile=None, format='text', token=None)
          list_profiles  (token=None, profile=None, format='text')
                 access  (token, path, mode, verbose=True)
                 access  (path, mode, token=None, verbose=True)
                 access  (path, mode=None, token=None, verbose=True)
                   stat  (token, path, verbose=True)
                   stat  (path, token=None, verbose=True)
                    get  (token, fr, to, mode='text', verbose=True, debug=False)
                    get  (opt1, opt2, fr='', to='', token=None, mode='text',
                          verbose=True, debug=False)
                    get  (optval, fr='', to='', token=None, mode='text',
                          verbose=True, debug=False)
                    get  (token=None, fr='', to='', mode='text', verbose=True,
                          debug=False)
                    put  (token, fr, to, verbose=True, debug=False)
                    put  (fr, to, token=None, verbose=True, debug=False)
                    put  (optval, fr='', to='vos//', token=None, verbose=True,
                          debug=False)
                    put  (fr='', to='vos//', token=None, verbose=True,
                          debug=False)
                     cp  (token, fr, to, verbose=False)
                     cp  (fr, to, token=None, verbose=False)
                     cp  (token, fr='', to='', verbose=False)
                     cp  (token=None, fr='', to='', verbose=False)
                     ln  (token, fr, target, verbose=False)
                     ln  (fr, target, token=None, verbose=False)
                     ln  (token, fr='', target='', verbose=False)
                     ls  (token, name, format='csv', verbose=False)
                     ls  (optval, name='vos//', token=None, format='csv',
                          verbose=False)
                     ls  (name='vos//', token=None, format='csv', verbose=False)
                  mkdir  (token, name)
                  mkdir  (optval, name='', token=None)
                     mv  (token, fr, to, verbose=False)
                     mv  (fr, to, token=None, verbose=False)
                     mv  (token, fr='', to='', verbose=False)
                     mv  (token=None, fr='', to='', verbose=False)
                     rm  (token, name, verbose=False)
                     rm  (optval, name='', token=None, verbose=False)
                     rm  (name='', token=None, verbose=False)
                  rmdir  (token, name, verbose=False)
                  rmdir  (optval, name='', token=None, verbose=False)
                  rmdir  (name='', token=None, verbose=False)
                 saveAs  (token, data, name)
                 saveAs  (data, name, token=None)
                    tag  (token, name, tag)
                    tag  (name, tag, token=None)
                    tag  (token, name='', tag='')
                   load  (token, name, endpoint, is_vospace=False)
                   load  (name, endpoint, token=None, is_vospace=False)
                   pull  (token, name, endpoint, is_vospace=False)
                   pull  (name, endpoint, token=None, is_vospace=False)


Import via

.. code-block:: python

    from dl import storeClient

'''

import os
import sys
import fnmatch
import requests
import glob
import socket
import json
import time

if os.path.isfile('./Util.py'):                # use local dev copy
    from Util import multimethod
    from Util import def_token
else:                                           # use distribution copy
    from dl.Util import multimethod
    from dl.Util import def_token

# Turn off some annoying astropy warnings
#import warnings
#from astropy.utils.exceptions import AstropyWarning
#warnings.simplefilter('ignore', AstropyWarning)

is_py3 = sys.version_info.major == 3


#####################################
#  Storage Manager Configuration
#####################################

# The URL of the Storage Manager service to contact.  This may be changed by
# passing a new URL into the set_svc_url() method before beginning.

DEF_SERVICE_ROOT = 'https://datalab.noao.edu'

# Allow the service URL for dev/test systems to override the default.
THIS_HOST = socket.gethostname()                        # host name
sock = socket.socket(type=socket.SOCK_DGRAM)  	# host IP address
sock.connect(('8.8.8.8', 1))  	# Example IP address, see RFC 5737
THIS_IP, _ = sock.getsockname()

if THIS_HOST[:5] == 'dldev':
    DEF_SERVICE_ROOT = 'http://dldev.datalab.noao.edu'
elif THIS_HOST[:6] == 'dltest':
    DEF_SERVICE_ROOT = 'http://dltest.datalab.noao.edu'

DEF_SERVICE_URL = DEF_SERVICE_ROOT + '/storage'
QM_SERVICE_URL = DEF_SERVICE_ROOT + '/query'

# The requested query 'profile'.  A profile refers to the specific
# machines and services used by the Storage Manager on the server.
DEF_PROFILE     = 'default'

# Use a /tmp/SM_DEBUG file as a way to turn on debugging in the client code.
DEBUG           = os.path.isfile('/tmp/SM_DEBUG')



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
def isAlive(svc_url=DEF_SERVICE_URL):
    return sc_client.isAlive(svc_url=svc_url)

# --------------------------------------------------------------------
# SET_SVC_URL -- Set the service url to use.
#
def set_svc_url(svc_url=DEF_SERVICE_URL):
    return sc_client.set_svc_url(svc_url=svc_url)

# --------------------------------------------------------------------
# GET_SVC_URL -- Get the service url being used.
#
def get_svc_url():
    return sc_client.get_svc_url()

# --------------------------------------------------------------------
# SET_PROFILE -- Set the profile to be used
#
def set_profile(profile=DEF_PROFILE):
    return sc_client.set_profile(profile=profile)

# --------------------------------------------------------------------
# GET_PROFILE -- Get the profile currently being used.
#
def get_profile():
    return sc_client.get_profile()

# --------------------------------------------------------------------
# SERVICES -- List public storage services
#
def services(name=None, svc_type='vos', format=None, profile='default'):
    return sc_client.services(name=name, svc_type=svc_type, format=format,
                               profile=profile)

# --------------------------------------------------------------------
# LIST_PROFILES -- List the profiles supported by the storage manager service
#
@multimethod('sc',1,False)
def list_profiles(optval, profile=None, format='text', token=None):
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return sc_client._list_profiles(token=def_token(optval),
                                         profile=profile, format=format)
    else:
        # optval looks like a profile name
        return sc_client._list_profiles(token=def_token(token), profile=optval,
                                      format=format)

@multimethod('sc',0,False)
def list_profiles(token=None, profile=None, format='text'):
    '''Retrieve the profiles supported by the storage manager service

    Usage:
        list_profiles(token=None, profile=None, format='text')

    MultiMethod Usage:
    ------------------
        storeClient.list_profiles(token)	# list default profile
        storeClient.list_profiles(profile)	# list named profile
        storeClient.list_profiles()		# list default profile

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
        profiles = storeClient.list_profiles(token)
    '''
    return sc_client._list_profiles(token=def_token(token), profile=profile,
                                  format=format)


# -----------------------------
#  Utility Functions
# -----------------------------

# --------------------------------------------------------------------
# ACCESS -- Determine whether the file can be accessed with the given node.
#           Modes are 'r' (read access), 'w' (write access), or '' or None
#           for an existence test.
#
@multimethod('sc',3,False)
def access(token, path, mode, verbose=True):
    return sc_client._access(path=path, mode=mode, token=def_token(token),
                          verbose=verbose)

@multimethod('sc',2,False)
def access(path, mode, token=None, verbose=True):
    return sc_client._access(path=path, mode=mode, token=def_token(token),
                          verbose=verbose)

@multimethod('sc',1,False)
def access(path, mode=None, token=None, verbose=True):
    '''Determine whether the file can be accessed with the given node.

    Usage:
        access(path, mode=None, token=None, verbose=True)

    MultiMethod Usage:
    ------------------
        storeClient.access(token, path, mode)
        storeClient.access(path, mode)
        storeClient.access(path)

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

        if storeClient.access('/mydata.csv')
            print('File exists')
        elif storeClient.access('/mydata.csv','rw')
            print('File is both readable and writable')
    '''
    return sc_client._access(path=path, mode=mode, token=def_token(token),
                          verbose=verbose)


# --------------------------------------------------------------------
# STAT -- Get file status. Values are returned as a dictionary of the
#         requested node.
#
@multimethod('sc',2,False)
def stat(token, path, verbose=True):
    return sc_client._stat(path=path, token=def_token(token), verbose=verbose)

@multimethod('sc',1,False)
def stat(path, token=None, verbose=True):
    '''Get file status information, similar to stat().

    Usage:
        stat(path, token=None, verbose=True)

    MultiMethod Usage:
    ------------------
        storeClient.stat(token, path)
        storeClient.stat(path)

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
            groupread	        List of group/owner names w/ read access
            groupwrite	        List of group/owner names w/ write access
            publicread	        Publicly readable (0=False, 1=True)
            owner		Owner name
            perms		Formatted unix-like permission string
            target		Node target if LinkNode
            size		Size of file node (bytes)
            type		Node type (container|data|link)

    Example
    -------
    .. code-block:: python

        # get status information for a specific node
        stat = storeClient.stat('vos://mydata.csv')

        if stat['type'] == 'container':
            print('This is a directory')
        else:
            print('File size is: ' + stat['size'])
    '''
    return sc_client._stat(path=path, token=def_token(token), verbose=verbose)



# --------------------------------------------------------------------
# GET -- Retrieve a file (or files) from the Store Manager service
#
@multimethod('sc',3,False)
def get(token, fr, to, mode='text', verbose=True, debug=False, timeout=30):
    return sc_client._get(fr=fr, to=to, token=def_token(token),
                        mode=mode, verbose=verbose, debug=debug,
                        timeout=timeout)

@multimethod('sc',2,False)
def get(opt1, opt2, fr='', to='', token=None, mode='text', verbose=True, 
        debug=False, timeout=30):
    if opt1 is not None and len(opt1.split('.')) >= 4:
        # opt1 looks like a token
        return sc_client._get(fr=opt2, to=to, token=def_token(opt1),
                            mode=mode, verbose=verbose, debug=debug,
                            timeout=timeout)
    else:
        # opt1 is the 'fr' value, opt2 is the 'to' value
        return sc_client._get(fr=opt1, to=opt2, token=def_token(token),
                            mode=mode, verbose=verbose, debug=debug,
                            timeout=timeout)

@multimethod('sc',1,False)
def get(optval, fr='', to='', token=None, mode='text', verbose=True, 
        debug=False, timeout=30):
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return sc_client._get(fr=fr, to=to, token=def_token(optval),
                            mode=mode, verbose=verbose, debug=debug,
                            timeout=timeout)
    else:
        # optval is the 'fr' value
        return sc_client._get(fr=optval, to=to, token=def_token(token),
                            mode=mode, verbose=verbose, debug=debug,
                            timeout=timeout)

@multimethod('sc',0,False)
def get(token=None, fr='', to='', mode='text', verbose=True, debug=False,
        timeout=30):
    '''Retrieve a file from the store manager service

    Usage:
        get(token=None, fr='', to='', mode='text', verbose=True, debug=False,
            timeout=30)

    MultiMethod Usage:
    ------------------
        storeClient.get(token, fr, to)
        storeClient.get(fr, to)
        storeClient.get(fr)
        storeClient.get(token, fr, to)

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    fr : str
        A name or file template of the file(s) to retrieve.

    to : str
        Name of the file(s) to locally.  If not specified, the contents
        of the file are returned to the caller.

    mode : [binary | text | fileobj]
        Return data type if note saving to file.  If set to 'text' the
        file contents are converted to string -- this is appropriate when
        dealing with unicode but may fail with general binary data.  If
        set to 'binary' the raw content of the HTTP response is returned --
        for Python 2 this will be a 'string', for Python 3 it will be a
        'bytes' data type (the caller is responsible for conversion).

    verbose : bool
        Print verbose output, e.g. progress indicators.

    debug : bool
        Print debug output.

    timeout : integer
        Retry timeout value.  When processing long lists, download will
        pause every `timeout` files to lessen server load.  For individual
        files, transfer will retry for `timeout` seconds before aborting.
        Failed transfers are automatically appended to the file list so
        they may be transferred again later.

    Returns
    -------
    result : str
        A list of the names of the files retrieved, or the contents of
        a single file.

    Example
    -------
    .. code-block:: python

        # get a single file to a local file of a different name
        data = storeClient.get('vos://mydata.csv', 'data.csv')

        # get the contents of a single file to a local variable
        data = storeClient.get('vos://mydata.csv')

        # get a list of remote files to a local directory
        flist = storeClient.get('vos://*.fits', './data/')
        flist = storeClient.get('*.fits', './data/')
    '''
    return sc_client._get(fr=fr, to=to, token=def_token(token),
                        mode=mode, verbose=verbose, debug=debug,
                        timeout=timeout)


# --------------------------------------------------------------------
# PUT -- Upload a file (or files) to the Store Manager service
#
@multimethod('sc',3,False)
def put(token, fr, to, verbose=True, debug=False):
    return sc_client._put(fr=fr, to=to, token=def_token(token),
                        verbose=verbose, debug=debug)

@multimethod('sc',2,False)
def put(fr, to, token=None, verbose=True, debug=False):
    return sc_client._put(fr=fr, to=to, token=def_token(token),
                        verbose=verbose, debug=debug)

@multimethod('sc',1,False)
def put(optval, fr='', to='vos://', token=None, verbose=True, debug=False):
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return sc_client._put(fr=fr, to=to, token=def_token(optval),
                            verbose=verbose, debug=debug)
    else:
        # optval looks like source name
        return sc_client._put(fr=optval, to=to, token=def_token(token),
                            verbose=verbose, debug=debug)

@multimethod('sc',0,False)
def put(fr='', to='vos://', token=None, verbose=True, debug=False):
    '''Upload a file to the store manager service

    Usage:
        put(fr='', to='vos://', token=None, verbose=True, debug=False)

    MultiMethod Usage:
    ------------------
        storeClient.put(token, fr, to)
        storeClient.put(fr, to)
        storeClient.put(fr)
        storeClient.put(fr='',to='')

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
    '''
    return sc_client._put(fr=fr, to=to, token=def_token(token),
                        verbose=verbose, debug=debug)


# --------------------------------------------------------------------
# CP -- Copy a file/directory within the store manager service
#
@multimethod('sc',3,False)
def cp(token, fr, to, verbose=False):
    return sc_client._cp(fr=fr, to=to, token=def_token(token), verbose=verbose)

@multimethod('sc',2,False)
def cp(fr, to, token=None, verbose=False):
    return sc_client._cp(fr=fr, to=to, token=def_token(token), verbose=verbose)

@multimethod('sc',1,False)
def cp(token, fr='', to='', verbose=False):
    return sc_client._cp(fr=fr, to=to, token=def_token(token), verbose=verbose)

@multimethod('sc',0,False)
def cp(token=None, fr='', to='', verbose=False):
    '''Copy a file/directory within the store manager service

    Usage:
        cp(token=None, fr='', to='', verbose=False)

    MultiMethod Usage:
    ------------------
        storeClient.cp(token, fr, to)
        storeClient.cp(fr, to)
        storeClient.cp(fr)
        storeClient.cp(fr='',to='')

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
        storeClient.cp('foo', 'bar')
        storeClient.cp('vos://foo', 'vos:///new/bar')
    '''
    return sc_client._cp(fr=fr, to=to, token=def_token(token), verbose=verbose)


# --------------------------------------------------------------------
# LN -- Create a link to a file/directory in the store manager service
#
@multimethod('sc',3,False)
def ln(token, fr, target, verbose=False):
    return sc_client._ln(fr=fr, target=target, token=def_token(token),
                       verbose=verbose)

@multimethod('sc',2,False)
def ln(fr, target, token=None, verbose=False):
    return sc_client._ln(fr=fr, target=target, token=def_token(token),
                       verbose=verbose)

@multimethod('sc',1,False)
def ln(token, fr='', target='', verbose=False):
    '''Create a link to a file/directory in the store manager service

    Usage:
        ln(token, fr='', target='', verbose=False)

    MultiMethod Usage:
    ------------------
        storeClient.ln(token, fr, target)
        storeClient.ln(fr, target)
        storeClient.ln(fr, target)

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
        storeClient.ln('foo', 'bar')
        storeClient.ln('vos://foo', 'vos:///new/bar')
    '''
    return sc_client._ln(fr=fr, target=target, token=def_token(token),
                       verbose=verbose)


# --------------------------------------------------------------------
# LS -- Get a file/directory listing from the store manager service
#
@multimethod('sc',2,False)
def ls(token, name, format='csv', verbose=False):
    return sc_client._ls(name=name, format=format, token=def_token(token),
                       verbose=verbose)

@multimethod('sc',1,False)
def ls(optval, name='vos://', token=None, format='csv', verbose=False):
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return sc_client._ls(name=name, format=format,
                          token=def_token(optval), verbose=verbose)
    else:
        return sc_client._ls(name=optval, format=format,
                          token=def_token(None), verbose=verbose)

@multimethod('sc',0,False)
def ls(name='vos://', token=None, format='csv', verbose=False):
    '''Get a file/directory listing from the store manager service

    Usage:
        ls(name='vos://', token=None, format='csv', verbose=False)

    MultiMethod Usage:
    ------------------
        storeClient.ls(token, name)
        storeClient.ls(name)
        storeClient.ls()

    Parameters
    ----------
    token : str
        Secure token obtained via :func:`authClient.login`

    name : str
        Valid name of file or directory, e.g. ``vos://somedir``
        .. todo:: [20161110] currently doesn't seem to work.

    format : str
        Default ``csv``.  The ``long`` option produces an output similar to 'ls -l'.

    Example
    -------
    .. code-block:: python

        listing = storeClient.ls(token, name='vos://somedir')
        listing = storeClient.ls(token, 'vos://somedir')
        listing = storeClient.ls('vos://somedir')
        print(listing)

    This prints for instance:

    .. code::

        bar2.fits,foo1.csv,fancyfile.dat
    '''
    return sc_client._ls(name=name, format=format, token=def_token(token),
                       verbose=verbose)


# --------------------------------------------------------------------
# MKDIR -- Create a directory in the store manager service
#
@multimethod('sc',2,False)
def mkdir(token, name):
    return sc_client._mkdir(name=name, token=def_token(token))

@multimethod('sc',1,False)
def mkdir(optval, name='', token=None):
    '''Make a directory in the storage manager service

    Usage:
        mkdir(optval, name='', token=None)

    MultiMethod Usage:
    ------------------
        storeClient.mkdir(token, name)
        storeClient.mkdir(name)

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
        storeClient.mkdir('foo')
    '''
    if optval is not None and len(optval.split('.')) >= 4:
        return sc_client._mkdir(name=name, token=def_token(optval))
    else:
        return sc_client._mkdir(name=optval, token=def_token(token))


# --------------------------------------------------------------------
# MV -- Move/rename a file/directory within the store manager service
#
@multimethod('sc',3,False)
def mv(token, fr, to, verbose=False):
    return sc_client._mv(fr=fr, to=to, token=def_token(token), verbose=verbose)

@multimethod('sc',2,False)
def mv(fr, to, token=None, verbose=False):
    return sc_client._mv(fr=fr, to=to, token=def_token(token), verbose=verbose)

@multimethod('sc',1,False)
def mv(token, fr='', to='', verbose=False):
    return sc_client._mv(fr=fr, to=to, token=def_token(token), verbose=verbose)

@multimethod('sc',0,False)
def mv(token=None, fr='', to='', verbose=False):
    '''Move/rename a file/directory within the store manager service

    Usage:
        mv(token=None, fr='', to='', verbose=False)

    MultiMethod Usage:
    ------------------
        storeClient.mv(token, fr, to)
        storeClient.mv(fr, to)
        storeClient.mv(fr)
        storeClient.mv(fr='',to='')

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
        storeClient.mv('foo', 'bar')             # rename file
        storeClient.mv('foo', 'vos://newdir/')   # move to new directory
        storeClient.mv('foo', 'newdir')
    '''
    return sc_client._mv(fr=fr, to=to, token=def_token(token), verbose=verbose)


# --------------------------------------------------------------------
# RM -- Delete a file from the store manager service
#
@multimethod('sc',2,False)
def rm(token, name, verbose=False):
    return sc_client._rm(name=name, token=def_token(token), verbose=verbose)

@multimethod('sc',1,False)
def rm(optval, name='', token=None, verbose=False):
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return sc_client._rm(name=name, token=def_token(optval),
                             verbose=verbose)
    else:
        # optval is the name to be removed
        return sc_client._rm(name=optval, token=def_token(token),
                             verbose=verbose)

@multimethod('sc',0,False)
def rm(name='', token=None, verbose=False):
    '''Delete a file from the store manager service

    Usage:
        rm(name='', token=None, verbose=False)

    MultiMethod Usage:
    ------------------
        storeClient.rm(token, name)
        storeClient.rm(name)

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
        storeClient.rm('foo.csv')
        storeClient.rm('vos://foo.csv')
    '''
    return sc_client._rm(name=name, token=def_token(token), verbose=verbose)


# --------------------------------------------------------------------
# RMDIR -- Delete a directory from the store manager service
#
@multimethod('sc',2,False)
def rmdir(token, name, verbose=False):
    return sc_client._rmdir(name=name, token=def_token(token), verbose=verbose)

@multimethod('sc',1,False)
def rmdir(optval, name='', token=None, verbose=False):
    if optval is not None and len(optval.split('.')) >= 4:
        return sc_client._rmdir(name=name, token=def_token(optval),
                            verbose=verbose)
    else:
        return sc_client._rmdir(name=optval, token=def_token(token),
                            verbose=verbose)

@multimethod('sc',0,False)
def rmdir(name='', token=None, verbose=False):
    '''Delete a directory from the store manager service

    Usage:
        rmdir(name='', token=None, verbose=False)

    MultiMethod Usage:
    ------------------
        storeClient.rmdir(token, name)
        storeClient.rmdir(name)

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
        storeClient.rmdir('datadir')
        storeClient.rmdir('vos://datadir')
    '''
    return sc_client._rmdir(name=name, token=def_token(token), verbose=verbose)


# --------------------------------------------------------------------
# SAVEAS -- Save the string representation of a data object as a file.
#
@multimethod('sc',3,False)
def saveAs(token, data, name):
    return sc_client._saveAs(data=data, name=name, token=def_token(token))

@multimethod('sc',2,False)
def saveAs(data, name, token=None):
    '''Save the string representation of a data object as a file.

    Usage:
        saveAs(data, name, token=None)

    MultiMethod Usage:
    ------------------
        storeClient.saveAs(token, data, name)
        storeClient.saveAs(data, name)

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
        storeClient.saveAs(pandas_data, 'pandas.example')
        storeClient.saveAs(json_data, 'json.example')
        storeClient.saveAs(table_data, 'table.example')
    '''
    return sc_client._saveAs(data=data, name=name, token=def_token(token))


# --------------------------------------------------------------------
# TAG -- Annotate a file/directory in the store manager service
#
@multimethod('sc',3,False)
def tag(token, name, tag):
    return sc_client._tag(name=name, tag=tag, token=def_token(token))

@multimethod('sc',2,False)
def tag(name, tag, token=None):
    return sc_client._tag(name=name, tag=tag, token=def_token(token))

@multimethod('sc',1,False)
def tag(token, name='', tag=''):
    '''Annotate a file/directory in the store manager service

    Usage:
        tag(token, name='', tag='')

    MultiMethod Usage:
    ------------------
        storeClient.tag(token, name, tag)
        storeClient.tag(name, tag)
        storeClient.tag(token, name='foo', tag='bar')

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
        storeClient.tag('foo.csv', 'This is a test')
    '''
    return sc_client._tag(name=name, tag=tag, token=def_token(token))


# --------------------------------------------------------------------
# LOAD/PULL -- Load a file from a remote endpoint to the store manager service
#
@multimethod('sc',3,False)
def load(token, name, endpoint, is_vospace=False):
    return sc_client._load(name=name, endpoint=endpoint,
                            token=def_token(token), is_vospace=is_vospace)

@multimethod('sc',2,False)
def load(name, endpoint, token=None, is_vospace=False):
    '''Load a file from a remote endpoint to the Store Manager service

    Usage:
        load(name, endpoint, token=None, is_vospace=False)

    MultiMethod Usage:
    ------------------
        storeClient.load(token, name, endpoint)
        storeClient.load(name, endpoint)

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
        storeClient.load('mydata.vot', 'http://example.com/data.vot')
    '''
    return sc_client._load(name=name, endpoint=endpoint,
                            token=def_token(token), is_vospace=is_vospace)

# Aliases for load() calls.

@multimethod('sc',3,False)
def pull(token, name, endpoint, is_vospace=False):
    return sc_client._load(name=name, endpoint=endpoint,
                            token=def_token(token), is_vospace=is_vospace)

@multimethod('sc',2,False)
def pull(name, endpoint, token=None, is_vospace=False):
    '''Load a file from a remote endpoint to the Store Manager service

    Usage:
        pull(name, endpoint, token=None, is_vospace=False)

    MultiMethod Usage:
    ------------------
        storeClient.pull(token, name, endpoint)
        storeClient.pull(name, endpoint)

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
        storeClient.load('mydata.vot', 'http://example.com/data.vot')
    '''
    return sc_client._load(name=name, endpoint=endpoint,
                            token=def_token(token), is_vospace=is_vospace)



# ####################################################################
#  Module Functions
# ####################################################################

class storeClient(object):
    '''
         STORECLIENT -- Client-side methods to access the Data Lab
                        Storage Manager Service.
    '''
    def __init__(self, profile=DEF_PROFILE, svc_url=DEF_SERVICE_URL):
        '''Initialize the store client object.
        '''
        self.svc_url = svc_url.strip('/')       # StoreMgr service URL
        self.qm_svc_url = QM_SERVICE_URL        # QueryMgr service URL
        self.svc_profile = profile              # StoreMgr service profile

        self.hostip = THIS_IP
        self.hostname = THIS_HOST
        self.async_wait = False

        # Get the $HOME/.datalab directory.
        self.home = '%s/.datalab' % os.path.expanduser('~')

        self.debug = DEBUG                      # interface debug flag


    # --------------------------------------------------------------------
    # ISALIVE -- Ping the Storage Manager service to see if it responds.
    #
    def isAlive(self, svc_url=None, timeout=2):
        '''Check whether the StorageManager service at the given URL is
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
        '''
        if svc_url is None:
            svc_url = self.svc_url

        try:
            r = requests.get(svc_url.strip('/'), timeout=timeout)
            resp = scToString(r.content)
            if r.status_code != requests.codes.ok:
                return False
            elif resp is not None and r.text.lower()[:11] != "hello world":
                return False
        except Exception:
            return False

        return True

    def set_svc_url(self, svc_url):
        '''Set the Storage Manager service URL.

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

            storeClient.set_scv_url("http://demo.datalab.noao.edu:7003")
        '''
        self.svc_url = scToString(svc_url.strip('/'))

    def get_svc_url(self):
        '''Return the currently-used Storage Manager service URL.

        Parameters
        ----------
            None

        Returns
        -------
            Current Storage Manager service URL

        Example
        -------
        .. code-block:: python

            print(storeClient.get_scv_url())
        '''
        return scToString(self.svc_url)

    def set_profile(self, profile):
        '''Set the service profile to be used.

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
        '''

        self.svc_profile = scToString(profile)

    def get_profile(self):
        '''Get the profile

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

            print('Store Service profile = ' + storeClient.get_profile())
        '''
        return scToString(self.svc_profile)


    @multimethod('_sc',1,True)
    def list_profiles(self, token, profile=None, format='text'):
        ''' Usage:  storeClient.list_profiles(token, ....)
        '''
        return self._list_profiles(token=def_token(token), profile=profile,
                                      format=format)

    @multimethod('_sc',0,True)
    def list_profiles(self, token=None, profile=None, format='text'):
        ''' Usage:  storeClient.list_profiles(....)
        '''
        return self._list_profiles(token=def_token(token), profile=profile,
                                      format=format)

    def _list_profiles(self, token=None, profile=None, format='text'):
        '''Implementation of the list_profiles() method.
        '''
        dburl = '/profiles?'
        if profile != None and profile != 'None' and profile != '':
            dburl += "profile=%s&" % profile
        dburl += "format=%s" % format

        r = self.getFromURL(self.svc_url, dburl, def_token(token))
        profiles = scToString(r.content)
        if '{' in profiles:
            profiles = json.loads(profiles)

        return scToString(profiles)


    # --------------------------------------------------------------------
    # SERVICES -- List public storage services
    #
    def services(self, name=None, svc_type='vos', format=None,
                  profile='default'):
        return self._services(name=name, svc_type=svc_type, format=format,
                                   profile=profile)

    def _services(self, name=None, svc_type='vos', format=None,
                   profile='default'):
        '''
        '''
        dburl = '/services?'
        if profile is not None and profile != 'None' and profile != '':
            dburl += ("profile=%s" % profile)
        if name is not None and name != 'None' and name != '':
            dburl += ("&name=%s" % name.replace('%','%25'))
        if svc_type is not None and svc_type != 'None' and svc_type != '':
            dburl += ("&type=%s" % svc_type)
        if format is not None and format != 'None' and format != '':
            dburl += "&format=%s" % format

        r = self.getFromURL(self.qm_svc_url, dburl, def_token(None))
        svcs = scToString(r.content)
        if '{' in svcs:
            svcs = json.loads(svcs)

        return scToString(svcs)


    # -----------------------------
    #  Utility Methods
    # -----------------------------

    # --------------------------------------------------------------------
    # ACCESS -- Determine whether the file can be accessed with the given node.
    #
    @multimethod('_sc',3,True)
    def access(self, token, path, mode, verbose=True):
        ''' Usage:  storeClient.access(token, path, mode)
        '''
        return self._access(path=path, mode=mode, token=def_token(token),
                             verbose=verbose)

    @multimethod('_sc',2,True)
    def access(self, path, mode, token=None, verbose=True):
        ''' Usage:  storeClient.access(path, mode)
        '''
        return self._access(path=path, mode=mode, token=def_token(token),
                             verbose=verbose)

    @multimethod('_sc',1,True)
    def access(self, path, mode=None, token=None, verbose=True):
        ''' Usage:  storeClient.access(path, mode)
        '''
        return self._access(path=path, mode=mode, token=def_token(token),
                             verbose=verbose)

    def _access(self, path='', mode='', token=None, verbose=True):
        '''Implementation of the access() method.
        '''
        uri = (path if path.count('://') > 0 else 'vos://' + path)
        url = self.svc_url + ("/access?name=%s&mode=%s&verbose=%s" % \
                         (uri,mode,verbose))
        r = requests.get(url, headers={'X-DL-AuthToken': def_token(token)})
        if r.status_code != requests.codes.ok:
            return False
        else:
            val = scToString(r.content).lower()
            return (True if val == 'true' else False)
        pass


    # --------------------------------------------------------------------
    # STAT -- Get file status. Values are returned as a dictionary of the
    #         requested node.
    #
    @multimethod('_sc',2,True)
    def stat(self, token, path, verbose=True):
        ''' Usage:  storeClient.stat(token, path)
        '''
        return self._stat(path=path, token=def_token(token), verbose=verbose)

    @multimethod('_sc',1,True)
    def stat(self, path, token=None, verbose=True):
        ''' Usage:  storeClient.stat(path)
        '''
        return self._stat(path=path, token=def_token(token), verbose=verbose)

    def _stat(self, path='', token=None, verbose=True):
        '''Implementation of the stat() method.
        '''

        uri = (path if path.count('://') > 0 else 'vos://' + path)
        url = self.svc_url + ("/stat?name=%s&verbose=%s" % (uri,verbose))
        r = requests.get(url, headers={'X-DL-AuthToken': def_token(token)})
        if r.status_code != requests.codes.ok:
            return {}
        else:
            return json.loads(scToString(r.content))


    # --------------------------------------------------------------------
    # GET -- Retrieve a file from the store manager service
    # --------------------------------------------------------------------
    @multimethod('_sc',3,True)
    def get(self, token, fr, to, mode='text', verbose=True, debug=False,
            timeout=30):
        ''' Usage:  storeClient.get(token, fr, to)
        '''
        return self._get(fr=fr, to=to, token=def_token(token),
                          mode=mode, verbose=verbose, debug=debug,
                          timeout=timeout)

    @multimethod('_sc',2,True)
    def get(self, opt1, opt2, fr='', to='', token=None, verbose=True,
             mode='text', debug=False, timeout=30):
        ''' Usage:  storeClient.get(fr, to)
        '''
        if opt1 is not None and len(opt1.split('.')) >= 4:
            # opt1 looks like a token
            return self._get(fr=opt2, to=to, token=def_token(opt1),
                              mode=mode, verbose=verbose, debug=debug,
                              timeout=timeout)
        else:
            # opt1 is the 'fr' value, opt2 is the 'to' value
            return self._get(fr=opt1, to=opt2, token=def_token(token),
                              mode=mode, verbose=verbose, debug=debug,
                              timeout=timeout)

    @multimethod('_sc',1,True)
    def get(self, optval, fr='', to='', token=None, mode='text',
             verbose=True, debug=False, timeout=30):
        ''' Usage:  storeClient.get(fr)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self._get(fr=fr, to=to, token=def_token(optval),
                              mode=mode, verbose=verbose, debug=debug,
                              timeout=timeout)
        else:
            # optval is the 'fr' value
            return self._get(fr=optval, to=to, token=def_token(token),
                              mode=mode, verbose=verbose, debug=debug,
                              timeout=timeout)

    @multimethod('_sc',0,True)
    def get(self, fr='', to='', token=None, mode='text', verbose=True,
             debug=False, timeout=30):
        ''' Usage:  storeClient.get(token, fr, to)
        '''
        return self._get(fr=fr, to=to, token=def_token(token),
                          mode=mode, verbose=verbose, debug=debug,
                          timeout=timeout)

    def _get(self, token=None, fr='', to='', mode='text', verbose=True,
              debug=False, timeout=30):
        '''Implementation of the get() method.
        '''

        def sizeof_fmt(num):
            '''Local pretty-printer for file sizes.
            '''
            for unit in ['B','K','M','G','T','P','E','Z']:
                if abs(num) < 1024.0:
                    if unit == 'B':
                        return "%5d%s" % (num, unit)
                    else:
                        return "%3.1f%s" % (num, unit)
                num /= 1024.0
            return "%.1f%s" % (num, 'Y')


        tok = def_token(token)
        user, uid, gid, hash = tok.strip().split('.', 3)
        hdrs = {'Content-Type': 'text/ascii',
                'X-DL-ClientVersion': __version__,
                'X-DL-OriginIP': self.hostip,
                'X-DL-OriginHost': self.hostname,
                'X-DL-User': user,
                'X-DL-AuthToken': tok}                  # application/x-sql

        # Patch the names with the default URI prefix if needed.
        nm = (fr if fr.count("://") > 0 else ("vos://" + fr))
        nm = nm.replace('///','//')

        if debug:
            print("get(): nm = %s" % nm)
        if hasmeta(fr):
            if to == '' or to is None:
                raise storeClientError("Multi-file requests require a download location")
            if not os.path.exists(to):
                raise storeClientError("Download directory does not exist")
            if not os.path.isdir(to):
                raise storeClientError("Location must be specified as a directory")

        if to != '' and to is not None:
            # Expand metacharacters to create a file list for download.
            flist = expandFileList(self.svc_url, token, nm, "csv", full=True)

            nfiles = len(flist)
            if nfiles < 1:
                return 'A Node does not exist with the requested URI.'

            if debug:
                print("get: flist = %s" % flist)
                print("get: nfiles = %s" % nfiles)
            fnum = 1
            resp = []
            for f in flist:
                nfiles = len(flist)     # recompute in case list was modified

                # Generate the download file path.
                junk, fn = os.path.split(f)
                if to.endswith("/"):
                    dlname = ((to + fn) if hasmeta(fr) else to)
                else:
                    dlname = ((to + "/" + fn) if hasmeta(fr) else to)

                # Get a single file.
                res = requests.get(self.svc_url + "/get?name=%s" % f, 
                                   headers=hdrs)

                if res.status_code != 200:
                    resp.append("Error: " + scToString(res.text))
                else:
                    r = None
                    for i in range(1,timeout):
                        try:
                            r = requests.get(res.text, stream=True)
                        except Exception as e:
                            if "No connection adapters" in str(e) and i%5 == 0:
                                print('GET error %d: retrying' % i)
                            if "Internal Server Error" in str(e) and i%5 == 0: 
                                print('GET internal error %d: retrying' % i)
                            time.sleep(1)
                            if i == (timeout-1):
                                r = None
                                break
                        else:
                            break

                    if r is None:
                        # If the download failed, put it back at the end
                        # of the list so we can retry later.
                        flist.append(f)
                    elif r.status_code != 200:
                        resp.append(scToString(r.content))
                    else:
                        clen = r.headers.get('content-length')
                        total_length = (0 if clen is None else int(clen))

                        # Download the file in chunks so we can have a progress
                        # indicator on each.
                        dl = 0
                        done = 0
                        with open(dlname, 'wb', 0) as fd:

                            while 1:
                                buf = r.raw.read((8*1024))
                                dl += len(buf)
                                if not buf:
                                    break
                                fd.write(buf)
                                if total_length > 0:
                                    done = int(20 * dl / total_length)
                                if verbose:     # Print a progress indicator
                                    sys.stdout.write(
                                        "\r(%d/%d) [%s%s] [%7s] %s" % \
                                        (fnum, nfiles, '='*done, ' '*(20-done),
                                        sizeof_fmt(dl), f[6:]))
                                    sys.stdout.flush()

                            # If the download failed, put it back at the end
                            # of the list so we can retry later.
                            if total_length > 0 and dl == 0:
                                flist.append(f)

                            # Handle a zero-length file download.
                            if verbose:
                                if dl == 0:
                                    print("\r(%d/%d) [%s] [%7s] %s" % \
                                        (fnum, nfiles, '=' * 20, "0 B", f[6:]))
                                else:
                                    print('')
                        fd.close()
                        resp.append('OK')
                fnum += 1
                if fnum % timeout == 0:
                    time.sleep(5)

            return resp

        else:
            # Get a single file, return the raw contents to the caller.
            url = requests.get(self.svc_url + "/get?name=%s" % nm,
                               headers=hdrs)
            r = requests.get(url.text, stream=False, headers=hdrs)
            if mode == 'text':
                return scToString(r.content)
            elif mode == 'fileobj':
                from astropy.utils.data import get_readable_fileobj
                from io import BytesIO
                try:
                    fileobj = BytesIO(r.content)
                    with get_readable_fileobj(fileobj, encoding='binary',
                                               cache=True) as f:
                        return f
                except Exception as e:
                    raise storeClientError(str(e))
            else:
                return scToString(r.content)


    # --------------------------------------------------------------------
    # PUT -- Upload a file to the store manager service
    # --------------------------------------------------------------------
    @multimethod('_sc',3,True)
    def put(self, token, fr, to, verbose=True, debug=False):
        ''' Usage:  storeClient.put(token, fr, to)
        '''
        return self._put(fr=fr, to=to, token=def_token(token),
                          verbose=verbose, debug=False)

    @multimethod('_sc',2,True)
    def put(self, fr, to, token=None, verbose=True, debug=False):
        ''' Usage:  storeClient.put(fr, to)
        '''
        return self._put(fr=fr, to=to, token=def_token(token),
                          verbose=verbose, debug=False)

    @multimethod('_sc',1,True)
    def put(self, optval, fr='', to='vos://', token=None, verbose=True,
             debug=False):
        ''' Usage:  storeClient.put(fr)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self._put(fr=fr, to=to, token=def_token(optval),
                              verbose=verbose, debug=False)
        else:
            # optval looks like a source name
            return self._put(fr=optval, to=to, token=def_token(token),
                              verbose=verbose, debug=False)

    @multimethod('_sc',0,True)
    def put(self,fr='', to='vos://', token=None, verbose=True, debug=False):
        ''' Usage:  storeClient.put(fr='',to='')
        '''
        return self._put(fr=fr, to=to, token=def_token(token),
                          verbose=verbose, debug=debug)

    def _put(self, token=None, fr='', to='vos://', verbose=True, debug=False):
        '''Implementation of the put() method.
        '''
        tok = def_token(token)
        user, uid, gid, hash = tok.strip().split('.', 3)
        hdrs = {'Content-Type': 'text/ascii',
                'X-DL-ClientVersion': __version__,
                'X-DL-OriginIP': self.hostip,
                'X-DL-OriginHost': self.hostname,
                'X-DL-User': user,
                'X-DL-AuthToken': tok}                  # application/x-sql

        # If the 'fr' is a directory, create it first and then transfer the
        # contents.
        if os.path.isdir(fr):
            if fr.endswith("/"):
                dname = (to if to.count("://") > 0 else to[:-1])
                self._mkdir(token=token, name=dname)
            flist = glob.glob(fr+"/*")
        else:
            dname = ''
            flist = glob.glob(fr)
        nfiles = len(flist)

        if debug:
            print("fr=%s  to=%s  dname=%s" % (fr, to, dname))
            print(flist)

        if nfiles > 1:
            #pstat = stat(to)
            #ptype = pstat.get('type')
            ptype = stat(to).get('type')
            if ptype is None:
                return ['Error: target directory not exist']
            elif ptype != 'container':
                return ['Error: target must be a container']

        fnum = 1
        resp = []
        for f in flist:
            if debug:
                print("put: f=%s" % (f))
            fr_dir, fr_name = os.path.split(f)

            # Patch the names with the URI prefix if needed.
            nm = (to if to.count("://") > 0 else ("vos://" + to))
            if to.endswith("/"):
                nm = nm + fr_name
            if is_vosDir(self.svc_url, token, nm):
                nm = nm + '/' + fr_name
            nm = nm.replace('///','//')      # fix extra path indicators

            if debug:
                print("put: fr_dir=%s  fr_name=%s" % (fr_dir,fr_name))
                print("put: f=%s  nm=%s" % (f,nm))

            if not os.path.exists(f):
                # Skip files that don't exist
                resp.append("Error: Local file '%s' does not exist" % f)
                if verbose:
                    print("Error: Local file '%s' does not exist" % f)
                continue

            r = requests.get(self.svc_url + "/put?name=%s" % nm, headers=hdrs)

            # Cannot upload directly to a container
            # if r.status_code == 500 and \
            #    r.content == "Data cannot be uploaded to a container":
            # This is now handles above where we check for a container using is_vosDir
            if r.status_code == requests.codes.server_error:
                resp.append(scToString(r.content))
            else:
                try:
                    if verbose:
                        sys.stdout.write("(%d / %d) %s -> " % (fnum, nfiles, f))

                    # This *should* work for large data files - MJG 05/24/17
                    with open(f, 'rb') as file:
                        requests.put(r.content, data=file,
                             headers={'Content-type': 'application/octet-stream',
                                      'X-DL-AuthToken': token})
                    if verbose:
                        sys.stdout.write("%s\n" % nm)

                except Exception as e:
                    resp.append(str(e))
                else:
                    resp.append('OK')
            fnum += 1

        return 'OK' if not resp else resp


    # --------------------------------------------------------------------
    # LOAD -- Load a file from a remote endpoint to the store manager service
    # --------------------------------------------------------------------
    @multimethod('_sc',3,True)
    def load(self, token, name, endpoint, is_vospace=False):
        ''' Usage:  storeClient.load(token, name, endpoint)
        '''
        return self._load(name=name, endpoint=endpoint,
                           token=def_token(token), is_vospace=is_vospace)

    @multimethod('_sc',2,True)
    def load(self, name, endpoint, token=None, is_vospace=False):
        ''' Usage:  storeClient.load(name, endpoint)
        '''
        return self._load(name=name, endpoint=endpoint,
                           token=def_token(token), is_vospace=is_vospace)

    # Aliases for load() calls.

    @multimethod('_sc',3,True)
    def pull(self, token, name, endpoint, is_vospace=False):
        ''' Usage:  storeClient.pull(token, name, endpoint)
        '''
        return self._load(name=name, endpoint=endpoint,
                           token=def_token(token), is_vospace=is_vospace)

    @multimethod('_sc',2,True)
    def pull(self, name, endpoint, token=None, is_vospace=False):
        ''' Usage:  storeClient.pull(name, endpoint)
        '''
        return self._load(name=name, endpoint=endpoint,
                           token=def_token(token), is_vospace=is_vospace)

    def _load(self, token=None, name='', endpoint='', is_vospace=False):
        '''Implementation of the load() method.
        '''
        try:
            from urllib import quote_plus               # Python 2
        except ImportError:
            from urllib.parse import quote_plus         # Python 3

        uri = (name if name.count('://') > 0 else 'vos://' + name)
        r = self.getFromURL(self.svc_url,
                            "/load?name=%s&endpoint=%s&is_vospace=%s" % \
                            (uri, quote_plus(endpoint), str(is_vospace)),
                            def_token(token))
        return scToString(r.content)


    # --------------------------------------------------------------------
    # CP -- Copy a file/directory within the store manager service
    # --------------------------------------------------------------------
    @multimethod('_sc',3,True)
    def cp(self, token, fr, to, verbose=False):
        ''' Usage:  storeClient.cp(token, fr, to)
        '''
        return self._cp(fr=fr, to=to, token=def_token(token), verbose=verbose)

    @multimethod('_sc',2,True)
    def cp(self, fr, to, token=None, verbose=False):
        ''' Usage:  storeClient.cp(fr, to)
        '''
        return self._cp(fr=fr, to=to, token=def_token(token), verbose=verbose)

    @multimethod('_sc',1,True)
    def cp(self, token, fr='', to='', verbose=False):
        ''' Usage:  storeClient.cp(fr, to)
        '''
        return self._cp(fr=fr, to=to, token=def_token(token), verbose=verbose)

    @multimethod('_sc',0,True)
    def cp(self, token=None, fr='', to='', verbose=False):
        ''' Usage:  storeClient.cp(fr, to)
        '''
        return self._cp(fr=fr, to=to, token=def_token(token), verbose=verbose)

    def _cp(self, token=None, fr='', to='', verbose=False):
        '''Implementation of the cp() method.
        '''
        # Patch the names with the URI prefix if needed.
        fr_rem = fr.count("://") > 0
        to_rem = to.count("://") > 0
        if not fr_rem and not to_rem:
            src = "vos://" + fr
            dest = "vos://" + to
        elif not fr_rem:
            return "Cannot copy from local to remote; use put() instead."
        elif not to_rem:
            return "Cannot copy from remote to local; use get() instead."
        else:
            src = fr
            dest = to

        # If the 'from' string has no metachars we're copying a single file,
        # otherwise expand the file list and process the matches individually.
        if not hasmeta(fr):
            src = src.replace('///','//')
            dest = dest.replace('///','//')
            r = self.getFromURL(self.svc_url, "/cp?from=%s&to=%s" % \
                                   (src, dest), def_token(token))
            if 'COMPLETED' in scToString(r.content):
                return "OK"
            else:
                return scToString(r.content)
        else:
            flist = expandFileList(self.svc_url, token, src, "csv", full=True)
            nfiles = len(flist)
            fnum = 1
            resp = []
            for f in flist:
                junk, fn = os.path.split(f)
                to_fname = (dest + ('/%s' % fn)).replace('///','//')
                if verbose:
                    print("(%d / %d) %s -> %s" % (fnum, nfiles, f, to_fname))
                r = self.getFromURL(self.svc_url, "/cp?from=%s&to=%s" % \
                                       (f, to_fname), def_token(token))
                fnum += 1
                if 'COMPLETED' in scToString(r.content):
                    resp.append("OK")
                else:
                    resp.append(scToString(r.content))

            return resp


    # --------------------------------------------------------------------
    # LN -- Create a link to a file/directory in the store manager service
    # --------------------------------------------------------------------
    @multimethod('_sc',3,True)
    def ln(self, token, fr, target, verbose=False):
        ''' Usage:  storeClient.ln(token, fr, target)
        '''
        return self._ln(fr=fr, target=target, token=def_token(token),
                           verbose=verbose)

    @multimethod('_sc',2,True)
    def ln(self, fr, target, token=None, verbose=False):
        ''' Usage:  storeClient.ln(fr, target)
        '''
        return self._ln(fr=fr, target=target, token=def_token(token),
                           verbose=verbose)

    @multimethod('_sc',1,True)
    def ln(self, token, fr='', target='', verbose=False):
        ''' Usage:  storeClient.ln(fr, target)
        '''
        return self._ln(fr=fr, target=target, token=def_token(token),
                           verbose=verbose)

    def _ln(self, token=None, fr='', target='', verbose=True):
        '''Implementation of the ln() method.
        '''
        try:
            fro = (fr if fr.count('://') > 0 else 'vos://' + fr)
            to = (target if target.count('://') > 0 else 'vos://' + target)
            r = self.getFromURL(self.svc_url, "/ln?from=%s&to=%s" % \
                                   (fro, to), def_token(token))
            if r.status_code != requests.codes.created:
                return scToString(r.content)
            else:
                return 'OK'
        except Exception:
            raise storeClientError(r.content)


    # --------------------------------------------------------------------
    # LS -- Get a file/directory listing from the store manager service
    # --------------------------------------------------------------------
    @multimethod('_sc',2,True)
    def ls(self, token, name, format='csv', verbose=False):
        ''' Usage:  storeClient.ls(token, name)
        '''
        return self._ls(name=name, format=format, token=def_token(token),
                         verbose=verbose)

    @multimethod('_sc',1,True)
    def ls(self, optval, name='vos://', token=None, format='csv',
           verbose=False):
        ''' Usage:  storeClient.ls(name)
             Usage:  storeClient.ls(token, name='foo')
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self._ls(name=name, format=format,
                             token=def_token(optval), verbose=verbose)
        else:
            return self._ls(name=optval, format=format, token=def_token(None),
                             verbose=verbose)

    @multimethod('_sc',0,True)
    def ls(self, name='vos://', token=None, format='csv', verbose=False):
        ''' Usage:  storeClient.ls()
        '''
        return self._ls(name=name, format=format, token=def_token(token),
                         verbose=verbose)

    def _ls(self, token=None, name='vos://', format='csv', verbose=False):
        '''Implementation of the ls() method.
        '''
        try:
            uri = (name if name.count('://') > 0 else 'vos://' + name)
            r = self.getFromURL(self.svc_url,
                                   "/ls?name=%s&format=%s&verbose=%s" % \
                                   (uri, format, verbose), def_token(token))
        except:
            raise Exception(scToString(r.content))
        return(scToString(r.content))


    # --------------------------------------------------------------------
    # MKDIR -- Create a directory in the store manager service
    # --------------------------------------------------------------------
    @multimethod('_sc',2,True)
    def mkdir(self, token, name):
        ''' Usage:  storeClient.mkdir(token, name)
        '''
        return self._mkdir(name=name, token=def_token(token))

    @multimethod('_sc',1,True)
    def mkdir(self, optval, name='', token=None):
        ''' Usage:  storeClient.mkdir(name)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            return self._mkdir(name=name, token=def_token(optval))
        else:
            return self._mkdir(name=optval, token=def_token(token))

    def _mkdir(self, token=None, name=''):
        '''Implementation of the mkdir() method.
        '''
        nm = (name if name.count("://") > 0 else ("vos://" + name))
        if nm and nm[-1] == '/': nm = nm[:-1]

        try:
            r = self.getFromURL(self.svc_url, "/mkdir?dir=%s" % nm,
                                   def_token(token))
            if r.status_code != requests.codes.created: return scToString(r.content)
            else: return 'OK'
        except Exception:
            raise storeClientError(r.content)
        else:
            return 'OK'


    # --------------------------------------------------------------------
    # MV -- Move/rename a file/directory within the store manager service
    # --------------------------------------------------------------------
    @multimethod('_sc',3,True)
    def mv(self, token, fr, to, verbose=False):
        ''' Usage:  storeClient.mv(token, fr, to)
        '''
        return self._mv(fr=fr, to=to, token=def_token(token), verbose=verbose)

    @multimethod('_sc',2,True)
    def mv(self, fr, to, token=None, verbose=False):
        ''' Usage:  storeClient.mv(fr, to)
        '''
        return self._mv(fr=fr, to=to, token=def_token(token), verbose=verbose)

    @multimethod('_sc',1,True)
    def mv(self, token, fr='', to='', verbose=False):
        ''' Usage:  storeClient.mv(fr, to)
        '''
        return self._mv(fr=fr, to=to, token=def_token(token), verbose=verbose)

    @multimethod('_sc',0,True)
    def mv(self, token=None, fr='', to='', verbose=False):
        ''' Usage:  storeClient.mv(fr, to)
        '''
        return self._mv(fr=fr, to=to, token=def_token(token), verbose=verbose)

    def _mv(self, token=None, fr='', to='', verbose=False):
        '''Implementation of the mv() method.
        '''
        # Patch the names with the URI prefix if needed.
        fr_rem = fr.count("://") > 0
        to_rem = to.count("://") > 0
        if not fr_rem and not to_rem:
            src = "vos://" + fr
            dest = "vos://" + to
        elif not fr_rem:
            return "Cannot move from local to remote; use put() instead."
        elif not to_rem:
            return "Cannot move from remote to local; use get() instead."
        else:
            src = fr
            dest = to

        # If the 'from' string has no metachars, we're copying a single file,
        # otherwise expand the file list on the and process the matches
        # individually.
        if not hasmeta(fr):
            src = src.replace('///','//')
            dest = dest.replace('///','//')
            r = self.getFromURL(self.svc_url, "/mv?from=%s&to=%s" % \
                                   (src, dest), def_token(token))
            if 'COMPLETED' in scToString(r.content):
                return "OK"
            else:
                return scToString(r.content)
        else:
            flist = expandFileList(self.svc_url, token, src, "csv", full=True)
            nfiles = len(flist)
            fnum = 1
            resp = []
            for f in flist:
                junk, fn = os.path.split(f)
                to_fname = (dest + ('/%s' % fn)).replace('///','//')
                if verbose:
                    print("(%d / %d) %s -> %s" % (fnum, nfiles, f, to_fname))
                r = self.getFromURL(self.svc_url, "/mv?from=%s&to=%s" % \
                                       (f,to_fname), def_token(token))
                fnum += 1
                if 'COMPLETED' in scToString(r.content):
                    resp.append("OK")
                else:
                    resp.append(scToString(r.content))
            return resp


    # --------------------------------------------------------------------
    # RM -- Delete a file from the store manager service
    # --------------------------------------------------------------------
    @multimethod('_sc',2,True)
    def rm(self, token, name, verbose=False):
        ''' Usage:  storeClient.rm(token, name)
        '''
        return self._rm(name=name, token=def_token(token), verbose=verbose)

    @multimethod('_sc',1,True)
    def rm(self, optval, name='', token=None, verbose=False):
        ''' Usage:  storeClient.rm(name)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self._rm(name=name,token=def_token(optval),verbose=verbose)
        else:
            return self._rm(name=optval,token=def_token(token),verbose=verbose)

    @multimethod('_sc',0,True)
    def rm(self, name='', token=None, verbose=False):
        ''' Usage:  storeClient.rm(name)
        '''
        return self._rm(name=name, token=def_token(token), verbose=verbose)

    def _rm(self, token=None, name='', verbose=False):
        '''Implementation of the rm() method.
        '''
        # Patch the names with the URI prefix if needed.
        nm = (name if name.count("://") > 0 else ("vos://" + name))
        if nm == "vos://" or nm == "vos://tmp" or nm == "vos://public":
            return "Error: operation not permitted"

        # If the 'name' string has no metacharacters we're removing a single file,
        # otherwise expand the file list on the and process the matches
        # individually.
        if not hasmeta(nm):
            r = is_vosDir(self.svc_url, token, nm)
            if not isinstance(r, bool): return scToString(r.content)
            elif r: return "%s is a directory." % name

            r = self.getFromURL(self.svc_url, "/rm?file=%s" % nm, def_token(token))
            if r.status_code != requests.codes.no_content: return scToString(r.content)
            else: return 'OK'
        else:
            flist = expandFileList(self.svc_url, token, nm, "csv", full=True)
            nfiles = len(flist)
            if nfiles < 1:
                return 'A Node does not exist with the requested URI.'
            fnum = 1
            resp = []
            for f in flist:
                if verbose: print("(%d / %d) %s" % (fnum, nfiles, f))
                resp.append(self._rm(token=token, name=f, verbose=verbose))
                fnum += 1
            return resp


    # --------------------------------------------------------------------
    # RMDIR -- Delete a directory from the store manager service
    # --------------------------------------------------------------------
    @multimethod('_sc',2,True)
    def rmdir(self, token, name, verbose=False):
        ''' Usage:  storeClient.rmdir(token, name)
        '''
        return self._rmdir(name=name, token=def_token(token), verbose=verbose)

    @multimethod('_sc',1,True)
    def rmdir(self, optval, name='', token=None, verbose=False):
        ''' Usage:  storeClient.rmdir(name)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            return self._rmdir(name=name, token=def_token(optval),
                                verbose=verbose)
        else:
            return self._rmdir(name=optval, token=def_token(token),
                                verbose=verbose)

    @multimethod('_sc',0,True)
    def rmdir(self, name='', token=None, verbose=False):
        ''' Usage:  storeClient.rm(name)
        '''
        return self._rmdir(name=name, token=def_token(token), verbose=verbose)

    def _rmdir(self, token=None, name='', verbose=False):
        '''Implementation of the rmdir() method.
        '''
        # FIXME - Should handle file templates(?)

        # Patch the names with the URI prefix if needed.
        nm = (name if name.count("://") > 0 else ("vos://" + name))
        if nm == "vos://" or nm == "vos://tmp" or nm == "vos://public":
            return "Error: operation not permitted"
        if nm and nm[-1] == '/': nm = nm[:-1]
        r = is_vosDir(self.svc_url, token, nm)
        if not isinstance(r, bool): return scToString(r.content)
        elif not r: return "%s is not a directory." % name
        try:
            r = self.getFromURL(self.svc_url, "/rmdir?dir=%s" % nm,
                                   def_token(token))
            if r.status_code != requests.codes.no_content: return scToString(r.content)
            else: return 'OK'
        except Exception as e:
            print('storeClient._rmdir: error: ' + scToString(r.content))
            raise storeClientError(str(e))
        else:
            return 'OK'


    # --------------------------------------------------------------------
    # SAVEAS -- Save the string representation of a data object as a file.
    # --------------------------------------------------------------------
    @multimethod('_sc',3,True)
    def saveAs(self, token, data, name):
        ''' Usage:  storeClient.saveAs(token, data, name)
        '''
        return self._saveAs(data=data, name=name, token=def_token(token))

    @multimethod('_sc',2,True)
    def saveAs(self, data, name, token=None):
        ''' Usage:  storeClient.saveAs(data, name)
        '''
        return self._saveAs(data=data, name=name, token=def_token(token))

    def _saveAs(self, token=None, data='', name=''):
        '''Implementation of the saveAs() method.
        '''
        import tempfile

        try:
            with tempfile.NamedTemporaryFile(mode='w',delete=False) as tfd:
                tfd.write(str(data))
                tfd.flush()
                tfd.close()
        except Exception as e:
            raise storeClientError(str(e))

        # Patch the names with the URI prefix if needed.
        nm = (name if name.count("://") > 0 else ("vos://" + name))

        # Put the temp file to the VOSpace.
        resp = self._put(token=token, fr=tfd.name, to=nm, verbose=False)

        os.unlink(tfd.name)                # Clean up

        return resp


    # --------------------------------------------------------------------
    # TAG -- Annotate a file/directory in the store manager service
    # --------------------------------------------------------------------
    @multimethod('_sc',3,True)
    def tag(self, token, name, tag):
        ''' Usage:  storeClient.tag(token, name, tag)
        '''
        return self._tag(name=name, tag=tag, token=def_token(token))

    @multimethod('_sc',2,True)
    def tag(self, name, tag, token=None):
        ''' Usage:  storeClient.tag(name, tag)
        '''
        return self._tag(name=name, tag=tag, token=def_token(token))

    @multimethod('_sc',1,True)
    def tag(self, token, name='', tag=''):
        ''' Usage:  storeClient.tag(token, name='foo', tag='bar')
        '''
        return self._tag(name=name, tag=tag, token=def_token(token))

    def _tag(self, token=None, name='', tag=''):
        '''Implementation of the saveAs() method.
        '''
        try:
            r = self.getFromURL(self.svc_url, "/tag?name=%s&tag=%s" % \
                                   (name, tag), def_token(token))
        except Exception:
            raise storeClientError(scToString(r.content))
        else:
            if r.status_code == requests.codes.ok:
                return 'OK'
            else:
                return scToString(r.content)


    def getFromURL(self, svc_url, path, token):
        '''Get something from a URL.  Return a 'response' object
        '''
        try:
            tok = def_token(token)
            user, uid, gid, hash = tok.strip().split('.', 3)

            hdrs = {'Content-Type': 'text/ascii',
                    'X-DL-ClientVersion': __version__,
                    'X-DL-OriginIP': self.hostip,
                    'X-DL-OriginHost': self.hostname,
                    'X-DL-User': user,
                    'X-DL-AuthToken': tok}  		# application/x-sql

            resp = requests.get("%s%s" % (svc_url, path), headers=hdrs)

        except Exception as e:
            raise storeClientError(str(e))
        return resp




# -------------------------------------------------------
#  Utility Methods
# -------------------------------------------------------

def hasmeta(s):
    '''Determine whether a string contains filename meta-characters.
    '''
    return (s.find('*') >= 0) or (s.find('[') >= 0) or (s.find('?') > 0)


def is_vosDir(svc_url, token, path):
    '''Determine whether 'path' is a ContainerNode in the VOSpace.
    '''
    url = svc_url + ("/isdir?name=%s" % (path))
    r = requests.get(url, headers={'X-DL-AuthToken': def_token(token)})
    if r.status_code != requests.codes.ok:
        return r
    else:
        return (True if scToString(r.content).lower() == 'true' else False)

def expandFileList(svc_url, token, pattern, format, full=False):
    '''Expand a filename pattern in a VOSpace URI to a list of files.  We
        do this by getting a listing of the parent container contents from
        the service and then match the pattern on the client side.
    '''
    debug = False

    # Check first that we're only getting a single file.
    if not hasmeta(pattern) and not pattern.endswith('/'):
        flist = [pattern]
        return (flist)

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
        print("-----------------------------------------")
        print("INPUT PATTERN = '" + str + "'")
        print("PATTERN = '" + str + "'")
        print('str = ' + str)
        print("split: '%s' '%s'" % (dir, name))

    pstr = (name if (name is not None and hasmeta(name)) else "*")

    if dir is not None:
        if dir == "/" and name is not None:
            dir = dir + name
        else:
            if dir.endswith("/"):
                dir = dir[:-1]                          # trim trailing '/'
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

    # Check to make sure the parent exists and is a container
    if debug:
        print ('stat of dir :  ' + (uri+dir))
    pstat = stat(uri+dir)
    if pstat.get('type') == 'link':
        dir = pstat['target']
        dir = dir[dir.index('://')+3:]
        pstat = stat(dir)
    if pstat.get('type') != 'container':
        return 'A Container does not exist with the requested URI.'

    # Make the service call to get a listing of the parent directory.
    url = svc_url + "/ls?name=%s%s&format=%s" % (uri, dir, "csv")
    r = requests.get(url, headers={'X-DL-AuthToken': def_token(token)})

    # Filter the directory contents list using the filename pattern.
    list = []
    flist = scToString(r.content).split(',')
    for f in flist:
        if f and (fnmatch.fnmatch(f, pstr) or f == pstr):
            furi = (f if not full else (uri + dir + "/" + f))
            list.append(furi.replace("///", "//"))

    if debug:
        print(url)
        print("%s --> '%s' '%s' '%s' => '%s'" % (pattern,uri,dir,name,pstr))

    return sorted(list)



# ###################################
#  Utility Methods
# ###################################

def chunked_upload(token, local_file, remote_file):
    '''A streaming file uploader.
    '''
    debug = False
    init = True
    CHUNK_SIZE = 4 * 1024 * 1024                   # 16MB chunks
    url = '%s/xfer' % (sc_client.svc_url)

    # Get the size of the file to be transferred.
    fsize = os.stat(local_file).st_size
    nchunks = fsize / CHUNK_SIZE + 1
    if (debug): print('Upload in %d chunks' % nchunks)
    with open(local_file, 'rb') as f:
        try:
            nsent = 0
            while nsent < fsize:
                data = f.read(CHUNK_SIZE)
                requests.post(url, data,
                    headers={'Content-type': 'application/octet-stream',
                             'X-DL-FileName': remote_file,
                             'X-DL-InitXfer': str(init),
                             'X-DL-AuthToken': token})
                nsent += len(data)
                if init: init = False
        except Exception as e:
            raise storeClientError('Upload error: ' + str(e))



# ###################################
#  Store Client Handles
# ###################################

def getClient(profile=DEF_PROFILE, svc_url=DEF_SERVICE_URL):
    ''' Create a new storeClient object and set a default profile.
    '''
    return storeClient(profile=profile, svc_url=svc_url)

# The default client handle for the module.
sc_client = getClient(profile=DEF_PROFILE, svc_url=DEF_SERVICE_URL)


# ##########################################
#  Patch the docstrings for module functions
#  that aren't MultiMethods.
# ##########################################

isAlive.__doc__ = sc_client.isAlive.__doc__
services.__doc__ = sc_client.services.__doc__
set_svc_url.__doc__ = sc_client.set_svc_url.__doc__
get_svc_url.__doc__ = sc_client.get_svc_url.__doc__
set_profile.__doc__ = sc_client.set_profile.__doc__
get_profile.__doc__ = sc_client.get_profile.__doc__


# ####################################################################
#  Py2/Py3 Compatability Utilities
# ####################################################################

def scToString(s):
    '''scToString -- Force a return value to be type 'string' for all
                     Python versions.  If there is an error, return the
                     original.
    '''
    try:
        if is_py3:
            if isinstance(s,bytes):
                strval = str(s.decode())
            elif isinstance(s,str):
                strval = s
        else:
            if isinstance(s,bytes) or isinstance(s,unicode):
                strval = str(s)
            else:
                strval = s
    except:
        return s

    return strval
