#!/usr/bin/env python
#
# STORECLIENT -- Client routines for the Data Lab Store Manager service
#

from __future__ import print_function

__authors__ = 'Matthew Graham <graham@noao.edu>, Mike Fitzpatrick <fitz@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20170530'  # yyyymmdd


"""
    Client routines for the DataLab store manager service

Import via

.. code-block:: python

    from dl import storeClient

"""

import os, sys
import fnmatch, glob
import requests
from io import StringIO           # Python 2/3 compatible
import json


#####################################
#  Store manager client procedures
#####################################


DEF_SERVICE_URL = "https://dlsvcs.datalab.noao.edu/storage"
PROFILE = "default"
DEBUG = False


class storeClientError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

def isAlive(svc_url=DEF_SERVICE_URL):
    """ Check whether the StorageManager service at the given URL is
        alive and responding.  This is a simple call to the root 
        service URL or ping() method.
    """
    try:
        r = requests.get(svc_url, timeout=2)
        output = r.content.decode('utf-8')
        status_code = r.status_code
    except Exception:
        return False
    else:
        return (True if (output is not None and status_code == 200) else False)

    
# Pretty-printer for file sizes.
def sizeof_fmt(num):
    for unit in ['B','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            if unit == 'B':
                return "%5d%s" % (num, unit)
            else:
                return "%3.1f%s" % (num, unit)
        num /= 1024.0
    return "%.1f%s" % (num, 'Y')


# GET -- Retrieve a file from the store manager service
def get(token, fr, to, verbose = True):
    """
        Retrieve a file from the store manager service
    """
    debug = False
    headers = {'X-DL-AuthToken': token}

    # Patch the names with the URI prefix if needed.
    nm = (fr if fr.startswith("vos://") else ("vos://" + fr))

    if debug:
        print ("get: nm = %s" % nm)
    if hasmeta(fr):
        if not os.path.isdir(to):
            raise storeClientError("Location must be specified as a directory")
        if to == '':
            raise storeClientError(
                "Multi-file requests require a download location")

    if to != '':
        flist = expandFileList(token, nm, "csv", full=True)
        if debug: print ("get: flist = %s" % flist)
        nfiles = len(flist)
        fnum = 1
        resp = []
        for f in flist:
            junk, fn = os.path.split(f)
            if to.endswith("/"):
                dlname = ((to + fn) if hasmeta(fr) else to)
            else:
                dlname = ((to + "/" + fn) if hasmeta(fr) else to)

            url = requests.get(DEF_SERVICE_URL + "/get?name=%s" % f,
                               headers=headers)

            if url.status_code != 200:
                resp.append("Error: " + url.text)
            else:
                r = requests.get(url.text, stream=True)
                clen = r.headers.get('content-length')
                total_length = (0 if clen is None else int(clen))

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
                            sys.stdout.write ("\r(%d/%d) [%s%s] [%7s] %s" % \
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
                resp.append(r)
            fnum += 1

        return str(resp)

    else:
        url = requests.get(DEF_SERVICE_URL + "/get?name=%s" % nm,
                           headers=headers)
        r = requests.get(url.text, stream=False, headers=headers)
        return r.content
    
    
# PUT -- Upload a file to the store manager service
def put(token, fr, to, verbose=True):
    """
        Upload a file to the store manager service
    """
    debug = False
    headers = {'X-DL-AuthToken': token}

    # If the 'to' is a directory, create it first and then transfer the
    # contents.
    if os.path.isdir (fr):
        if fr.endswith("/"):
            dname = (to if to.startswith("vos://") else to[:-1])
            mkdir (token, dname)
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
            print ("Error: file '%s' does not exist" % f)
            continue

        r = requests.get(DEF_SERVICE_URL + "/put?name=%s" % nm, headers=headers)

        # Cannot upload directly to a container
        # if r.status_code == 500 and r.content == "Data cannot be uploaded to a
        # container":
        if r.status_code == 500:
            file = fr[fr.rfind('/') + 1:]
            nm += '/%s' % f
            r = requests.get(DEF_SERVICE_URL + "/put?name=%s" % nm, 
                                headers=headers)
#        file = open(f).read()

        try:

            if verbose:
                sys.stdout.write ("(%d / %d) %s -> " % (fnum, nfiles, f))

            # This should work for large data files - MJG 05/24/17
            
            with open(f, 'rb') as file:
                requests.put(r.content, data=file,
                     headers={'Content-type': 'application/octet-stream',
                              'X-DL-AuthToken': token})
            if verbose:
                print ("%s" % nm)

        except Exception as e:
            #raise storeClientError(str(e))
            resp.append (str(e))
        else:
            resp.append ("OK")

        fnum += 1
        

# LOAD -- Load a file from a remote endpoint to the store manager service
def load(token, name, endpoint):
    """Load a file from a remote endpoint to the store manager service
    """
    r = getFromURL("/load?name=%s&endpoint=%s" % (name, endpoint), token)

    
# CP -- Copy a file/directory within the store manager service
def cp(token, fr, to, verbose=False):
    """
        Copy a file/directory within the store manager service
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
        flist = expandFileList(token, src, "csv", full=True)
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


# LN -- Create a link to a file/directory in the store manager service
def ln(token, fr, target):
    """
        Create a link to a file/directory in the store manager service
    """
    try:
        r = getFromURL("/ln?from=%s&to=%s" % (fr, target), token)
    except Exception:
        raise storeClientError(r.content.decode('utf-8'))
    else:
        return "OK"

    
# LS -- Get a file/directory listing from the store manager service
def ls(token, name, format = 'csv'):
    """
        Get a file/directory listing from the store manager service

    Parameters
    ----------
    token : str
        Secure token obtained via :func:`dl.auth.login`

    name : str
        Valid name of file or directory, e.g. ``vos://somedir``
    
        .. todo:: [20161110] currently doesn't seem to work.

    format : str
        Default ``str``.

    Example
    -------

    .. code-block:: python

        listing = dl.storeClient.ls(token,name='vos://somedir')
        print listing

    This prints for instance:

    .. code::

        bar2.fits,foo1.csv,fancyfile.dat

    """
    flist = expandFileList(token, name, format, full=False)
    if (format == 'csv'):
        result = ",".join(flist)
        return (result[1:] if result.startswith(",") else result)

    else:
        results = []
        for f in flist:
            url = DEF_SERVICE_URL + "/ls?name=vos://%s&format=%s" % (f, format)
            r = requests.get(url, headers={'X-DL-AuthToken': token})
            results.append(r.content.decode('utf-8'))

        return "\n".join(results)

    
# MKDIR -- Create a directory in the store manager service
def mkdir (token, name):
    """
        Create a directory in the storage manager service
    """
    nm = (name if name.startswith("vos://") else ("vos://" + name))
    
    try:
        r = getFromURL("/mkdir?dir=%s" % nm, token)
    except Exception:
        raise storeClientError(r.content.decode('utf-8'))
    else:
        return "OK"
        
    
# MV -- Move/rename a file/directory within the store manager service
def mv(token, fr, to, verbose = False):
    """
        Move/rename a file/directory within the store manager service
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
        flist = expandFileList(token, src, "csv", full=True)
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

    
# RM -- Delete a file from the store manager service
def rm(token, name, verbose = False):
    """
        Delete a file from the store manager service
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
        flist = expandFileList(token, nm, "csv", full=True)
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

    
# RMDIR -- Delete a directory from the store manager service
def rmdir(token, name):
    """
        Delete a directory from the store manager service
    """

    # Patch the names with the URI prefix if needed.
    nm = (name if name.startswith("vos://") else ("vos://" + name))
    if nm == "vos://" or nm == "vos://tmp" or nm == "vos://public":
        return "Error: operation not permitted"

    try:        
        saveAs (token, "deleted", nm+"/.deleted")
        r = getFromURL("/rmdir?dir=%s" % nm, token)
    except Exception:
        raise storeClientError(r.content)
    else:
        return "OK"


# SAVEAS -- Save the string representation of a data object as a file.
def saveAs(token, data, name):
    """
        Save the string representation of a data object as a file.
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
    put(token, fr=tfd.name, to=nm, verbose=False)

    os.unlink(tfd.name)                # Clean up

    return "OK"

        
# TAG -- Annotate a file/directory in the store manager service
def tag(token, name, tag):
    """
        Annotate a file/directory in the store manager service
    """
    try:
        r = getFromURL("/tag?file=%s&tag=%s" % (name, tag), token)
    except Exception:
        raise storeClientError (r.content.decode('utf-8'))
    else:
        return "OK"
    

# CREATE -- Create a node in the store manager service
def create(token, name, type):
    """
        Create a node in the store manager service
    """
    try:
        r = getFromURL("/create?name=%s&type=%s" % (name, type), token)   
    except Exception:
        raise storeClientError(r.content.decode('utf-8'))
    else:
        return "OK"


# -------------------------------------------------------
#  Utility Methods
# -------------------------------------------------------

def hasmeta(s):
    """ Determine whether a string contains filename meta-characters.
    """
    return (s.find('*') >= 0) or (s.find('[') >= 0) or (s.find('?') > 0)


def expandFileList(token, pattern, format, full=False):
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
    url = DEF_SERVICE_URL + "/ls?name=vos://%s&format=%s" % (dir, "csv")
    r = requests.get(url, headers={'X-DL-AuthToken': token})

    # Filter the directory contents list using the filename pattern.
    list = []
    flist = r.content.decode('utf-8').split(',')
    for f in flist:
        if fnmatch.fnmatch(f, pstr) or f == pstr:
            furi = (f if not full else (uri + dir + "/" + f))
            list.append(furi.replace("///", "//"))

    if debug:
        print (url)
        print ("%s --> '%s' '%s' '%s' => '%s'" % (pattern, uri, dir, name, pstr))

    return sorted(list)
        

# Get from a URL
def getFromURL(path, token):
    try:
        resp = requests.get("%s%s" % (DEF_SERVICE_URL, path), headers = {"X-DL-AuthToken": token})
    except Exception as e:
        raise storeClientError(str(e))
    return resp


# SERVICE_URL -- Set the service url to use
#
def set_svc_url(svc_url):
    """Set the storage manager service URL.

    Parameters
    ----------
    svc_url : str
        The service URL of the storage manager to use 
    
    Returns
    -------

    Example
    -------

    .. code-block:: python

        # set the service url

        url = "http://dldemo.sdm.noao.edu:7003"
        storeClient.set_scv_url(url)

    """
    global DEF_SERVICE_URL
    DEF_SERVICE_URL = svc_url


# PROFILES -- Get the profiles supported by the storage manager service
#
def list_profiles(token, profile = None, format = 'text'):
    """Retrieve the profiles supported by the storage manager service

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`dl.auth.login()`)

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
    """
    
    headers = {'Content-Type': 'text/ascii', 'X-DL-AuthToken': token} # application/x-sql
    dburl = '/profiles?' 
    if profile != None and profile != 'None' and profile != '':
        dburl += "profile=%s&" % profile
    dburl += "format=%s" % format
    
    r = getFromURL(dburl, token)
    profiles = r.content.decode('utf-8')
    if '{' in profiles:
#        profiles = json.load(StringIO(profiles))
        profiles = json.loads(profiles)
    return profiles
    

# PROFILES -- Set the profile to be used
#
def set_profile(profile):
    """Set the profile

    Parameters
    ----------
    profile : str
        The name of the profile to use. The list of available ones can be retrieved from the service (see function :func:`storeClient.list_profiles()`)

    Returns
    -------
    
    Example
    -------

    .. code-block:: python

        # set the profile
        storeClient.set_profile("default")
    """
    
    global PROFILE
    PROFILE = profile
        

# PROFILES -- Set the profile to be used
#
def get_profile(profile):
    """Get the profile

    Parameters
    ----------

    Returns
    -------
    profile : str
        The name of the current profile used with the storage manager service

        
    Example
    -------

    .. code-block:: python

        # get the profile
        storeClient.get_profile()
    """

    return PROFILE
        
    
