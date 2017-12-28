#!/usr/bin/env python
#
# QUERYCLIENT -- Client routines for the Data Lab Query Manager Service
#

from __future__ import print_function

__authors__ = 'Matthew Graham <graham@noao.edu>, Mike Fitzpatrick <fitz@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20170530'  # yyyymmdd


"""
    Client routines for the DataLab Query Manager Service.

Import via

.. code-block:: python

    from dl import queryClient
"""

import requests
try:
    from urllib import quote_plus               # Python 2
except ImportError:
    from urllib.parse import quote_plus         # Python 3
from io import StringIO				# Python 2/3 compatible
import json


#####################################
#  Query manager client procedures
#####################################


DEF_SERVICE_URL = "https://dlsvcs.datalab.noao.edu/query"
SM_SERVICE_URL = "https://dlsvcs.datalab.noao.edu/storage"

PROFILE = "default"
DEBUG = False

TIMEOUT_REQUEST = 120 # sync query timeout default (120sec)


class queryClientError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

def isAlive(svc_url=DEF_SERVICE_URL):
    """ Check whether the QueryManager service at the given URL is
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


# QUERY -- Send a query to the query manager service
#
def query(token, adql=None, sql=None, fmt='csv', out=None, async=False, **kw):
    """Send SQL query to DB.

    Parameters
    ----------
    token : str
        Secure token obtained via :func:`dl.auth.login`

    adql : str or None
        ADQL query string that will be passed to the DB query manager, e.g.

        .. code-block:: python

            adql='select ra,dec from gaia_dr1.gaia_source limit 3'

        If ``adql=None``, then a kwarg ``uri`` must be provided, which
        contains a properly formatted URI to an object (e.g. data
        table) on some remote service, e.g.

        .. code-block:: python

            dl.queryClient.query(token, adql=None, uri=XYZ)

        .. todo:: [20161110] write example once this works

    sql : str or None
        SQL query string that will be passed to the DB query manager, e.g.

        .. code-block:: python

            adql='select ra,dec from gaia_dr1.gaia_source limit 3'

        This will be run as a query directly against the DB.
        If ``sql=None``, then a kwarg ``uri`` must be provided, which
        contains a properly formatted URI to an object (e.g. data
        table) on some remote service, e.g.

        .. code-block:: python

            dl.queryClient.query(token, adql=None, uri=XYZ)

        .. todo:: [20161110] write example once this works

    fmt : str
        Format of the result to be returned by the query. Permitted values are:
          * 'csv'     the returned result is a comma-separated string that looks like a csv file (newlines at the end of every row)
          * 'ascii'   same, but the column separator is a tab \t
          * 'votable' result is a string XML-formatted as a VO table
          * 'fits' FITS binary
          * 'hdf5'    HDF5 file

        .. todo:: [20161110] fits and hdf5 currently don't work 

    out : str or None

        If `None`

        .. todo:: [20161110] write this...

    async : bool
        If ``True``, the query is asynchronous, i.e. a job is
        submitted to the DB, and a job token is returned. The token
        must be then used to check the query's status and to retrieve
        the result (when status is ``COMPLETE``). Default is
        ``False``, i.e. synchroneous query.

    Returns
    -------
    result : str
        If ``async=False``, the return value is the result of the
        query as a formatted string (see ``fmt``). Otherwise the
        result string is a job token, with which later the
        asynchroneaous query's status can be checked
        (:func:`dl.query.status()`), and the result retrieved (see
        :func:`dl.query.result()`.

    Example
    -------
    Get security token first, see :func:`dl.auth.login`. Then:

    .. code-block:: python

        from dl import queryClient
        query = 'select ra,dec from gaia_dr1.gaia_source limit 3'
        response = queryClient.query(token, adql = query, fmt = 'csv')
        print response

    This prints

    .. code::

          ra,dec
          315.002571989537842,35.2662974820284489
          315.00408275885701,35.2665448169895797
          314.996334457679438,35.2673478725552698

    """

    # Set any requested timeout on the call.
    if 'timeout' in kw:
        timeout = int(kw['timeout']) 
        set_timeout_request (timeout)

    # Set service headers.
    headers = {'Content-Type': 'text/ascii',
               'X-DL-TimeoutRequest': str(TIMEOUT_REQUEST),
               'X-DL-ClientVersion': __version__,
               'X-DL-AuthToken': token}  # application/x-sql

    if adql is not None and adql != '':
        query = quote_plus(adql)
        dburl = '%s/query?adql=%s&ofmt=%s&out=%s&async=%s' % (
            DEF_SERVICE_URL, query, fmt, out, async)
        if 'q3c_' in query:
            raise queryClientError("q3c functionality is not part of the ADQL specification")
        if 'healpix_' in query:
            raise queryClientError("healpix functionality is not part of the ADQL specification")
    elif sql is not None and sql != '':
        query = quote_plus(sql)
        dburl = '%s/query?sql=%s&ofmt=%s&out=%s&async=%s' % (
            DEF_SERVICE_URL, query, fmt, out, async)
    else:
        raise queryClientError("No query specified")

    if PROFILE != "default":
        dburl += "&profile=%s" % PROFILE

    r = requests.get(dburl, headers=headers)
    
    if r.status_code != 200:
        raise queryClientError(r.text)

    if (out is not None and out != '') and not async:
        if out[:7] == 'file://':
            out = out[7:]
        if ':' not in out or out[:out.index(':')] not in ['vos', 'mydb']:
            file = open(out, 'wb', 0)
            file.write(r.content)
            file.close()
    else:
        return r.content.decode('utf-8')

    return "OK"


# SIAQUERY -- Send a SIA query to the query manager service
#
def siaquery(token, input=None, out=None, search=0.5):
    """Send a SIA (Simple Image Access) query to the query manager service
    """

    headers = {'X-DL-AuthToken': token}
    user, uid, gid, hash = token.strip().split('.', 3)

    shortname = '%s_%s' % (uid, input[input.rfind('/') + 1:])
    if input[:input.find(':')] not in ['vos', 'mydb']:
        # Need to set this from config?
        target = 'vos://datalab.noao.edu!vospace/siawork/%s' % shortname
        r = requests.get(SM_SERVICE_URL + "/put?name=%s" %
                         target, headers={'X-DL-AuthToken': token})
        file = open(input).read()
                     
        headers2 = {'Content-type': 'application/octet-stream', 
                    'X-DL-AuthToken': token}
        requests.put(r.content, data=file, headers=headers2)

    dburl = '%s/sia?in=%s&radius=%s&out=%s' % (
        DEF_SERVICE_URL, shortname, search, out)
    r = requests.get(dburl, headers=headers)

    if out is not None:
        if out[:out.index(':')] not in ['vos', 'mydb']:
            file = open(out, 'wb')
            file.write(r.content)
            file.close()

    else:
        return r.content.decode('utf-8')


# STATUS -- Get the status of an asynchronous query
#
def status(token, jobId=None):
    """Get the status of an asynchronous query.

    Use the authentication token and the jobId of a previously issued
    asynchronous query to check the query's current status. 

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`dl.auth.login()`)

    jobId : str
        The jobId returned when issuing an asynchronous query via
        :func:`dl.queryClient.query()` with ``async=True``.

    Returns
    -------
    status : str

        Either 'QUEUED' or 'EXECUTING' or 'COMPLETED'. If the token &
        jobId combination does not correspond to an actual job, then a
        HTML-formatted error message is returned. If there is a
        problem with the backend, the returned value can be 'ERROR'.

        When status is 'COMPLETED', you can retrieve the results of
        the query via :func:`dl.queryClient.results()`

    Example
    -------
    .. code-block:: python

        import time
        query = 'select ra,dec from gaia_dr1.gaia_source limit 200000'
        jobId = queryClient.query(token, adql = query, fmt = 'csv', async=True)
        while True:
            status = queryClient.status(token, jobId)
            print "time index =", time.localtime()[5], "   status =", status
            if status == 'COMPLETED':
                break
            time.sleep(1)

    This prints

    .. code::

        time index = 16    status = EXECUTING
        time index = 17    status = EXECUTING
        time index = 18    status = COMPLETED

    """

    headers = {'Content-Type': 'text/ascii',
               'X-DL-AuthToken': token}  # application/x-sql
    dburl = '%s/status?jobid=%s' % (DEF_SERVICE_URL, jobId)
    r = requests.get(dburl, headers=headers)
    return r.content.decode('utf-8')


# RESULTS -- Get the results of an asynchronous query
#
def results(token, jobId=None):
    """Retrieve the results of an asynchronous query, once completed.

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`dl.auth.login()`)

    jobId : str
        The jobId returned when issuing an asynchronous query via
        :func:`dl.queryClient.query()` with ``async=True``.

    Returns
    -------

    Example
    -------

    .. code-block:: python

        # issue a async query (here a tiny one, but nonetheless async, just for this example)
        query = 'select ra,dec from gaia_dr1.gaia_source limit 3'
        jobId = queryClient.query(token, adql = query, fmt = 'csv', async=True)

        # wait a bit... then check status and retrieve results
        time.sleep(4)
        if queryClient.status(token, jobId) == 'COMPLETED':
            results = queryClient.results(token,jobId)
            print type(results)
            print results

    This prints

    .. code::

        <type 'str'>
        ra,dec
        301.37502633933002,44.4946851014515588
        301.371102372343785,44.4953207577355698
        301.385106974224186,44.4963443903961604
    """

    headers = {'Content-Type': 'text/ascii',
               'X-DL-AuthToken': token}  # application/x-sql
    dburl = '%s/results?jobid=%s' % (DEF_SERVICE_URL, jobId)
    if PROFILE != "default":
        dburl += "&profile=%s" % PROFILE
    r = requests.get(dburl, headers=headers)
    return r.content.decode('utf-8')


# SET_TIMEOUT_REQUEST -- Set the requested sync query timeout value (in seconds).
#
def set_timeout_request(nsec):
    """ Set the requested sync query timeout value (in seconds).

    Parameters
    ----------
    nsec : int
        The number of seconds requested before a sync query timeout occurs.
        The service may cap this as a server defined maximum.

    Returns
    -------

    Example
    -------

    .. code-block:: python

        # set the sync query timeout request to 30 seconds
        queryClient.set_timeout_request(30)

    """
    global TIMEOUT_REQUEST
    TIMEOUT_REQUEST = nsec


# GET_TIMEOUT_REQUEST -- Get the current sync query timeout value.
#
def get_timeout_request():
    """ Get the current sync query timeout value.

    Parameters
    ----------
        None

    Returns
    -------
        Current sync query timeout value.

    Example
    -------

    .. code-block:: python

        # get the current timeout value
        print (queryClient.get_timeout_request())

    """
    global TIMEOUT_REQUEST
    return TIMEOUT_REQUEST


# SET_SVC_URL -- Set the service url to use
#
def set_svc_url(svc_url):
    """Set the query manager service URL.

    Parameters
    ----------
    svc_url : str
        The service URL of the query manager to use 

    Returns
    -------

    Example
    -------

    .. code-block:: python

        # set the service url

        url = "http://dldemo.sdm.noao.edu:7002"
        queryClient.set_scv_url(url)

    """
    global DEF_SERVICE_URL
    DEF_SERVICE_URL = svc_url



# GET_SVC_URL -- Get the service url to use
#
def get_svc_url():
    """Get the query manager service URL.

    Parameters
    ----------
        None

    Returns
    -------
        Current Query Manager service URL

    Example
    -------

    .. code-block:: python

        # get the service url
        print (queryClient.get_scv_url())

    """
    global DEF_SERVICE_URL
    return DEF_SERVICE_URL



# LIST_PROFILES -- Get the profiles supported by the query manager service
#
def list_profiles(token, profile=None, format='text'):
    """Retrieve the profiles supported by the query manager service

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
        profiles = queryClient.list_profiles(token)
    """

    headers = {'Content-Type': 'text/ascii',
               'X-DL-AuthToken': token}  # application/x-sql
    dburl = '%s/profiles?' % DEF_SERVICE_URL
    if profile != None and profile != 'None' and profile != '':
        dburl += "profile=%s&" % profile
    dburl += "format=%s" % format

    r = requests.get(dburl, headers=headers)
    profiles = r.content.decode('utf-8')
    if '{' in profiles:
        #profiles = json.load(StringIO(profiles))
        profiles = json.loads(profiles)
    return profiles


# SET_PROFILES -- Set the profile to be used
#
def set_profile(profile):
    """Set the profile

    Parameters
    ----------
    profile : str
        The name of the profile to use. The list of available ones can be retrieved from the service (see function :func:`queryClient.list_profiles()`)

    Returns
    -------

    Example
    -------

    .. code-block:: python

        # set the profile
        queryClient.set_profile("default")
    """

    global PROFILE
    PROFILE = profile


# GET_PROFILES -- Set the profile to be used
#
def get_profile(profile):
    """Get the profile

    Parameters
    ----------

    Returns
    -------
    profile : str
        The name of the current profile used with the query manager service


    Example
    -------

    .. code-block:: python

        # get the profile
        queryClient.get_profile()
    """

    return PROFILE


# LIST -- List the tables in the user's MyDB
#
def list(token, table=''):
    """ List the tables in the user's MyDB

    Parameters
    ----------
    table: str
        The specific table to list (returns the schema)

    Returns
    -------
    listing : str
        The list of tables in the user's MyDB or the schema of a specific table

    Example
    -------

    .. code-block:: python

        # List the tables
        queryClient.list()
    """

    headers = {'Content-Type': 'text/ascii',
               'X-DL-AuthToken': token}  # application/x-sql
    dburl = '%s/list?table=%s' % (DEF_SERVICE_URL, table)
    r = requests.get(dburl, headers=headers)
    return r.content.decode('utf-8')


# SCHEMA -- Return information about a data service schema value.
#
def schema(value, format, profile):
    """ 
        Return information about a data service schema value.

        Parameters
        ----------
        value : str
        format : str
        profile : str
            The name of the profile to use. The list of available ones can be
            retrieved from the service (see function :func:`queryClient.list_profiles()`)

    Returns
    -------

    Example
    -------

    .. code-block:: python

        # set the profile
        queryClient.schema("usno.a2.raj2000","text","default")
    """

    url = '%s/schema?value=%s&format=%s&profile=%s' % \
            (DEF_SERVICE_URL, (value), str(format), str(profile))
    r = requests.get(url)
    return r.content.decode('utf-8')


# REMOVE -- Drop the specified table from the user's MyDB
#
def drop(token, table=''):
    """ Drop the specified table from the user's MyDB

    Parameters
    ----------
    table: str
        The specific table to drop

    Returns
    -------

    Example
    -------

    .. code-block:: python

        # List the tables
        queryClient.drop('foo1')
    """

    headers = {'Content-Type': 'text/ascii',
               'X-DL-AuthToken': token}  # application/x-sql
    dburl = '%s/delete?table=%s' % (DEF_SERVICE_URL, table)
    r = requests.get(dburl, headers=headers)
    return r.content.decode('utf-8')
