#!/usr/bin/env python
#
# QUERYCLIENT -- Client routines for the Data Lab Query Manager Service


from __future__ import print_function

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>, Matthew Graham <graham@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = 'v2.18.5'


'''
    Client methods for the DataLab Query Manager Service.


Query Manager Client Interface
------------------------------
               isAlive  (svc_url=DEF_SERVICE_URL, timeout=2)
           set_svc_url  (svc_url)
           get_svc_url  ()
           set_profile  (profile)
           get_profile  ()
         list_profiles  (optval, token=None, profile=None, format='text')
         list_profiles  (token=None, profile=None, format='text')

   set_timeout_request  (nsec)
   get_timeout_request  ()
                schema  (value, format='text', profile=None)
                schema  (value='', format='text', profile=None)
              services  (name=None, svc_type=None, format=None,
                         profile='default')

                 query  (token, query, adql=None, sql=None, fmt='csv', out=None,
                         async_=False, profile='default', **kw)
                 query  (optval, adql=None, sql=None, fmt='csv', out=None,
                         async_=False, profile='default', **kw)
                 query  (token=None, adql=None, sql=None, fmt='csv', out=None,
                         async_=False, profile='default', **kw)
                status  (token, jobId)
                status  (optval, jobId=None)
                status  (token=None, jobId=None)
               results  (token, jobId, delete=True)
               results  (optval, jobId=None, delete=True)
               results  (token=None, jobId=None, delete=True)
                 error  (token, jobId)
                 error  (optval, jobId=None)
                 error  (token=None, jobId=None)
                 abort  (token, jobId)
                 abort  (optval, jobId=None)
                 abort  (token=None, jobId=None)

             mydb_list  (optval, table=None)
             mydb_list  (table=None, token=None)
           mydb_create  (token, table, schema, **kw)
           mydb_create  (table, schema, token=None, **kw)
           mydb_insert  (token, table, data, **kw)
           mydb_insert  (table, data, token=None, **kw)
           mydb_import  (token, table, data, **kw)
           mydb_import  (table, data, token=None, **kw)
         mydb_truncate  (token, table)
         mydb_truncate  (table, token=None)
            mydb_index  (token, table, column)
            mydb_index  (table, column, token=None)
             mydb_drop  (token, table)
             mydb_drop  (table, token=None)
           mydb_rename  (token, source, target)
           mydb_rename  (source, target, token=None)
             mydb_copy  (token, source, target)
             mydb_copy  (source, target, token=None)

                  list  (token, table)			# DEPRECATED
                  list  (optval, table=None)		# DEPRECATED
                  list  (table=None, token=None)	# DEPRECATED
                  drop  (token, table)			# DEPRECATED
                  drop  (optval, table=None)		# DEPRECATED
                  drop  (table=None, token=None)	# DEPRECATED


Import via

.. code-block:: python

    from dl import queryClient
'''

import requests
try:
    from urllib import quote_plus               # Python 2
except ImportError:
    from urllib.parse import quote_plus         # Python 3
try:
    from cStringIO import StringIO
except:
    from io import StringIO			# Python 2/3 compatible
import socket
import json
import time
import os
import sys
import collections
import ast, csv
import pandas
from tempfile import NamedTemporaryFile

from dl import storeClient
from dl.helpers.utils import convert
if os.path.isfile('./Util.py'):			# use local dev copy
    from Util import multimethod
    from Util import def_token
else:						# use distribution copy
    from dl.Util import multimethod
    from dl.Util import def_token

is_py3 = sys.version_info.major == 3



# ####################################
#  Query Manager Configuration
# ####################################


# The URL of the QueryManager service to contact.  This may be changed by
# passing a new URL into the set_svc_url() method before beginning.

DEF_SERVICE_ROOT = 'https://datalab.noao.edu'
DAL_SERVICE_URL = 'https://datalab.noao.edu' 	# The base DAL service URL

# Allow the service URL for dev/test systems to override the default.
THIS_HOST = socket.gethostname()			# host name
sock = socket.socket(type=socket.SOCK_DGRAM)     # host IP address
sock.connect(('8.8.8.8', 1))        # Example IP address, see RFC 5737
THIS_IP, _ = sock.getsockname()


if THIS_HOST[:5] == 'dldev':
    DEF_SERVICE_ROOT = 'http://dldev.datalab.noao.edu'
elif THIS_HOST[:6] == 'dltest':
    DEF_SERVICE_ROOT = 'http://dltest.datalab.noao.edu'

DEF_SERVICE_URL = DEF_SERVICE_ROOT + '/query'
SM_SERVICE_URL  = DEF_SERVICE_ROOT + '/storage'

# The requested query 'profile'.  A profile refers to the specific
# machines and services used by the QueryManager on the server.
DEF_PROFILE 	= 'default'

# Use a /tmp/QM_DEBUG file as a way to turn on debugging in the client code.
DEBUG 		= os.path.isfile('/tmp/QM_DEBUG')

# Default sync query timeout default (300sec)
TIMEOUT_REQUEST = 300



# ####################################################################
#  Query Client error class
# ####################################################################

class queryClientError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


# ####################################################################
#  Module Functions
# ####################################################################

# --------------------------------------------------------------------
# ISALIVE -- Ping the Query Manager service to see if it responds.
#
def isAlive(svc_url=DEF_SERVICE_URL, timeout=5):
    return qc_client.isAlive(svc_url=svc_url, timeout=timeout)


# --------------------------------------------------------------------
# SET_SVC_URL -- Set the Query Manager ServiceURL to call.
#
def set_svc_url(svc_url):
    return qc_client.set_svc_url(svc_url)

# --------------------------------------------------------------------
# GET_SVC_URL -- Get the Query Manager ServiceURL being called.
#
def get_svc_url():
    return qc_client.get_svc_url()

# --------------------------------------------------------------------
# SET_PROFILE -- Set the Query Manager service profile to be used.
#
def set_profile(profile):
    return qc_client.set_profile(profile)

# --------------------------------------------------------------------
# GET_PROFILE -- Get the Query Manager service profile being used.
#
def get_profile():
    return qc_client.get_profile()


# -----------------------------
#  Utility Functions
# -----------------------------

# --------------------------------------------------------------------
# LIST_PROFILES -- List the available service profiles.
#
@multimethod('qc',1,False)
def list_profiles(optval, token=None, profile=None, format='text'):
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return qc_client._list_profiles (token=def_token(optval),
                                         profile=profile, format=format)
    else:
        # optval looks like a profile name
        return qc_client._list_profiles (token=def_token(token), profile=optval,
                                         format=format)

@multimethod('qc',0,False)
def list_profiles(token=None, profile=None, format='text'):
    '''Retrieve the profiles supported by the query manager service.

    Usage:
        list_profiles (token=None, profile=None, format='text')

    MultiMethod Usage:  
    ------------------
            queryClient.list_profiles (token)
            queryClient.list_profiles ()

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    profile : str
        A specific profile configuration to list.  If None, a list of
        profiles available to the given auth token is returned.

    format : str
        Result format: One of 'text' or 'json'

    Returns
    -------
    profiles : list/dict
        A list of the names of the supported profiles or a dictionary of
        the specific profile

    Example
    -------
    .. code-block:: python

        profiles = queryClient.list_profiles()
        profiles = queryClient.list_profiles(token)
    '''
    return qc_client._list_profiles (token=def_token(token), profile=profile,
                                 format=format)

# --------------------------------------------------------------------
# SET_TIMEOUT_REQUEST -- Set the Synchronous query timeout value (in sec).
#
def set_timeout_request(nsec):
    return qc_client.set_timeout_request (nsec)

# --------------------------------------------------------------------
# GET_TIMEOUT_REQUEST -- Get the Synchronous query timeout value (in sec).
#
def get_timeout_request():
    return qc_client.get_timeout_request ()

# --------------------------------------------------------------------
# SCHEMA -- Return information about a data service schema value.
#
@multimethod('qc',1,False)
def schema(value, format='text', profile=None):
    return qc_client._schema (value=value, format=format, profile=profile)

@multimethod('qc',0,False)
def schema(value='', format='text', profile=None):
    '''Return information about a data service schema.

    Usage:
        schema (value='', format='text', profile=None)

    Parameters
    ----------
    value : str
        Schema object to return: Of the form <schema>[.<table>[.<column]]

    profile : str
        The name of the service profile to use. The list of available
        profiles can be retrieved from the service (see function
        :func:`queryClient.list_profiles()`)

    format : str
        Result format:  One of 'text' or 'json'  (NOT CURRENTLY USED)

    Returns
    -------

    Example
    -------
    .. code-block:: python

        # List the available schema
        queryClient.schema("", "text", "default")

        # List the tables in the USNO schema
        queryClient.schema("usno", "text", "default")

        # List the columns of the USNO-A2 table
        queryClient.schema("usno.a2", "text", "default")

        # List the attributes of the USNO-A2 'raj2000' column
        queryClient.schema("usno.a2.raj2000", "text", "default")
    '''
    return qc_client._schema (value=value, format=format, profile=profile)

# --------------------------------------------------------------------
# SERVICES -- List public storage services
#
def services(name=None, svc_type=None, mode='list', profile='default'):
    '''Search or list available data services.

    Usage:
        services (name=None, svc_type=None, mode='list', profile='default')

    Parameters
    ----------
    name : str
        Schema object to return: Of the form <schema>[.<table>[.<column]]

    svc_type : str
        Limit results to specified service type.  Supported options are
	'tap', 'sia', 'scs', or 'vos'.

    mode : str
	Query mode:  

    profile : str
        The name of the service profile to use. The list of available
        profiles can be retrieved from the service (see function
        :func:`queryClient.list_profiles()`)

    Returns
    -------
	If mode is 'list' then a human-readable list of matching services is
	returned.  If mode is 'resolve' then a JSON string of matching services
	is returned un the form "{<svc_name> : <svc_url>, ....}"

    Example
    -------
    .. code-block:: python

        # List the available SIA services
        queryClient.services(svc_type="sia")

        # List the available USNO services, note the '%' matching metacharacter
        queryClient.services(name="usno%")

        # Get the serviceURL of the USNO-A2 table
        queryClient.services(name="usno/a2", mode="resolve")
    '''
    return qc_client.services (name=name, svc_type=svc_type, mode=mode,
                               profile=profile)


# -----------------------------
#  Query Functions
# -----------------------------

# --------------------------------------------------------------------
# QUERY -- Send a query to the Query Manager service
#
@multimethod('qc',2,False)
def query(token, query, adql=None, sql=None, fmt='csv', out=None,
           async_=False, profile='default', **kw):
    return qc_client._query (token=def_token(token), adql=adql, sql=query,
                          fmt=fmt, out=out, async_=async_,
                          profile=profile, **kw)

@multimethod('qc',1,False)
def query(optval, adql=None, sql=None, fmt='csv', out=None, async_=False,
           token=None, profile='default', **kw):
    if optval is not None and optval.lower()[:6] == 'select':
        # optval looks like a query string
        return qc_client._query (token=def_token(None), adql=adql, sql=optval,
                              fmt=fmt, out=out, async_=async_,
                              profile=profile, **kw)
    else:
        # optval is (probably) a token
        return qc_client._query (token=def_token(optval), adql=adql, sql=sql,
                              fmt=fmt, out=out, async_=async_,
                              profile=profile, **kw)

@multimethod('qc',0,False)
def query(token=None, adql=None, sql=None, fmt='csv', out=None, async_=False,
           profile='default', **kw):
    '''Send an SQL or ADQL query to the database or TAP service.

    Usage:
        query (token=None, adql=None, sql=None, fmt='csv', out=None,
               async_=False, profile='default', **kw):

    MultiMethod Usage:
    ------------------
        queryClient.query (token, query, <args>)
        queryClient.query (token | query, <args>)

    Parameters
    ----------
    token : str
        Secure token obtained via :func:`authClient.login()`

    adql : str or None
        ADQL query string that will be passed to the DB query manager, e.g.

        .. code-block:: python

            adql='select top 3 ra,dec from gaia_dr1.gaia_source'

    sql : str or None
        SQL query string that will be passed to the DB query manager, e.g.

        .. code-block:: python

            sql='select ra,dec from gaia_dr1.gaia_source limit 3'

    fmt : str
        Format of result to be returned by the query. Permitted values are:

          * 'csv'          The returned result is a comma-separated string
                           that looks like a csv file (newlines at the end
                           of every row) [DEFAULT]
          * 'ascii'        Same, but the column separator is a tab \t
          * 'array'        Returns a NumPy array
          * 'pandas'       Returns a Pandas DataFrame
          * 'structarray'  Numpy structured array (aka 'record array')
          * 'table'        Returns an Astropy Table object

            The following formats may be used when saving a file to
            virtual storage on the server:

          * 'fits'         FITS binary
          * 'hdf5'         HDF5 file                (NOT YET IMPLEMENTED)
          * 'votable'      An XML-formatted VOTable

    out : str or None
        The output filename to create on the local machine, the URI of a
        VOSpace or MyDB resource to create, or ``None`` if the result is
        to be returned directly to the caller.

    async_ : bool
        If ``True``, the query is Asynchronous, i.e. a job is submitted to
        the DB, and a jobID token is returned the caller. The jobID must
        be then used to check the query's status and to retrieve the result
        (when the job status is ``COMPLETED``) or the error message (when
        the job status is ``ERROR``). Default is ``False``, i.e. the task
        runs a Synchroneous query.

        ``async_`` replaces the previous ``async`` parameter, because ``async``
        was promoted to a keyword in Python 3.7. Users of Python versions
        prior to 3.7 can continue to use the ``async`` keyword.

    profile : str or None
        The Query Manager profile to use for this call.  If ``None`` then
        the default profile is used.  Available profiles may be listed
        using the :func:`queryClient.list_profiles()`

    **kw : dict
        Optional keyword arguments.  Supported keywords currently include:

           wait = False
               Wait for asynchronous queries to complete? If enabled,
               the query() method will submit the job in async mode
               and then poll for results internally before returning.
               the default is to return the job ID immediately and let
               the client poll for job status and return results.

           timeout = 300
               Requested timeout (in seconds) for a query. For a Sync
               query, this value sets a session timeout request in the
               database that will abort the query at the specified time.
               A maximum value of 600 seconds is permitted.  If the
               ``wait`` option is enabled for an ASync query, this is the
               maximum time the query will be allowed to run before an
               abort() is issued on the job.  The maximum timeout for
               an ASync job is 24-hrs (86400 sec).

           poll = 1
               ASync job polling time in seconds.

           verbose = False
               Print verbose messages during ASync job.

    Returns
    -------
    result : str
        If ``async=False``, the return value is the result of the
        query as a formatted string (see ``fmt``). Otherwise the
        result string is a job token, with which later the
        Asynchroneaous query's status can be checked
        (:func:`queryClient.status()`), and the result retrieved (see
        :func:`queryClient.result()`.

    Example
    -------
    Get security token first, see :func:`authClient.login()`. Then:

    .. code-block:: python

        query = 'select ra,dec from gaia_dr1.gaia_source limit 3'
        response = queryClient.query(token, adql=query, fmt='csv')
        print response

    This prints

    .. code::

          ra,dec
          315.002571989537842,35.2662974820284489
          315.00408275885701,35.2665448169895797
          314.996334457679438,35.2673478725552698
    '''
    return qc_client._query (token=def_token(token), adql=adql, sql=sql, 
                             fmt=fmt, out=out, async_=async_, profile=profile,
                             **kw)



# --------------------------------------------------------------------
# STATUS -- Get the status of an Asynchronous query
#
@multimethod('qc',2,False)
def status(token, jobId):
    return qc_client._status (token=def_token(token), jobId=jobId)

@multimethod('qc',1,False)
def status(optval, jobId=None):
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return qc_client._status (token=def_token(optval), jobId=jobId)
    else:
        # optval is probably a jobId
        return qc_client._status (token=def_token(None), jobId=optval)

@multimethod('qc',0,False)
def status(token=None, jobId=None):
    '''Get the status of an asynchronous query.

    Usage:
        status (token=None, jobId=None)

    MultiMethod Usage:
    ------------------
        queryClient.status (jobId)
        queryClient.status (token, jobId)
        queryClient.status (token, jobId=<id>)
        queryClient.status (jobId=<str>)

    Use the authentication token and the jobId of a previously issued
    asynchronous query to check the query's current status.

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    jobId : str
        The jobId returned when issuing an asynchronous query via
        :func:`queryClient.query()` with ``async=True``.

    Returns
    -------
    status : str

        Either 'QUEUED' or 'EXECUTING' or 'COMPLETED'. If the token &
        jobId combination does not correspond to an actual job, then a
        HTML-formatted error message is returned. If there is a
        problem with the backend, the returned value can be 'ERROR'.

        When status is 'COMPLETED', you can retrieve the results of
        the query via :func:`queryClient.results()`

    Example
    -------
    .. code-block:: python

        import time
        query = 'select ra,dec from gaia_dr1.gaia_source limit 200000'
        jobId = queryClient.query(token, adql=query, fmt='csv', async=True)
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

    '''
    return qc_client._status (token=def_token(token), jobId=jobId)


# --------------------------------------------------------------------
# RESULTS -- Get the results of an Asynchronous query
#
@multimethod('qc',2,False)
def results(token, jobId, delete=True):
    return qc_client._results (token=def_token(token), jobId=jobId, delete=True)

@multimethod('qc',1,False)
def results(optval, jobId=None, delete=True):
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return qc_client._results (token=def_token(optval), jobId=jobId,
                                delete=delete)
    else:
        # optval is probably a jobId
        return qc_client._results (token=def_token(None), jobId=optval,
                                delete=delete)

@multimethod('qc',0,False)
def results(token=None, jobId=None, delete=True):
    '''Retrieve the results of an asynchronous query, once completed.

    Usage:
        results (token=None, jobId=None, delete=True)

    MultiMethod Usage:
    ------------------
        queryClient.results (jobId)
        queryClient.results (token, jobId)
        queryClient.results (token, jobId=<id>)
        queryClient.results (jobId=<str>)

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    jobId : str
        The jobId returned when issuing an asynchronous query via
        :func:`queryClient.query()` with ``async=True``.

    Returns
    -------
    results : str

    Example
    -------
    .. code-block:: python

        # issue an async query (here a tiny one just for this example)
        query = 'select ra,dec from gaia_dr1.gaia_source limit 3'
        jobId = queryClient.query(token, adql=query, fmt='csv', async=True)

        # ensure job completes...then check status and retrieve results
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
    '''
    return qc_client._results (token=def_token(token), jobId=jobId, delete=True)


# --------------------------------------------------------------------
# ERROR -- Get the error message of a failed Asynchronous query
#
@multimethod('qc',2,False)
def error(token, jobId):
    return qc_client._error (token=def_token(token), jobId=jobId)

@multimethod('qc',1,False)
def error(optval, jobId=None):
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return qc_client._error (token=def_token(optval), jobId=jobId)
    else:
        # optval is probably a jobId
        return qc_client._error (token=def_token(None), jobId=optval)

@multimethod('qc',0,False)
def error(token=None, jobId=None):
    '''Retrieve the error of an asynchronous query, once completed.

    Usage:
        error (token=None, jobId=None)

    MultiMethod Usage:
    ------------------
        queryClient.error (jobId)
        queryClient.error (token, jobId)
        queryClient.error (token, jobId=<id>)
        queryClient.error (jobId=<str>)

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    jobId : str
        The jobId returned when issuing an asynchronous query via
        :func:`queryClient.query()` with ``async=True``.

    Returns
    -------
    error : str

    Example
    -------
    .. code-block:: python

        # issue an async query (here a tiny one just for this example)
        query = 'select ra,dec from gaia_dr1.gaia_source limit 3'
        jobId = queryClient.query(token, adql=query, fmt='csv', async=True)

        # ensure job completes...then check status and retrieve error
        time.sleep(4)
        if queryClient.status(token, jobId) == 'ERROR':
            error = queryClient.error(token,jobId)
            print type(error)
            print error

    This prints

    .. code::

        <type 'str'>
        ra,dec
        301.37502633933002,44.4946851014515588
        301.371102372343785,44.4953207577355698
        301.385106974224186,44.4963443903961604
    '''
    return qc_client._error (token=def_token(token), jobId=jobId)


# --------------------------------------------------------------------
# ABORT -- Abort the specified Asynchronous job.
#
@multimethod('qc',2,False)
def abort(token, jobId):
    return qc_client._abort (token=def_token(token), jobId=jobId)

@multimethod('qc',1,False)
def abort(optval, jobId=None):
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return qc_client._abort (token=def_token(optval), jobId=jobId)
    else:
        # optval is probably a jobId
        return qc_client._abort (token=def_token(None), jobId=optval)

@multimethod('qc',0,False)
def abort(token=None, jobId=None):
    '''Abort the specified asynchronous job.

    Usage:
        abort (token=None, jobId=None)

    MultiMethod Usage:
    ------------------
        queryClient.abort (token, jobId)
        queryClient.abort (jobId)
        queryClient.abort (token, jobId=<id>)
        queryClient.abort (jobId=<str>)

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    jobId : str
        The jobId to abort.

    Returns
    -------
    results : str

    Example
    -------
    .. code-block:: python

        # issue an async query (here a tiny one just for this example)
        query = 'select ra,dec from gaia_dr1.gaia_source limit 3'
        jobId = queryClient.query(token, adql=query, fmt='csv', async=True)

        # ensure job completes...then check status and retrieve results
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
    '''
    return qc_client._abort (token=def_token(token), jobId=jobId)



# -----------------------------
#  MyDB Functions (deprecated)
# -----------------------------

# --------------------------------------------------------------------
# LIST -- List the tables or table schema in a user's MyDB.
#
@multimethod('qc',2,False)
def list(token, table):
    return qc_client.mydb_list (token=def_token(token), table=table)

@multimethod('qc',1,False)
def list(optval, table=None):
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return qc_client.mydb_list (token=def_token(optval), table=table)
    else:
        # optval is likely a table
        return qc_client.mydb_list (token=def_token(None), table=optval)

@multimethod('qc',0,False)
def list(table=None, token=None):
    '''List the tables or table schema in the user's MyDB.

    Usage:
        list (table=None, token=None)

    MultiMethod Usage:
    ------------------
        queryClient.list (token, table)
        queryClient.list (table)
        queryClient.list (token, table=<id>)
        queryClient.list ()

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    table: str
        The specific table to list (returns the table schema), or
        an empty string to return a list of the names of all tables.

    Returns
    -------
    listing : str
        The list of tables in the user's MyDB or the schema of the
        named table

    Example
    -------
    .. code-block:: python

        # List the tables
        queryClient.list()
    '''
    return qc_client.mydb_list (token=def_token(token), table=table)


# --------------------------------------------------------------------
# DROP -- Drop the named table from a user's MyDB.
#
@multimethod('qc',2,False)
def drop(token, table):
    return qc_client.mydb_drop (token=def_token(token), table=table)

@multimethod('qc',1,False)
def drop(optval, table=None):
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return qc_client.mydb_drop (token=def_token(optval), table=table)
    else:
        # optval is likely a table
        return qc_client.mydb_drop (token=def_token(None), table=optval)

@multimethod('qc',0,False)
def drop(table=None, token=None):
    '''Drop the specified table from the user's MyDB

    Usage:
        drop (table=None, token=None)

    MultiMethod Usage:
    ------------------
        queryClient.drop (token, table)
        queryClient.drop (table)
        queryClient.drop (token, table=<id>)
        queryClient.drop (table=<str>)

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    table: str
        The specific table to drop

    Returns
    -------

    Example
    -------
    .. code-block:: python

        # List the tables
        queryClient.drop('foo1')
    '''
    return qc_client.mydb_drop (token=def_token(token), table=table)



# -----------------------------
#  MyDB Functions (New API)
# -----------------------------

# --------------------------------------------------------------------
# MYDB_LIST -- List the tables or table schema in a user's MyDB.
#
@multimethod('qc',1,False)
def mydb_list(optval, table=None, index=False):
    if optval is not None and len(optval.split('.')) >= 4:
        # optval looks like a token
        return qc_client._mydb_list (token=def_token(optval), table=table,
                                     index=index)
    else:
        # optval is likely a table name
        return qc_client._mydb_list (token=def_token(None), table=optval,
                                     index=index)

@multimethod('qc',0,False)
def mydb_list(table=None, token=None, index=False):
    '''List the tables or table schema in the user's MyDB.

    Usage:
        mydb_list (table=None, token=None)

    MultiMethod Usage:
    ------------------
        queryClient.mydb_list (table)
        queryClient.mydb_list (token, table=<str>)
        queryClient.mydb_list (table=<str>)

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    table: str
        The specific table to list (returns the table schema), or
        an empty string to return a list of the names of all tables.

    Returns
    -------
    listing : str
        The list of tables in the user's MyDB or the schema of the
        named table

    Example
    -------
    .. code-block:: python

        # List the tables
        queryClient.mydb_list()
    '''
    return qc_client._mydb_list (token=def_token(token), table=table,
                                 index=index)


# --------------------------------------------------------------------
# MYDB_CREATE -- Create a table in the user's MyDB from a local file
# or python data object.
#
@multimethod('qc',3,False)
def mydb_create(token, table, schema, **kw):
    return qc_client._mydb_create (token=def_token(None), table=table,
                              schema=schema, **kw)

@multimethod('qc',2,False)
def mydb_create(table, schema, token=None, **kw):
    '''Create a table in the user's MyDB

    Usage:
        mydb_create (table, schema, token=None, **kw)

    MultiMethod Usage:
    ------------------
        queryClient.mydb_create (token, table, <schema_dict>)
        queryClient.mydb_create (table, <schema_dict>)

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    table: str
        The name of the table to create

    schema: str or dict
        The schema is CSV text containing the name of the column and
        it's PostgreSQL data type.  If set as a 'str' type it is either
        a CSV string, or the name of a file containing the CSV.  If passed
        as a 'dict' type, it is a dictionary object where keys are the
        column names and values are the data types.

    drop: bool   (optional)
        Drop any existing table of the same name before creating new one.

    Returns
    -------

    Example
    -------
    .. code-block:: python

        # Create table in MyDB named 'foo' with columns defined by
        # the file 'schema.txt'.  The schema file contains:
        #
        #     id,text
        #     ra,double precision
        #     dec,double precision
        #
        queryClient.mydb_create ('foo', 'schema.txt')
    '''
    return qc_client._mydb_create (token=def_token(None), table=table,
                              schema=schema, **kw)

#--------------------------------------------------------------------
# MYDB_INSERT -- Insert data into a table in the user's MyDB from a local
# file or python data object.
#
@multimethod('qc',3,False)
def mydb_insert(token, table, data, **kw):
    return qc_client._mydb_insert (token=def_token(token), table=table,
                                   data=data, **kw)

@multimethod('qc',2,False)
def mydb_insert(table, data, token=None, **kw):
    '''Insert data into a table in the user's MyDB

    Usage:
        mydb_insert (table, data, token=None, **kw)

    MultiMethod Usage:
    ------------------
        queryClient.mydb_insert (token, table, <filename>)
        queryClient.mydb_insert (token, table, <data_object>)
        queryClient.mydb_insert (table, <filename>)
        queryClient.mydb_insert (table, <data_object>)

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    table: str
        The name of the table to append

    data: str or data object
        The schema is CSV text containing the name of the column and
        it's PostgreSQL data type.  If set as a 'str' type it is either
        a CSV string, or the name of a file containing the CSV data. If
        passed as a tabular data object, it is converted to CSV and sent
        to the service.

    csv_header: bool        [OPTIONAL]
        If True, then the CSV data object contains a CSV header line, i.e.
        the first line is a row of column names.  Otherwise, no column
        names are assumed and the column order must match the table schema.

    Returns
    -------

    Example
    -------
    .. code-block:: python

        # Insert data into a MyDB table named 'foo'.
        queryClient.mydb_insert ('foo', 'data.csv')
    '''
    return qc_client._mydb_insert (token=def_token(token), table=table,
                                   data=data, **kw)


# --------------------------------------------------------------------
# MYDB_IMPORT -- Import a file or Python object to a MyDB table.
#
@multimethod('qc',3,False)
def mydb_import(token, table, data, **kw):
    try:
        result = qc_client._mydb_import (token=def_token(token), table=table,
                                         data=data, **kw)
    except Exception as e:
        return (str(e))

    return result

@multimethod('qc',2,False)
def mydb_import(table, data, token=None, **kw):
    '''Import data into a table in the user's MyDB

    Usage:
        mydb_import (table, data, token=None, **kw)

    MultiMethod Usage:
    ------------------
        queryClient.mydb_import (token, table, data)
        queryClient.mydb_import (table, data)

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    table: str
        The name of the table to be loaded

    data: str or data object
        The data file or python object to be loaded.  The 'data' value
        may be one of the following types:

            filename	    A CSV file of data
            string		    A string containing CSV data
            Pandas DataFrame    A Pandas DataFrame object
                :                   :        :       :

        Additional object types can be added provided the data can be
        converted to a CSV format.

    schema: str	[OPTIONAL]
        If set, this is a filename or string containing a schema for the
        data table to be created.  A schema contains a comma-delimited row
        for each column containing the column name and it's Postgres data
        type.  If not set, the schema is determined automatically from
        the data.

    drop: bool   	[Optional]
        Drop any existing table of the same name before creating new one.

    verbose: bool   [Optional]
        Be verbose about operations.

    Returns
    -------
        schema	A string containing the table schema
        data_obj	The CSV data to be imported (possibly converted)

    Example
    -------
    .. code-block:: python

        # Import data into a MyDB table named 'foo' from file 'data.csv'.
        schema, data = queryClient.mydb_import ('foo', 'data.csv')
    '''
    try:
        result = qc_client._mydb_import (token=def_token(token), table=table,
                                         data=data, **kw)
    except Exception as e:
        return (str(e))

    return result


# --------------------------------------------------------------------
# MYDB_TRUNCATE -- Truncate a table in the user's MyDB.
#
@multimethod('qc',2,False)
def mydb_truncate(token, table):
    return qc_client._mydb_truncate (token=def_token(token), table=table)

@multimethod('qc',1,False)
def mydb_truncate(table, token=None):
    '''Truncate the specified table in the user's MyDB

    Usage:
        mydb_truncate (table, token=None)

    MultiMethod Usage:
    ------------------
        queryClient.mydb_truncate (token, table)
        queryClient.mydb_truncate (table)
        queryClient.mydb_truncate (token, table=<id>)

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    table: str
        The specific table to drop

    Returns
    -------

    Example
    -------
    .. code-block:: python

        # Truncate the table 'foo'
        queryClient.truncate('foo')
    '''
    return qc_client._mydb_truncate (token=def_token(token), table=table)

# --------------------------------------------------------------------
# MYDB_INDEX -- Index a column in a user's MyDB table.
#
@multimethod('qc',3,False)
def mydb_index(token, table, column, q3c=None, cluster=False, async_=False):
    return qc_client._mydb_index(token=def_token(token), table=table,
                                 column=column, q3c=q3c, cluster=cluster,
                                 async_=async_)

@multimethod('qc',2,False)
def mydb_index(table, column, token=None, q3c=None, cluster=False,
               async_=False):
    return qc_client._mydb_index(token=def_token(token), table=table,
                                 column=column, q3c=q3c, cluster=cluster,
                                 async_=async_)

@multimethod('qc',1,False)
def mydb_index(table, column='', token=None, q3c=None, cluster=False,
               async_=False):
    '''Index the specified column in a table in the user's MyDB

    MultiMethod Usage:
    ------------------
        queryClient.mydb_index (table, colunm)
        queryClient.mydb_index (token, table, column)
        queryClient.mydb_index (table, column, token=None)

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    table: str
        The table to be indexed

    column: str
        The column

    q3c: str
        A comma-delimited list of two column names giving the RA and Dec
        positions (decimal degrees) to be used to Q3C index the table.  If
        None, no Q3C index will be computed.

    cluster: bool
        If enabled, data table will be rewritten to cluster on the Q3C index
        for efficiency.  Only used when 'q3c' columns are specified.

    async_: bool
	If enabled, index commands will be submitted asynchronously.

    Returns
    -------
	command status

    Example
    -------
    .. code-block:: python

	# Index the table's "id" column
        queryClient.index('foo1', 'id') 	

	# Index and cluster the table by position
        queryClient.index('foo1', q3c='ra,dec', cluster=True) 	
    '''
    return qc_client._mydb_index(token=def_token(token), table=table,
                                 column=column, q3c=q3c, cluster=cluster,
                                 async_=async_)

# --------------------------------------------------------------------
# MYDB_DROP -- Drop the named table from a user's MyDB.
#
@multimethod('qc',2,False)
def mydb_drop(token, table):
    return qc_client._mydb_drop (token=def_token(token), table=table)

@multimethod('qc',1,False)
def mydb_drop(table, token=None):
    '''Drop the specified table from the user's MyDB

    Usage:
        mydb_drop (table, token=None)

    MultiMethod Usage:
    ------------------
        queryClient.mydb_drop (token, table)
        queryClient.mydb_drop (table)
        queryClient.mydb_drop (token, table=<id>)

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    table: str
        The specific table to drop

    Returns
    -------

    Example
    -------
    .. code-block:: python

        # Drop the 'foo1' table
        queryClient.drop('foo1')
    '''
    return qc_client._mydb_drop (token=def_token(token), table=table)


# --------------------------------------------------------------------
# MYDB_RENAME -- Rename a table in the user's MyDB.
#
@multimethod('qc',3,False)
def mydb_rename(token, source, target):
    return qc_client._mydb_rename (token=def_token(token),
                                source=source, target=target)

@multimethod('qc',2,False)
def mydb_rename(source, target, token=None):
    '''Rename a table in the user's MyDB to a new name

    Usage:
        mydb_rename (source, target, token=None)

    MultiMethod Usage:
    ------------------
        queryClient.mydb_rename (token, source, target)
        queryClient.mydb_rename (source, target)

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    source: str
        The old table name

    target: str
        The new table name

    Returns
    -------

    Example
    -------
    .. code-block:: python

        # Copy table 'foo' to a new table, 'bar'
        queryClient.mydb_rename ('foo', 'bar')
    '''
    return qc_client._mydb_rename (source, target, token=def_token(token))


# --------------------------------------------------------------------
# MYDB_COPY -- Copy a table in the user's MyDB.
#
@multimethod('qc',3,False)
def mydb_copy(token, source, target):
    return qc_client._mydb_copy (token=def_token(token),
                              source=source, target=target)

@multimethod('qc',2,False)
def mydb_copy(source, target, token=None):
    '''Copy a table in the user's MyDB to a new name

    Usage:
        mydb_copy (source, target, token=None)

    MultiMethod Usage:
    ------------------
        queryClient.mydb_copy (token, source, target)
        queryClient.mydb_copy (source, target)

    Parameters
    ----------
    token : str
        Authentication token (see function :func:`authClient.login()`)

    source: str
        The old table name, i.e. the table to be copied

    target: str
        The new table name, i.e. the table to be created

    Returns
    -------

    Example
    -------
    .. code-block:: python

        # Copy table 'foo' to a new table, 'bar'
        queryClient.mydb_copy ('foo', 'bar')
    '''
    return qc_client._mydb_copy (source, target, token=def_token(token))




# ###################################
#  Query Class procedures
# ###################################

class queryClient (object):
    '''
         QUERYCLIENT -- Client-side methods to access the Data Lab
                        Query Manager Service.
    '''
    def __init__(self, profile=DEF_PROFILE, svc_url=DEF_SERVICE_URL):
        '''Initialize the query client. '''

        self.svc_url = svc_url.strip('/')       # QueryMgr service URL
        self.svc_profile = profile  		# QueryMgr service profile

        self.sm_svc_url = SM_SERVICE_URL        # StorageMgr service URL
        self.hostip = THIS_IP
        self.hostname = THIS_HOST
        self.timeout_request = TIMEOUT_REQUEST
        self.async_wait = False

        # Get the $HOME/.datalab directory.
        self.home = '%s/.datalab' % os.path.expanduser('~')

        self.debug = DEBUG                      # interface debug flag


    def isAlive(self, svc_url=None, timeout=5):
        '''Check whether the QueryManager service at the given URL is
           alive and responding.  This is a simple call to the root
           service URL or ping() method.

        Parameters
        ----------
        svc_url : str
            The Query Service URL to ping.
        timeout : int
            Call will assume to have failed if 'timeout' seconds pass.

        Returns
        -------
        result : bool
            True if service responds properly, False otherwise

        Example
        -------
        .. code-block:: python

            if queryClient.isAlive():
                print ("Query Manager is alive")
        '''
        if svc_url is None:
            svc_url = self.svc_url

        try:
            r = requests.get (svc_url, timeout=timeout)
            resp = r.text
            if r.status_code != 200:
                return False
            elif resp is not None and r.text.lower()[:11] != "hello world":
                return False
        except Exception:
            return False

        return True

    def set_svc_url(self, svc_url):
        '''Set the Query Manager service URL.

        Parameters
        ----------
        svc_url : str
            The service URL of the Query Manager to call.

        Returns
        -------
        Nothing

        Example
        -------
        .. code-block:: python

            queryClient.set_svc_url ("http://localhost:7002")
        '''
        self.svc_url = qcToString(svc_url.strip('/'))

    def get_svc_url(self):
        '''Get the currently-used Query Manager serice URL.

        Parameters
        ----------
        None

        Returns
        -------
        service_url : str
            The currently-used Query Service URL.

        Example
        -------
        .. code-block:: python

            print (queryClient.get_svc_url())
        '''
        return qcToString(self.svc_url)

    def set_profile(self, profile):
        '''Set the service profile to be used.

        Parameters
        ----------
        profile : str
            The name of the profile to use. The list of available profiles
            can be retrieved from the service (see function
            func:`queryClient.list_profiles()`)

        Returns
        -------
        Nothing

        Example
        -------
        .. code-block:: python

            queryClient.set_profile('test')
        '''
        self.svc_profile = qcToString(profile)

    def get_profile(self):
        '''Get the current query profile.

        Parameters
        ----------
        None

        Returns
        -------
        profile : str
            The name of the current profile used with the Query Manager service

        Example
        -------
        .. code-block:: python

            print ("Query Service profile = " + queryClient.get_profile())
        '''
        return qcToString(self.svc_profile)

    def set_timeout_request(self, nsec):
        '''Set the requested Sync query timeout value (in seconds).

        Parameters
        ----------
        nsec : int
            The number of seconds requested before a sync query timeout occurs.
            The service may cap this as a server defined maximum.

        Returns
        -------
        Nothing

        Example
        -------
        .. code-block:: python

            # set the sync query timeout request to 30 seconds
            queryClient.set_timeout_request(30)

        '''
        self.timeout_request = nsec

    def get_timeout_request(self):
        '''Get the current Sync query timeout value.

        Parameters
        ----------
        None

        Returns
        -------
        result: int
            Current sync query timeout value.

        Example
        -------
        .. code-block:: python

            # get the current timeout value
            print (queryClient.get_timeout_request())

        '''
        return self.timeout_request


    # ###########################
    #  Utility Methods
    # ###########################

    @multimethod('_qc',1,True)
    def list_profiles(self, optval, token=None, profile=None, format='text'):
        '''Usage:  queryClient.client.list_profiles (token, ...)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self._list_profiles (token=def_token(optval),
                                        profile=profile, format=format)
        else:
            # optval looks like a token
            return self._list_profiles (token=def_token(token), profile=optval,
                                        format=format)

    @multimethod('_qc',0,True)
    def list_profiles(self, token=None, profile=None, format='text'):
        '''Usage:  queryClient.client.list_profiles (...)
        '''
        return self._list_profiles(token=def_token(token), profile=profile,
                             format=format)

    def _list_profiles(self, token=None, profile=None, format='text'):
        '''Implementation of the list_profiles() method.
        '''

        headers = self.getHeaders (token)

        dburl = '%s/profiles?' % self.svc_url
        if profile != None and profile != 'None' and profile != '':
            dburl += "profile=%s&" % profile
        dburl += "format=%s" % format

        r = requests.get (dburl, headers=headers)
        profiles = qcToString(r.content)
        if '{' in profiles:
            profiles = json.loads(profiles)

        return qcToString(profiles)


    @multimethod('_qc',1,True)
    def schema(self, value, format='text', profile=None):
        '''Usage:  queryClient.schema ([value])
        '''
        return self._schema (value=value, format=format, profile=profile)

    @multimethod('_qc',0,True)
    def schema(self, value='', format='text', profile=None):
        '''Usage:  queryClient.schema ([value])
        '''
        return self._schema (value=value, format=format, profile=profile)


    def _schema(self, value='', format='text', profile=None, **kw):
        '''Implementation of the schema() method.
        '''
        if profile is None:
           profile = self.svc_profile

        url = '%s/schema?value=%s&format=%s&profile=%s' % \
                (self.svc_url, (value), str(format), str(profile))
        try:
            r = requests.get (url, timeout=2)
            resp = r.text
            return resp
        except Exception:
            raise queryClientError("Error getting schema: " + value)

        return qcToString(resp)


    def services(self, name=None, svc_type=None, mode='list',
                  profile='default'):
        '''Usage:  queryClient.services ()
        '''
        return self._services (name=name, svc_type=svc_type, mode=mode,
                                   profile=profile)

    def _services(self, name=None, svc_type=None, mode='list',
                  profile='default'):
        '''Implementation of the services() method.
        '''
        dburl = '/services?'
        if profile is not None and profile != 'None' and profile != '':
            dburl += ("profile=%s" % profile)
        if name is not None and name != 'None' and name != '':
            dburl += ("&name=%s" % name.replace('%','%25'))
        if svc_type is not None and svc_type != 'None' and svc_type != '':
            dburl += ("&type=%s" % svc_type)
        dburl += "&mode=%s" % mode

        r = self.getFromURL(self.svc_url, dburl, def_token(None))
        svcs = qcToString(r.content)

        return svcs



    # ###########################
    #  Query Methods
    # ###########################

    @multimethod('_qc',2,True)
    def query(self, token, query, adql=None, sql=None, fmt='csv', out=None,
               async_=False, profile='default', **kw):
        '''Usage:  queryClient.query (token)
        '''
        return self._query (token=def_token(token), adql=adql, sql=query,
                            fmt=fmt, out=out, async_=async_,
                            profile=profile, **kw)

    @multimethod('_qc',1,True)
    def query(self, optval, adql=None, sql=None, fmt='csv', out=None,
               async_=False, token=None, profile='default', **kw):
        '''Usage:  queryClient.client.query (token, ...)
        '''
        if optval is not None and optval.lower()[:6] == 'select':
            # optval looks like a query string
            return self._query (token=def_token(None), adql=adql, sql=optval,
                                fmt=fmt, out=out, async_=async_,
                                profile=profile, **kw)
        else:
            # optval is (probably) a token
            return self._query (token=def_token(optval), adql=adql, sql=sql,
                                fmt=fmt, out=out, async_=async_,
                                profile=profile, **kw)

    @multimethod('_qc',0,True)
    def query(self, token=None, adql=None, sql=None, fmt='csv', out=None,
               async_=False, profile='default', **kw):
        '''Usage:  queryClient.client.query (...)
        '''
        return self._query (token=def_token(token), adql=adql, sql=sql,
                            fmt=fmt, out=out, async_=async_,
                            profile=profile, **kw)

    def _query(self, token=None, adql=None, sql=None, fmt='csv', out=None,
              async_=False, profile='default', **kw):
        '''Implementation of the query() method.
        '''

        # Process optional keyword arguments.
        if 'async' in kw:
            async_ = kw['async']

        if 'timeout' in kw: 		# set requested timeout on the query
            timeout = int(kw['timeout'])
        else:
            timeout = self.timeout_request
        self.set_timeout_request (timeout)

        wait = self.async_wait 		# see if we wait for an Async result
        if async_ and 'wait' in kw:
            self.async_wait = wait = kw['wait']

        poll_time = 1 			# see if we wait for an Async result
        if async_ and 'poll' in kw:
            self.async_poll = poll_time = int(kw['poll'])

        verbose = False 		# see if we wait for an Async result
        if async_ and 'poll' in kw:
            verbose = kw['verbose']


        # Set service call headers.
        headers = {'Content-Type': 'text/ascii',
                   'X-DL-TimeoutRequest': str(timeout),
                   'X-DL-ClientVersion': __version__,
                   'X-DL-OriginIP': self.hostip,
                   'X-DL-OriginHost': self.hostname,
                   'X-DL-AuthToken': def_token(token)}  # application/x-sql

        if fmt in ['pandas','array','structarray','table']:
            qfmt = 'csv'
        else:
            qfmt = fmt

        if adql is not None and adql != '':
            # Check for unsupported ADQL functions.
            #if 'q3c_' in adql.lower():
            #    msg = "Q3C functions are not allowed in ADQL queries"
            #    raise queryClientError(msg)
            #if 'healpix_' in adql.lower():
            #    msg = "Healpix functions are is not allowed in ADQL queries"
            #    raise queryClientError(msg)
            #if 'htm_' in adql.lower():
            #    msg = "HTM functions are is not allowed in ADQL queries"
            #    raise queryClientError(msg)

            query = quote_plus(adql)		# URL-encode the query string
            dburl = '%s/query?adql=%s&ofmt=%s&out=%s&async=%s' % (
                self.svc_url, query, qfmt, out, async_)

        elif sql is not None and sql != '':
            query = quote_plus(sql)		# URL-encode the query string
            dburl = '%s/query?sql=%s&ofmt=%s&out=%s&async=%s' % (
                self.svc_url, query, qfmt, out, async_)
        else:
            raise queryClientError("No query specified")

        if profile != "default":        	# append the service profile
            dburl += "&profile=%s" % profile
        else:
            dburl += "&profile=%s" % self.svc_profile

        # Make the service call.
        r = requests.get (dburl, headers=headers)
        if r.status_code != 200:
            raise queryClientError (r.text)
        resp = r.content

        if async_ and wait:
            # Sync query timeouts are handled on the server.  If waiting
            # for an async query, loop until job is completed or the timeout
            # expires.
            jobId = resp
            stat = self._status (token=token, jobId=jobId)
            tval = 0
            while (stat != 'COMPLETED'):
                time.sleep (poll_time)
                try:
                    stat = self._status (token=token, jobId=jobId)
                except Exception as e:
                    raise queryClientError (str(e))
                else:
                    if tval > self.timeout_request:
                        stat = self._abort (token=token, jobId=jobId)
                        break
                    if verbose:
                        tim = tval * poll_time
                        rem = timeout - tim
                        print ('Status = %s; elapsed time: %d, timeout in %d' %
                               (stat, tim, rem))
                tval = tval + poll_time

            if tval > self.timeout_request:
                if verbose:
                    print ('Timeout (%d sec) exceeded' % self.timeout_request)
                raise queryClientError ('Query timeout exceeded')
            elif stat not in ['COMPLETED','ERROR']:
                resp = stat
            elif stat == 'ERROR':
		# Retrieve Async error.
                if verbose:
                    print ('Retrieving error')
                resp = self._error (token=token, jobId=jobId).lower()
            elif stat == 'COMPLETED':
		# Retrieve Async results.  A save to vos/mydb is handled below.
                if verbose:
                    print ('Retrieving results')
                resp = self._results (token=token, jobId=jobId).lower()

        if (out is not None and out != '') and not async_:
            # If we're saving to a local file (e.g. in a notebook directory),
            # the file here.  Results saved to VOSpace or MyDB are handled on
            # the server side.
            if out[:7] == 'file://':
                out = out[7:]
            if ':' not in out or out[:out.index(':')] not in ['vos', 'mydb']:
                file = open (out, 'wb')
                file.write (resp)
                file.close ()
            return 'OK'
        else:
            # Otherwise, simply return the result of the query.
            strval = qcToString(resp)
            if fmt in ['pandas','array','structarray','table']:
                return convert (strval,fmt)
            else:
                return strval


    # --------------------------
    # Async jobs status()
    # --------------------------

    @multimethod('_qc',2,True)
    def status(self, token, jobId):
        '''Usage:  queryClient.status (token, jobID)
        '''
        return self._status (token=def_token(token), jobId=jobId)

    @multimethod('_qc',1,True)
    def status(self, optval, jobId=None):
        '''Usage:  queryClient.status (jobID)
                   queryClient.status (token, jobId=<id>)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self._status (token=def_token(optval), jobId=jobId)
        else:
            # optval is probably a jobId
            return self._status (token=def_token(None), jobId=optval)

    @multimethod('_qc',0,True)
    def status(self, token=None, jobId=None):
        '''Usage:  queryClient.status (jobID=<str>)
        '''
        return self._status (token=def_token(token), jobId=jobId)

    def _status(self, token=None, jobId=None):
        '''Implementation of the status() method.
        '''
        headers = self.getHeaders (token)

        dburl = '%s/status?jobid=%s' % (self.svc_url, jobId)
        if self.svc_profile != "default":
            dburl += "&profile=%s" % self.svc_profile

        r = requests.get (dburl, headers=headers)
        return qcToString(r.content)


    # --------------------------
    # Async jobs results()
    # --------------------------

    @multimethod('_qc',2,True)
    def results(self, token, jobId, delete=True):
        '''Usage:  queryClient.results (token, jobID)
        '''
        return self._results (token=def_token(token), jobId=jobId,
                              delete=delete)

    @multimethod('_qc',1,True)
    def results(self, optval, jobId=None, delete=True):
        '''Usage:  queryClient.results (jobID)
                   queryClient.results (token, jobId=<id>)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self._results (token=def_token(optval), jobId=jobId,
                                  delete=delete)
        else:
            # optval is probably a jobId
            return self._results (token=def_token(None), jobId=optval,
                                  delete=delete)

    @multimethod('_qc',0,True)
    def results(self, token=None, jobId=None, delete=True):
        '''Usage:  queryClient.results (jobID=<str>)
        '''
        return self._results (token=def_token(token), jobId=jobId,
                              delete=delete)

    def _results(self, token=None, jobId=None, delete=True):
        '''Implementation of the results() method.
        '''
        headers = self.getHeaders (token)

        dburl = '%s/results?jobid=%s&delete=%s' % (self.svc_url, jobId, delete)
        if self.svc_profile != "default":
            dburl += "&profile=%s" % self.svc_profile

        r = requests.get (dburl, headers=headers)
        return qcToString(r.content)


    # --------------------------
    # Async jobs error()
    # --------------------------

    @multimethod('_qc',2,True)
    def error(self, token, jobId):
        '''Usage:  queryClient.error (token, jobID)
        '''
        return self._error (token=def_token(token), jobId=jobId)

    @multimethod('_qc',1,True)
    def error(self, optval, jobId=None):
        '''Usage:  queryClient.error (jobID)
                     queryClient.error (token, jobId=<id>)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self._error (token=def_token(optval), jobId=jobId)
        else:
            # optval is probably a jobId
            return self._error (token=def_token(None), jobId=optval)

    @multimethod('_qc',0,True)
    def error(self, token=None, jobId=None):
        '''Usage:  queryClient.error (jobID=<str>)
        '''
        return self._error (token=def_token(token), jobId=jobId)

    def _error(self, token=None, jobId=None):
        '''Implementation of the error() method.
        '''
        headers = self.getHeaders (token)

        dburl = '%s/error?jobid=%s' % (self.svc_url, jobId)
        if self.svc_profile != "default":
            dburl += "&profile=%s" % self.svc_profile

        r = requests.get (dburl, headers=headers)
        return qcToString(r.content)


    # --------------------------
    # Async jobs abort()
    # --------------------------

    @multimethod('_qc',2,True)
    def abort(self, token, jobId):
        '''Usage:  queryClient.abort (token, jobID)
        '''
        return self._abort (token=def_token(token), jobId=jobId)

    @multimethod('_qc',1,True)
    def abort(self, optval, jobId=None):
        '''Usage:  queryClient.abort (jobID)
                   queryClient.abort (token, jobId=<id>)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self._abort (token=def_token(optval), jobId=jobId)
        else:
            # optval is probably a jobId
            return self._abort (token=def_token(None), jobId=optval)

    @multimethod('_qc',0,True)
    def abort(self, token=None, jobId=None):
        '''Usage:  queryClient.abort (jobID=<str>)
        '''
        return self._abort (token=def_token(token), jobId=jobId)

    def _abort(self, token=None, jobId=None):
        '''Implementation of the abort() method.
        '''
        headers = self.getHeaders (token)

        dburl = '%s/abort?jobid=%s' % (self.svc_url, jobId)
        if self.svc_profile != "default":
            dburl += "&profile=%s" % self.svc_profile

        r = requests.get (dburl, headers=headers)
        return qcToString(r.content)


#=========================================================================

    # ###########################
    #  MyDB Methods
    # ###########################

    # LIST -- List the tables or table schema in the user's MyDB.
    #
    @multimethod('_qc',2,True)
    def list(self, token, table):
        '''Usage:  queryClient.list (token, table)
        '''
        return self.mydb_list (token=def_token(token), table=table)

    @multimethod('_qc',1,True)
    def list(self, optval, table=None):
        '''Usage:  queryClient.list (table)
                     queryClient.list (token, table=<id>)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self.mydb_list (token=def_token(optval), table=table)
        else:
            # optval is probably a table
            return self.mydb_list (token=def_token(None), table=optval)

    @multimethod('_qc',0,True)
    def list(self, token=None, table=None):
        '''Usage:  queryClient.list (table=<str>)
        '''
        return self.mydb_list (token=def_token(token), table=table)


    # DROP -- Drop the specified table from the user's MyDB
    #
    @multimethod('_qc',2,True)
    def drop(self, token, table):
        '''Usage:  queryClient.drop (token, table)
        '''
        return self.mydb_drop (token=def_token(token), table=table)

    @multimethod('_qc',1,True)
    def drop(self, optval, table=None):
        '''Usage:  queryClient.drop (table)
                     queryClient.drop (token, table=<id>)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self.mydb_drop (token=def_token(optval), table=table)
        else:
            # optval is probably a table
            return self._drop (token=def_token(None), table=optval)

    @multimethod('_qc',0,True)
    def drop(self, token=None, table=None):
        '''Usage:  queryClient.drop (table=<str>)
        '''
        return self.mydb_drop (token=def_token(token), table=table)


    # -----------------------------
    #  MyDB Functions (New API)
    # -----------------------------

    # --------------------------------------------------------------------
    # MYDB_LIST -- List the tables or table schema in a user's MyDB.
    #
    @multimethod('_qc',1,True)
    def mydb_list(self, optval, table=None, index=False):
        '''Usage:  queryClient.mydb_list (table)
                     queryClient.mydb_list (token, table=<str>)
        '''
        if optval is not None and len(optval.split('.')) >= 4:
            # optval looks like a token
            return self._mydb_list (token=def_token(optval), table=table,
                                    index=index)
        else:
            # optval is probably a table
            return self._mydb_list (token=def_token(None), table=optval,
                                    index=index)

    @multimethod('_qc',0,True)
    def mydb_list(self, token=None, table=None, index=False):
        '''Usage:  queryClient.mydb_list (table=<str>)
        '''
        return self._mydb_list (token=def_token(token), table=table,
                                index=index)

    def _mydb_list(self, token=None, table=None, index=False):
        '''Implementation of the mydb_list() method.
        '''
        headers = self.getHeaders (token)

        if table is None:
            table = ''
        dburl = '%s/list?table=%s&index=%s' % (self.svc_url, table, str(index))
        if self.svc_profile != "default":
            dburl += "&profile=%s" % self.svc_profile

        r = requests.get (dburl, headers=headers)
        return qcToString(r.content)


    # --------------------------------------------------------------------
    # MYDB_CREATE -- Copy a table in the user's MyDB.
    #
    @multimethod('_qc',3,True)
    def mydb_create(self, token, table, schema, **kw):
        '''Usage:  queryClient.mydb_create (token, table, <schema_dict>)
        '''
        return self._mydb_create (token=def_token(None), table=table,
                                  schema=schema, **kw)

    @multimethod('_qc',2,True)
    def mydb_create(self, table, schema, token=None, **kw):
        '''Usage:  queryClient.mydb_create (table, <schema_dict>)
        '''
        return self._mydb_create (token=def_token(None), table=table,
                                  schema=schema, **kw)

    def _mydb_create(self, token, table, schema, **kw):
        '''Implementation of the mydb_create() method.
        '''
        # Set the request headers.
        headers = self.getHeaders (token)
        headers['Content'] = 'text/ascii'

        params = { 'table' : table}
        dburl = '%s/create' % (self.svc_url)

        drop = True			# drop table if exists
        verbose = False
        verbose = False			# verbose output
        if 'verbose' in kw:
            verbose = kw['verbose']
        if 'drop' in kw:
            drop = kw['drop']
        params['verbose'] = str(verbose)
        params['drop'] = str(drop)
        params['profile'] = self.svc_profile

        # Schema can be a dictionary, a CSV string, or the name of a file.
        if isinstance (schema,str):
            if os.path.exists (schema):
                with open(schema, 'r') as f:
                    s = f.read()
                params['schema'] = s
            else:
                params['schema'] = schema

        elif isinstance (schema,collections.OrderedDict):
            # We can't use a regular 'dict' object because the key ordered isn't
            # guaranteed to be preserved, but allow OrderedDict.
            s = ''
            for i in schema:
               s += i + ',' + schema[i] + '\n'
            params['schema'] = s

        r = requests.post (dburl, params=params, headers=headers)

        if r.content[:5].lower() == 'error':
            raise queryClientError (qcToString(r.content))
        else:
            return 'OK'


    # --------------------------------------------------------------------
    # MYDB_INSERT -- Insert data into a table in the user's MyDB from a local
    # file or python data object.
    #
    @multimethod('_qc',3,True)
    def mydb_insert(self, token, table, data, **kw):
        '''Usage:  queryClient.mydb_insert (token, table, <filename>)
                   queryClient.mydb_insert (token, table, <data_object>)
        '''
        return self._mydb_insert (token=def_token(token), table=table,
                                  data=data, **kw)

    @multimethod('_qc',2,True)
    def mydb_insert(self, table, data, token=None, **kw):
        '''Usage:  queryClient.mydb_insert (table, <filename>)
                   queryClient.mydb_insert (table, <data_object>)
        '''
        return self._mydb_insert (token=def_token(token), table=table,
                                  data=data, **kw)

    def _mydb_insert(self, token=None, table=None, data=None, **kw):
        '''Implementation of the mydb_create() method.
        '''
        # Get optional parameters.
        csv_header = (kw['csv_header'] if 'csv_header' in kw else True)
        verbose = (kw['drop'] if 'drop' in kw else False)

        # Set up the request headers and initialize.
        params = {}
        headers = self.getHeaders (token)
        params['profile'] = self.svc_profile
        dburl = '%s/ingest' % (self.svc_url)

        # Data can be the name of a CSV file or a python tablular object that
        # can be converted.
        tmp_file = NamedTemporaryFile(delete=True, dir='/tmp').name
        if isinstance (data, str):
            params = { 'table' : table,
                       'csv_header' : str(csv_header) }
            if data.startswith ('http://') or data.startswith('https://') or \
               data.startswith ('vos://'):
                    # Passing a URI in the filename to be loaded on server-side
                    params['filename'] = data

            elif os.path.exists (data):
                # Upload the file to the staging area.
                data_name = os.path.basename (data)
                storeClient.chunked_upload (token, data, data_name)
                params['filename'] = data_name

            else:
                # Upload the file to the staging area.
                with open(tmp_file, 'w') as f:
                    f.write(data)
                f.close()
                tmp_name = os.path.basename (tmp_file)
                storeClient.chunked_upload(token, tmp_file, tmp_name)
                params['filename'] = tmp_name

        elif isinstance (data, pandas.core.frame.DataFrame):
            # Convert a DataFrame to a CSV string object.
            schema, data_to_load = self.getSchema (data)

            with open (tmp_file, 'w') as f:
                f.write(data_to_load)
            f.close()
            tmp_name = os.path.basename (tmp_file)
            storeClient.chunked_upload(token, tmp_file, tmp_name)
            params['filename'] = tmp_name

        else:
            pass

        r = requests.post (dburl, params=params, headers=headers)

        if tmp_file is not None and os.path.exists(tmp_file):
            os.remove (tmp_file)
        if verbose or self.debug:
            print (str(r.text))

        if r.content[:5].lower() == 'error':
            raise queryClientError (qcToString(r.content))
        else:
            return 'OK'


    # --------------------------------------------------------------------
    # MYDB_IMPORT -- Import a file or Python object to a MyDB table.
    #
    @multimethod('_qc',3,True)
    def mydb_import(self, token, table, data, **kw):
        '''Usage:  queryClient.mydb_import (token, table, data)
        '''
        return self._mydb_import (token=def_token(token), table=table,
                                  data=data, **kw)

    @multimethod('_qc',2,True)
    def mydb_import(self, table, data, token=None, **kw):
        '''Usage:  queryClient.mydb_import (table, data)
        '''
        return self._mydb_import (token=def_token(token), table=table,
                                  data=data, **kw)

    def _mydb_import(self, token=None, table=None, data=None, **kw):
        '''Implementation of the mydb_create() method.
        '''
        # Get optional parameters.
        csv_header = (kw['csv_header'] if 'csv_header' in kw else True)
        verbose = (kw['drop'] if 'drop' in kw else False)
        drop = (kw['drop'] if 'drop' in kw else True)
        delimiter = (kw['delimiter'] if 'delimiter' in kw else ',')

        # Set up the request headers and initialize.
        headers = self.getHeaders (token)
        dburl = '%s/import' % (self.svc_url)
        if self.svc_profile != "default":
            dburl += "&profile=%s" % self.svc_profile

        # Data can be the name of a CSV file or a python tablular object that
        # can be converted.
        tmp_file = NamedTemporaryFile(delete=True, dir='/tmp').name
        params = { 'table' : table,
                   'delimiter' : str(delimiter),
                   'drop' : str(drop),
                   'profile' : self.svc_profile,
                   'csv_header' : str(csv_header) }

        if isinstance (data, str):
            if data.startswith ('http://') or \
               data.startswith ('https://') or \
               data.startswith ('vos://'):
                    # Passing a URI in the filename to be loaded on server-side
                    params['filename'] = data

            elif os.path.exists (data):
                # Upload the file to the staging area.
                data_name = os.path.basename (data)
                storeClient.chunked_upload (token, data, data_name)
                params['filename'] = data_name

            else:
                # Upload the CSV string to the staging area.
                with open (tmp_file, 'w') as f:
                    f.write(data)
                f.close()
                tmp_name = os.path.basename (tmp_file)
                storeClient.chunked_upload(token, tmp_file, tmp_name)
                params['filename'] = tmp_name

        elif isinstance (data, pandas.core.frame.DataFrame):
            # Convert a DataFrame to a CSV string object.
            schema, data_to_load = self.getSchema (data)

            with open (tmp_file, 'w') as f:
                f.write(data_to_load)
            f.close()
            tmp_name = os.path.basename (tmp_file)
            storeClient.chunked_upload(token, tmp_file, tmp_name)
            params['filename'] = tmp_name

        else:
            # Reserved for future format support.
            pass

        # Execute the service call.
        r = requests.post (dburl, params=params, headers=headers)

        if verbose or self.debug:
            print (qcToString (r.content))
        if tmp_file is not None and os.path.exists(tmp_file):
            os.remove(tmp_file)

        if r.content[:5].lower() == 'error' or r.status_code != 200:
            raise queryClientError (qcToString(r.content))
        else:
            return 'OK'


    # --------------------------------------------------------------------
    # MYDB_TRUNCATE -- Truncate a table in the user's MyDB.
    #
    @multimethod('_qc',2,True)
    def mydb_truncate(self, token, table):
        '''Usage:  queryClient.mydb_truncate (token, table)
        '''
        return self._mydb_truncate (token=def_token(token), table=table)

    @multimethod('_qc',1,True)
    def mydb_truncate(self, table, token=None):
        '''Usage:  queryClient.mydb_truncate (table)
        '''
        return self._mydb_truncate (token=def_token(token), table=table)

    def _mydb_truncate(self, token=None, table=None):
        '''Implementation of the mydb_truncate() method.
        '''
        headers = self.getHeaders (token)

        dburl = '%s/truncate?table=%s' % (self.svc_url, table)
        if self.svc_profile != "default":
            dburl += "&profile=%s" % self.svc_profile

        r = requests.get (dburl, headers=headers)
        if r.content[:5].lower() == 'error':
            return qcToString(r.content)
        else:
            return 'OK'



    # --------------------------------------------------------------------
    # MYDB_INDEX -- Index a column in a user's MyDB table.
    #
    @multimethod('_qc',3,True)
    def mydb_index(self, token, table, column, q3c=None, cluster=False,
                    async_=False):
        '''Usage:  queryClient.mydb_index (token, table, column)
        '''
        return self._mydb_index(token=def_token(token), table=table,
                                column=column, q3c=q3c, cluster=cluster,
                                async_=async_)

    @multimethod('_qc',2,True)
    def mydb_index(self, table, column, token=None, q3c=None, cluster=False,
                    async_=False):
        '''Usage:  queryClient.mydb_index (table, colunm)
        '''
        return self._mydb_index(token=def_token(token), table=table,
                                column=column, q3c=q3c, cluster=cluster,
                                async_=async_)

    def _mydb_index(self, token=None, table=None, column='', q3c=None,
                    cluster=False, async_=False):
        '''Implementation of the mydb_index() method.
        '''
        headers = self.getHeaders (token)

        if async_:
            def async_call (url, params, headers):
                r = requests.get (url, params=params, headers=headers)
                return qcToString(r.content)

            from multiprocessing.pool import ThreadPool
            params = { 'tbl' : table,
                       'col' : column,
                       'profile' : self.svc_profile }
            if q3c is not None:
                params['q3c'] = q3c
                params['cluster'] = cluster

            pool = ThreadPool(processes=1)
            dburl = '%s/index' % self.svc_url
            res = pool.apply_async (async_call, (dburl, params, headers))
            return 'OK'
        else:
            dburl = '%s/index?tbl=%s&col=%s' % (self.svc_url, table, column)
            if q3c is not None:
                dburl += '&q3c=%s&cluster=%s' % (q3c, cluster)
            if self.svc_profile != "default":
                dburl += "&profile=%s" % self.svc_profile

            r = requests.get (dburl, headers=headers)
            if r.content[:5].lower() == 'error':
                return qcToString(r.content)
            else:
                return 'OK'



    # --------------------------------------------------------------------
    # MYDB_DROP -- Drop the named table from a user's MyDB.
    #
    @multimethod('_qc',2,True)
    def mydb_drop(self, token, table):
        '''Usage:  queryClient.mydb_drop (token, table)
        '''
        return self._mydb_drop (token=def_token(token), table=table)

    @multimethod('_qc',1,True)
    def mydb_drop(self, table, token=None):
        '''Usage:  queryClient.mydb_drop (table)
                     queryClient.mydb_drop (token, table=<id>)
        '''
        return self._mydb_drop (token=def_token(token), table=table)

    def _mydb_drop(self, token=None, table=None):
        '''Implementation of the mydb_drop() method.
        '''
        headers = self.getHeaders (token)

        dburl = '%s/delete?table=%s' % (self.svc_url, table)
        if self.svc_profile != "default":
            dburl += "&profile=%s" % self.svc_profile

        r = requests.get (dburl, headers=headers)
        if r.content[:5].lower() == 'error':
            return qcToString(r.content)
        else:
            return 'OK'


    # --------------------------------------------------------------------
    # MYDB_RENAME -- Rename a table in the user's MyDB.
    #
    @multimethod('_qc',3,True)
    def mydb_rename(self, token, source, target):
        '''Usage:  queryClient.mydb_rename (token, source, target)
        '''
        return self._mydb_rename (token=def_token(token),
                                    source=source, target=target)

    @multimethod('_qc',2,True)
    def mydb_rename(self, source, target, token=None):
        '''Usage:  queryClient.mydb_rename (source, target)
        '''
        return self._mydb_rename (source=source, target=target,
                                  token=def_token(token))

    def _mydb_rename(self, source='', target='', token=None):
        '''Implementation of the mydb_rename() method.
        '''
        headers = self.getHeaders (token)

        dburl = '%s/rename?source=%s&target=%s' % (self.svc_url, source, target)
        if self.svc_profile != "default":
            dburl += "&profile=%s" % self.svc_profile

        r = requests.get (dburl, headers=headers)
        if r.content[:5].lower() == 'error':
            return qcToString(r.content)
        else:
            return 'OK'

    # --------------------------------------------------------------------
    # MYDB_COPY -- Copy a table in the user's MyDB.
    #
    @multimethod('_qc',3,True)
    def mydb_copy(self, token, source, target):
        '''Usage:  queryClient.mydb_copy (token, source, target)
        '''
        return self._mydb_copy (token=def_token(token),
                                  source=source, target=target)

    @multimethod('_qc',2,True)
    def mydb_copy(self, source, target, token=None):
        '''Usage:  queryClient.mydb_copy (source, target)
        '''
        return self._mydb_copy (source=source, target=target,
                                token=def_token(token))

    def _mydb_copy(self, source='', target='', token=None):
        '''Implementation of the mydb_copy() method.
        '''
        headers = self.getHeaders (token)

        dburl = '%s/copy?source=%s&target=%s' % (self.svc_url, source, target)
        if self.svc_profile != "default":
            dburl += "&profile=%s" % self.svc_profile

        r = requests.get (dburl, headers=headers)

        if r.content[:5].lower() == 'error':
            return qcToString(r.content)
        else:
            return 'OK'


    @staticmethod
    def pretty_print_POST(req):
        '''
        At this point it is completely built and ready
        to be fired; it is "prepared".

        However pay attention at the formatting used in
        this function because it is programmed to be pretty
        printed and may differ from the actual request.
        '''
        print('{}\n{}\n{}\n\n{}'.format(
            '-----------START-----------',
            req.method + ' ' + req.url,
            '\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
            req.body,
        ))


    # ############################################
    #    DEPRECATED / NOT-YET-IMPLEMENTED
    # ############################################

    # SIAQUERY -- Send a SIA query to the query manager service
    #
    def siaquery(self, token, input=None, out=None, search=0.5):
        '''Send a SIA (Simple Image Access) query to the query manager service
        '''

        headers = self.getHeaders (token)
        user, uid, gid, hash = token.strip().split('.', 3)

        shortname = '%s_%s' % (uid, input[input.rfind('/') + 1:])
        if input[:input.find(':')] not in ['vos', 'mydb']:
            # Need to set this from config?
            target = 'vos://datalab.noao.edu!vospace/siawork/%s' % shortname
            r = requests.get (SM_SERVICE_URL + "/put?name=%s" %
                             target, headers=headers)
            file = open(input).read()

            headers2 = {'Content-type': 'application/octet-stream',
                        'X-DL-AuthToken': token}
            requests.put(r.content, data=file, headers=headers2)

        dburl = '%s/sia?in=%s&radius=%s&out=%s' % (
            self.svc_url, shortname, search, out)
        r = requests.get (dburl, headers=headers)

        if out is not None:
            if out[:out.index(':')] not in ['vos', 'mydb']:
                file = open(out, 'wb')
                file.write(r.content)
                file.close()
        else:
            return qcToString(r.content)


    # CONEQUERY -- Send a cone search query to the query manager service
    #
    def conequery(self, token, input=None, out=None, schema=None, table=None, ra=None, dec=None, search=0.5):
        '''Send a cone search query to the consearch service
        '''

        headers = self.getHeaders (token)
        user, uid, gid, hash = token.strip().split('.', 3)

    #    shortname = '%s_%s' % (uid, input[input.rfind('/') + 1:])
    #    if input[:input.find(':')] not in ['vos', 'mydb']:
    #        # Need to set this from config?
    #        target = 'vos://datalab.noao.edu!vospace/siawork/%s' % shortname
    #        r = requests.get (SM_SERVICE_URL + "/put?name=%s" %
    #                         target, headers={'X-DL-AuthToken': token})
    #        file = open(input).read()
    #
    #        headers2 = {'Content-type': 'application/octet-stream',
    #                    'X-DL-AuthToken': token}
    #        requests.put(r.content, data=file, headers=headers2)

        dburl = '%s/scs/%s/%s?ra=%s&dec=%s&radius=%s' % (
            DAL_SERVICE_URL, schema, table, ra, dec)
        r = requests.get (dburl, headers=headers)

        if out is not None:
            if out[:out.index(':')] not in ['vos', 'mydb']:
                file = open(out, 'wb')
                file.write(r.content)
                file.close()
        else:
            return qcToString(r.content)


    # -------------------------------------------------------
    #  Private Utility Methods
    # -------------------------------------------------------

    def getHeaders(self, token):
        '''Get default tracking headers,
        '''
        tok = def_token(token)
        user, uid, gid, hash = tok.strip().split('.', 3)
        hdrs = {#'Content-Type': 'text/ascii',
                'X-DL-ClientVersion': __version__,
                'X-DL-OriginIP': self.hostip,
                'X-DL-OriginHost': self.hostname,
                'X-DL-User': user,
                'X-DL-AuthToken': tok}                  # application/x-sql

        return hdrs

    def getFromURL(self, svc_url, path, token):
        '''Get something from a URL.  Return a 'response' object
        '''
        try:
            hdrs = self.getHeaders (token)
            resp = requests.get("%s%s" % (svc_url, path), headers=hdrs)

        except Exception as e:
            raise queryClientError(str(e))
        return resp

    def chunked_upload(self, token, local_file, remote_file):
        '''A streaming file uploader.
        '''
        debug = False
        init = True
        CHUNK_SIZE = 4 * 1024 * 1024                   # 16MB chunks
        url = '%s/xfer' % (self.svc_url)

        # Get the size of the file to be transferred.
        fsize = os.stat(local_file).st_size
        nchunks = fsize / CHUNK_SIZE + 1
        if (debug): print ('Upload in %d chunks ....' % nchunks)
        with open(local_file, 'rb') as f:
            try:
                nsent = 0
                while nsent < fsize:
                    data = f.read(CHUNK_SIZE)
                    requests.post (url, data,
                        headers={'Content-type': 'application/octet-stream',
                                 'X-DL-FileName': remote_file,
                                 'X-DL-InitXfer': str(init),
                                 'X-DL-AuthToken': token})
                    nsent += len(data)
                    if init: init = False
            except Exception as e:
                raise queryClientError ('Upload error: ' + str(e))

    def dataType(self, val, current_type):
        '''Lexically scan a value to determine the datatype.
        '''
        try:
            # Evaluates numbers to an appropriate type, and strings an error
            t = ast.literal_eval(val.strip())
        except ValueError:
            return 'text'
        except SyntaxError:
            return 'text'
        if type(t) in [int, float]:
           if (type(t) in [int]) and current_type not in ['float', 'varchar']:
               # Use smallest possible int type
               if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
                   return 'smallint'
               elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
                   return 'int'
               else:
                   return 'bigint'
           if type(t) is float and current_type not in ['varchar']:
               return ('float' if len(val) < 6 else 'double precision')
        else:
            return 'text'

    def getSchema(self, data, **kw):
        '''Generate a schema for mydb_create() from a CSV file or data object.
        '''
        data_obj = data
        if isinstance (data, str):
            # If a file, assume it is CSV but allow the 'delimiter' et al opts
            if os.path.exists (data):
                reader = csv.reader(open(data, 'r'), **kw)
            else:
                reader = csv.reader(StringIO(data), **kw)
        elif isinstance (data, pandas.core.frame.DataFrame):
            data_obj = data.to_csv(index=False)
            reader = csv.reader(StringIO(data_obj), **kw)
        else:
            print ('Unsupported data format')
            return '', data

        # TODO: Check behavior when no CSV header and data row contains floats

        longest, headers, type_list = [], [], []
        nrows = 10
        for row in reader:
            if len(headers) == 0:		# First row of CSV
                headers = row
                for col in row:
                    longest.append(0)
                    type_list.append('')
            else:
                for i in range(len(row)):
                    if type_list[i] == 'varchar' or row[i] == 'NA':
                        # NA is the csv null value
                        pass
                    else:
                        var_type = self.dataType(row[i], type_list[i])
                        type_list[i] = var_type
                if len(row[i]) > longest[i]:
                    longest[i] = len(row[i])
            if nrows == 0:			# Only read first 10 rows
                break
            nrows = nrows - 1

        schema = ''
        for i in range(len(headers)):
            if type_list[i] == 'text':
                schema += '{},text\n'.format(headers[i].lower().replace('.','_'))
            else:
                schema += '{},{}\n'.format(headers[i].lower().replace('.','_'), type_list[i])

        return schema, data_obj



# ###################################
#  Query Client Handles
# ###################################

def getClient(profile=DEF_PROFILE, svc_url=DEF_SERVICE_URL):
    '''Create a new queryClient object and set a default profile.
    '''
    return queryClient(profile=profile, svc_url=svc_url)

# The default client handle for the module.
qc_client = getClient(profile=DEF_PROFILE, svc_url=DEF_SERVICE_URL)



# ##########################################
#  Patch the docstrings for module functions
#  that aren't MultiMethods.
# ##########################################

isAlive.__doc__ = qc_client.isAlive.__doc__
services.__doc__ = qc_client.services.__doc__
set_svc_url.__doc__ = qc_client.set_svc_url.__doc__
get_svc_url.__doc__ = qc_client.get_svc_url.__doc__
set_profile.__doc__ = qc_client.set_profile.__doc__
get_profile.__doc__ = qc_client.get_profile.__doc__
set_timeout_request.__doc__ = qc_client.set_timeout_request.__doc__
get_timeout_request.__doc__ = qc_client.get_timeout_request.__doc__


# ####################################################################
#  Py2/Py3 Compatability Utilities
# ####################################################################

def qcToString(s):
    '''qcToString -- Force a return value to be type 'string' for all
                      Python versions.
    '''
    strval = s
    if is_py3 and isinstance(s,bytes):
        strval = str(s.decode())
    elif not is_py3 and (isinstance(s,bytes) or isinstance(s,unicode)):
        strval = str(s)
    else:
        strval = s

    return strval
