"""Legacy helpers for Data Lab. Most are deprecated."""

__authors__ = 'Robert Nikutta <nikutta@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20200204' # yyyymmdd

# std lib imports
from io import StringIO          # python 3
from queue import deque
from functools import partial

from collections import OrderedDict
import getpass
import warnings
warnings.simplefilter('always', DeprecationWarning)

# 3rd party Python imports
import numpy as np
from pandas import read_csv
from astropy.table import Table
from astropy.io.votable import parse_single_table

# Data Lab imports
from dl import authClient, queryClient


class Querist:

    def __init__(self,username='anonymous'):

        """Helper class to authenticate user with Data Lab, run queries, and
        convert results to the requested data type.

        Parameters
        ----------
        username : str
            User name, will be supplied to :func:`authClient.login()`
            to obtain an authentication token. The default username is
            'anonymous', which obtains an anonymous access token from
            :func:`authClient.login()`.

            Other user names will trigger a password prompt.

            The token can be cleared by calling :func:`clearToken()`.

        """

        warnings.warn("The 'Querist' helper class is deprecated, and may be removed in future versions of Data Lab. Please use 'dlinterface'.",DeprecationWarning)

        # obtain auth token in secure way
        self.token = self._getToken(username)

        # map outfmt container types to a tuple:
        # (:func:`queryClient.query()` fmt-value, descriptive title,
        # processing function for the result string)
        self.mapping = OrderedDict([
            ('string'      , ('csv',     'CSV formatted table as a string', lambda x: x.getvalue())),
            ('array'       , ('csv',     'Numpy array',                     partial(np.loadtxt,unpack=False,skiprows=1,delimiter=','))),
            ('structarray' , ('csv',     'Numpy structured / record array', partial(np.genfromtxt,dtype=float,delimiter=',',names=True))),
            ('pandas'      , ('csv',     'Pandas dataframe',                read_csv)),
            ('table'       , ('csv',     'Astropy Table',                   partial(Table.read,format='csv'))),
            ('votable'     , ('votable', 'Astropy VOtable',                 parse_single_table))
        ])

        self.openjobs = deque()  # FIFO queue of submitted async jobIDs


    def _getToken(self,username):

        """Get authentication token through :func:`authClient.login()`

        Parameters
        ----------
        username : str
            If 'anonymous', use default password and obtain default
            auth token. Otherwise prompt for password while trying to
            obtain a valid auth token.

            :func:`authClient.login() returns either a valid token, or
            an error message (as string), so we check the return value
            using :func:`authClient.isValidToken()`. If this returns
            False, we raise an Exception here.
        """

        if username == 'anonymous':
            token = authClient.login('anonymous','')
        else:
#            print("Enter password:")
            token = authClient.login(username,getpass.getpass(prompt='Enter password:'))

        if not authClient.isValidToken(token):
            raise Exception ("Invalid user name and/or password provided. Please try again.")
        else:
            print("Authentication successful.")
            return token


    def clearToken(self):

        """Sets to token to empty string. Useful e.g. before saving a notebook."""

        self.token = ''


    def __call__(self,query=None,outfmt='array',preview=0,async_=False,**kw):

        """Submit `query` string via :func:`queryClient.query()`, and process
        the result.

        Parameters
        ----------
        query : str or None
            The query string (sql). Example:

            .. code-block:: python

               query = "SELECT ra,dec,g FROM ls_dr3.tractor_primary WHERE g != 'nan'"

            If None, and the async FIFO queue is not empty, this
            triggers an attempt to retrieve the query results for the
            first async job in int queue. See below for more details.

        outfmt : str
            Desired output container type. The result of a query will
            be returned in this format. Possible values are:

            ``'string'`` (default) -- A table as comma-separated string.

            ``'array'`` -- Numpy array, with shape (ncols,nrows)

            ``'array'`` -- Numpy structured / record array, with shape (ncols,), and column names.

            ``'pandas'`` -- Pandas dataframe.

            ``'table'`` -- Astropy.table Table object.

            ``'votable'`` -- Astropy.io.votable. Note that this is much slower than e.g. 'pandas' or 'array'.

        preview : int
            Number of lines to preview on STDOUT. This does not count
            the header line. If `outfmt='votable'`, `preview` is not
            very useful, because of the XML that votable carries
            around. Default: 0

        async_ : bool
            If ``False`` (default), submit queries in sync mode,
            i.e. expecting results immediately.

            If ``True``, submit query in async mode, storing the jobid
            in a FIFO queue (first-in-first-out). A subsequent call
            without arguments will attempt to retrieve the query
            result. If the query status is not yet ``COMPLETED``, the
            jobid is re-inserted into the queue (at old position), and
            the user is instructed to try later.

            Not yet implemented: automatic re-submission of a sync
            query in async mode, if the queryManager / DB raise
            Exception that "the query not suitable for sync mode".

            ``async_`` replaces the previous ``async`` parameter, because ``async``
            was promoted to a keyword in Python 3.7. Users of Python versions
            prior to 3.7 can continue to use the ``async`` keyword.
        """

        # Process optional keyword arguments.
        if 'async' in kw:
            async_ = kw['async']

        if query is None:
            response, outfmt, preview = self.checkAsyncJob()

        else:
            try:
                response = queryClient.query(self.token,sql=query,fmt=self.mapping[outfmt][0],async_=async_)  # submit the query, using your authentication token
            except Exception as e:
                print(str(e))
                raise

        output = self._processOutput(response,outfmt,async_,preview)

        return output


    def clearQueue(self):

        """Clears the async job queue, i.e. they become unretrievable."""

        print("Clearing the queue of async queries.")
        self.openjobs.clear()


    def _processOutput(self,response,outfmt,async_,preview):

        """Process the responses returned by calls to
        :func:`queryClient.query()`, either directly or indirectly
        through calls to :func:`checkAsyncJob()`.

        Parameters
        ----------
        response : None or str
            If None, just return None, no processing. If str and
            async=None, this is hopefully a proper string-formatted
            response from a call to :func:`queryClient.query()`. If str
            and async=True, then response is the string-values async
            jobID.

        outfmt : str
            As in :func:`__init__()`.

        async_ : bool.
            As in :func:`__init__()`.

        preview : int
            As in :func:`__init__()`.

        """

        # response=None means checkAsyncJob() was called, but status
        # was not 'COMPLETED'. No processing in this case.
        if response is None:
            return response

        # response is not None...
        else:

            # ... and async is False, means response is the returned query result; process it
            if async_ is False:
                s = StringIO(response)
                output = self.mapping[outfmt][2](s)
                print("Returning %s" % self.mapping[outfmt][1])
                self._printPreview(response,preview)
                return output

            # ... and async is True, means the response is an async query jobID; put in in the FIFO queue
            elif async_ is True:
                self.openjobs.append((response,outfmt,preview))
                print("Asynchronous query submitted as jobid=%s" % response)
                print("Get results a bit later with: result = Q()")
                return None


    def checkAsyncJob(self):

        """Check the first async job in the FIFO queue (if queue is
        not empty).

        Parameters
        ----------
        None

        Returns
        -------
        Always returns a 3 tuple. If no async job was in the queue,
        returns (None,None,None). If there was an async query in the
        queue but its status did not return 'COMPLETED', re-inserts
        the query at its old position in the queue, and returns
        (None,None,None). If the status was 'COMPLETED', returns the
        tuple (query result,outfmt,preview).

        """

        try:
            jobid, outfmt, preview = self.openjobs.popleft()
            print("jobid, outfmt, preview", jobid, outfmt, preview)

        except IndexError:
            print("There are no pending async jobs.")
            return None, None, None

        except Exception as e:
            print(str(e))
            raise

        else:
            status = queryClient.status(self.token,jobid)

            if status in ('QUEUED','EXECUTING'):
                print("Async query job %s is currently %s. Please check a bit later with: result=Q()" % (jobid,status))
                self.openjobs.appendleft((jobid,outfmt,preview))  # putting back in queue (from left, i.e. old position)
                return None, None, None

            elif status == 'COMPLETED':
                print("Async query job %s is COMPLETED. Attempting to retrieve results." % jobid)
                response = queryClient.results(self.token,jobid)
                return response, outfmt, preview


    def _printPreview(self,response,preview):

        """Print to STDOUT `preview` number of lines from the string-valued
        `response` of a query.

        For many response formats this makes sense (e.g. CSV), for
        some less so (e.g. 'votable' because of the XML surrounding
        the result string).

        Parameters
        ----------
        response : str
            A string-valued result returned by the :mod:`queryClient`.

        preview : int
            As in :func:`__init__()`.

        Returns
        -------
        Nothing.

        """

        if response is not None:
            if preview > 0: # TODO: take a (large enough) heading sub-string of response, and count lines on that)
                print("RESULT PREVIEW (%d rows)" % preview)
                print(response[:response.replace('\n', '|', preview).find('\n')]) # print the response preview


    def printMapping(self):

        """Pretty-print to STDOUT the available `outfmt` values.

        Parameters
        ----------
        None

        Returns
        -------
        Nothing

        """

        length = max([len(s) for s in self.mapping.keys()]) + 1 # max length of any outfmt string, plus one
        fmt = "%%%ds   %%s" % length
        title = fmt % ("'outfmt' arg","Returned output")  # mini table header
        print(title)
        print('-'*len(title))
        for k,v in self.mapping.items():
            print(fmt % (k,v[1]))

    output_formats = property(printMapping)
