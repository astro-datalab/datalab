"""Helper classes and methods for datalab client."""

__authors__ = 'Robert Nikutta <nikutta@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20170606' # yyyymmdd

try:
    from cStringIO import StringIO   # python 2
    from Queue import deque
except ImportError:
    from io import StringIO          # python 3
    from queue import deque
    
# std lib imports
from functools import partial

from collections import OrderedDict
import getpass
import warnings
warnings.simplefilter('always', DeprecationWarning)

# 3rd party Python imports
import pylab as plt
import numpy as np
from pandas import read_csv
from astropy.table import Table
from astropy.io.votable import parse_single_table
from astropy.coordinates import SkyCoord
import astropy.units as u
from matplotlib.ticker import MaxNLocator

# Data Lab imports
from dl import authClient, queryClient


def convert(inp,outfmt='pandas'):

    """Convert input `inp` to a data structure defined by `outfmt`.

    Parameters
    ----------
    inp : str
        String representation of the result of a query. Usually this
        is a CSV-formatted string, but can also be, e.g. an
        XML-formatted votable (as string)

    outfmt : str
        The desired data structure for converting `inp` to. Default:
        'pandas', which returns a Pandas dataframe. Other available
        conversions are:

          string - no conversion
          array - Numpy array
          structarray - Numpy structured array (also called record array)
          table - Astropy Table
          votable - Astropy VOtable
    
        For outfmt='votable', the input string must be an
        XML-formatted string. For all other values, as CSV-formatted
        string.

    Example
    -------
    Convert a CSV-formatted string to a Pandas dataframe

    .. code-block:: python

       df = helpers.convert(inpst,outfmt='pandas')
       print df.head()  # df is as Pandas dataframe, with all its methods

    """
    
    # map outfmt container types to a tuple:
    # (:func:`queryClient.query()` fmt-value, descriptive title,
    # processing function for the result string)
    mapping = OrderedDict([
        ('string'      , ('csv',     'CSV formatted table as a string', lambda x: x.getvalue())),
        ('array'       , ('csv',     'Numpy array',                     partial(np.loadtxt,unpack=False,skiprows=1,delimiter=','))),
        ('structarray' , ('csv',     'Numpy structured / record array', partial(np.genfromtxt,dtype=float,delimiter=',',names=True))),
        ('pandas'      , ('csv',     'Pandas dataframe',                read_csv)),
        ('table'       , ('csv',     'Astropy Table',                   partial(Table.read,format='csv'))),
        ('votable'     , ('votable', 'Astropy VOtable',                 parse_single_table))
    ])

    s = StringIO(inp)
    output = mapping[outfmt][2](s)
    print ("Returning %s" % mapping[outfmt][1])

    return output


class Querist:

    def __init__(self,username='anonymous'):

        """Helper class to authenticate user with Data Lab, run queries, and
        convert results to the requested data type.
        
        Parameters
        ----------
        username: str
            User name, will be supplied to :func:`authClient.login()`
            to obtain an authentication token. The default username is
            'anonymous', which obtains an anonymous access token from
            :module:`authClient`.

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
#            print ("Enter password:")
            token = authClient.login(username,getpass.getpass(prompt='Enter password:'))

        if not authClient.isValidToken(token):
            raise Exception ("Invalid user name and/or password provided. Please try again.")
        else:
            print ("Authentication successful.")
            return token

        
    def clearToken(self):

        """Sets to token to empty string. Useful e.g. before saving a notebook."""

        self.token = ''

    
    def __call__(self,query=None,outfmt='array',preview=0,async=False):
        
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
                'string' (default) -- A table as comma-separated string.
                'array' -- Numpy array, with shape (ncols,nrows)
                'array' -- Numpy structured / record array, with shape (ncols,), and column names.
                'pandas' -- Pandas dataframe.
                'table' -- Astropy.table Table object.
                'votable' -- Astropy.io.votable. Note that this is much slower than e.g. 'pandas' or 'array'.
            
        preview : int
            Number of lines to preview on STDOUT. This does not count
            the header line. If `outfmt='votable'`, `preview` is not
            very useful, because of the XML that votable carries
            around. Default: 0
            
        async : bool
            If False (default), submit queries in sync mode,
            i.e. expecting results immediately.

            If True, submit query in async mode, storing the jobid in
            a FIFO queue (first-in-first-out). A subsequent call
            without arguments will attempt to retrieve the query
            result. If the query status is not yet COMPLETED, the
            jobid is re-inserted into the queue (at old position), and
            the user is instructed to try later.

            Not yet implemented: automatic re-submission of a sync
            query in async mode, if the queryManager / DB raise
            Exception that "the query not suitable for sync mode".

        """

        if query is None:
            response, outfmt, preview = self.checkAsyncJob()

        else:
            try:
                response = queryClient.query(self.token,sql=query,fmt=self.mapping[outfmt][0],async=async)  # submit the query, using your authentication token
            except Exception as e:
                print (str(e))
                raise

        output = self._processOutput(response,outfmt,async,preview)
        
        return output


    def clearQueue(self):

        """Clears the async job queue, i.e. they become unretrievable."""
        
        print ("Clearing the queue of async queries.")
        self.openjobs.clear()

        
    def _processOutput(self,response,outfmt,async,preview):

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

        async : bool.
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
            if async is False:
                s = StringIO(response)
                output = self.mapping[outfmt][2](s)
                print ("Returning %s" % self.mapping[outfmt][1])
                self._printPreview(response,preview)
                return output

            # ... and async is True, means the response is an async query jobID; put in in the FIFO queue
            elif async is True:
                self.openjobs.append((response,outfmt,preview))
                print ("Asynchronous query submitted as jobid=%s" % response)
                print ("Get results a bit later with: result = Q()")
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
            print ("jobid, outfmt, preview", jobid, outfmt, preview)
            
        except IndexError:
            print ("There are no pending async jobs.")
            return None, None, None

        except Exception as e:
            print (str(e))
            raise

        else:
            status = queryClient.status(self.token,jobid)
            
            if status in ('QUEUED','EXECUTING'):
                print ("Async query job %s is currently %s. Please check a bit later with: result=Q()" % (jobid,status))
                self.openjobs.appendleft((jobid,outfmt,preview))  # putting back in queue (from left, i.e. old position)
                return None, None, None

            elif status == 'COMPLETED':
                print ("Async query job %s is COMPLETED. Attempting to retrieve results." % jobid)
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
                print ("RESULT PREVIEW (%d rows)" % preview)
                print (response[:response.replace('\n', '|', preview).find('\n')]) # print the response preview

                
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
        print (title)
        print ('-'*len(title))
        for k,v in self.mapping.items():
            print (fmt % (k,v[1]))

    output_formats = property(printMapping)
        

def normalizeCoordinates(x,y,frame_in='icrs',units_in='deg',frame_out=None,wrap_at=180):

    """Makes 2D spatial coordinates (e.g. RA & Dec) suitable for use with
    matplotlib's all-sky projection plotting.

    Parameters
    ----------
    x, y : seq (e.g. tuple,list,1-d array)
        Location of points in (x,y) feature space (e,g, RA & Dec in
        degrees). Avoid supplying x and y as columns from a pandas
        dataframe, as this unfortunately makes the coordinate
        conversions much slower. Numpy arrays, lists, astropy table
        and votable columns, all are fine.

    frame_in : str
        Coordinate frame of x & y. Default: 'icrs'. 'galactic' is also
        available. If the user desires other frames from
        :mod:`astropy.coordinates`, please contact __author__.

    units_in : str
        Units of x & y. Default 'deg' (degrees).

    frame_out : None or str
        If not None, and not same as frame_in, the x & y coordinates
        will be transformed from frame_in to frame_out.

    wrap_at : float
        :mod:`matplotlib` plotting functions such as
        :func:`matplotlib.scatter()` with all-sky projections expect
        the x-coordinate (e.g. RA) to be between -180 and +180 degrees
        (or more precisely: between -pi and +pi). The default
        wrap_at=180 shifts the input coordinate x (e.g. RA)
        accordingly.

    """

    # currently available values for frame_in
    mapping = {'galactic': ('l','b'), 'icrs': ('ra','dec')}

    frame = frame_in

    uin = getattr(u,units_in)  # input coordinate units as astropy.units
    c = SkyCoord(x*uin,y*uin,frame=frame) # convenience coordinate object handler. Avoid fee

    # transform coordinate system if necessary
    if frame_out is not None and frame_out != frame_in:
        c = c.transform_to(frame_out)
        frame = frame_out

    # convert x & y to radian, x possibly wrapped at wrap_at value
    xout = getattr(c,mapping[frame][0]).wrap_at(wrap_at*uin).radian
    yout = getattr(c,mapping[frame][1]).radian

    return xout, yout
                 

def findClusters(x,y,method='MiniBatchKMeans',**kwargs):  # x,y can be for instance ra,dec

    """Find 2D clusters from x & y data.

    Parameters
    ----------
    x, y : seq (e.g. tuple,list,1-d array)
        Location of points in (x,y) feature space, e,g, RA & Dec, but
        x & y need not be spatial in nature.

    method : str
        Cluster finding method from :mod:`sklearn.cluster` to
        use. Default: 'MiniBatchKMeans' (a streaming implementation of
        KMeans), which is very fast, but not the most robust. 'DBSCAN'
        is much more robust, but MUCH slower. For other methods,
        consult :mod:`sklearn.cluster`.

    **kwargs
        Any other keyword arguments will be passed to the cluster
        finding method. If method='MiniBatchKMeans' or 'KMeans',
        n_clusters (integer number of clusters to find) must be
        passed, e.g.

        .. code-block:: python

           clusters = findClusters(x,y,method='MiniBatchKMeans',n_clusters=3)

    """

    import sklearn.cluster as C #import KMeans, MiniBatchKMeans, DBSCAN

    if method in ('MiniBatchKMeans','KMeans'):
        if 'n_clusters' not in kwargs:
            kwargs['n_clusters'] = 3
            
    try:
        METHOD = getattr(C,method)(**kwargs)
    except Exception as e:
        print (str(e))
        raise

    X = np.matrix(zip(x,y))
    
    clusters = METHOD.fit(X)

    return clusters


def constructOutlines(x,y,clusterlabels):  # compute convex hull, one per cluster label

    """Construct the convex hull (outline) of points in (x,y) feature space,

    Parameters
    ----------
    x, y : seq (e.g. tuple,list,1-d array)
        Location of points in (x,y) feature space (e,g, RA & Dec).

    Returns
    -------
    hull : instance
        The convex hull of points (x,y), an instance of
        :class:`scipy.spatial.qhull.ConvexHull`.

    Example
    -------
    Given `x` & `y` coordinates as 1d sequences:

    .. code-block:: python

       points = np.vstack((x,y)).T  # make 2-d array of correct shape
       hull = constructOutlines(x,y)
       plt.plot(points[hull.vertices,0], points[hull.vertices,1], 'r-', lw=2) # plot the hull
       plt.plot(points[hull.vertices[0],0], points[hull.vertices[0],1], 'r-') # closing last point of the hull

    """

    from scipy.spatial import ConvexHull
    points = np.vstack((x,y)).T
    hull = ConvexHull(points)
        
    return hull


def plotSkymapScatter(x,y,c=None,clusterlabels=None,s=3,plot='both',xlabel='RA',ylabel='Dec',clabel='',title='',projection='aitoff',**kwargs):

    """Plot an all-sky projection of data x,y.

    Parameters
    ----------
    x, y : sequences
        1-d sequence of data (both same length), typically RA & Dec,
        to plotted via scatter onto an all-sky projection. If `c=None`
        and `clusterlabels=None`, the plot is a simple scatter plot.

    c : seq or None
        If not None, `c` is a 1-d sequence of the same length as `x` &
        `y`, and will be used as the value for the colormap applied to
        the datapoints (x,y). If c is not None, a colorbar will be
        plotted alongside the sky map.

    clusterlabels : seq or None
        If not None, `clusterlabels` is a 1-d sequence of same length
        as `x` & `y`, and carries 'label' values for each (x,y) pair
        designating that datapoint as member of some cluster or
        class. In this case, all scatter points with the same value in
        `clusterlabels` will be plotted in the same color.

    s : float
        Marker size for scatter plot. Will be passed to matplotlib's
        :func:`scatter()`. Default: s=3.

    plot : str
        Either 'both' (default) or 'scatter' or 'outlines'. If
        clusterlabels is not none, then plot='outlines' or plot='both'
        triggers the computation of outlines (as convex hulls) around
        all clusters identified by the labels in `clusterlabels`. If
        plot='both', the scatter points and the outlines will be
        plotted. If plot='outlines', only the outlines will be
        plotted.

    xlabel, ylabel : str
        x and y labels to be plotted. Defaults: 'RA' and 'Dec'. To
        turn off labels, supply ''.

    clabel : str
        Colorbar label (if c is not None).

    title : str
        Figure title. Default ''.

    projection : str
        Projection of the all-sky map. Default: 'aitoff'. For other
        options, check:

        .. code-block:: python

           from matplotlib import projections
           projections.get_projection_names()
              [u'aitoff', u'hammer', u'lambert', u'mollweide', u'polar', u'rectilinear']

    **kwargs : keyword arguments
        All kwargs will be passed on to pylab.scatter().

    """

    plt.figure(figsize=(14,7))
    ax = plt.subplot(111, projection=projection)

    if clusterlabels is not None:

        if plot in ('scatter','both'):
            for label in np.unique(clusterlabels):
                sel = (clusterlabels == label)
                im = plt.scatter(x[sel], y[sel], marker='o', s=s, edgecolors='none', alpha=1, label=label, **kwargs)

            plt.legend(loc='upper right',title='clusters',markerscale=5)

        if plot in ('outlines','both'):
            for label in np.unique(clusterlabels):
                sel = (clusterlabels == label)
                x_ = x[sel]
                y_ = y[sel]
                points = np.vstack((x_,y_)).T
                hull = constructOutlines(x_,y_,clusterlabels)
                plt.plot(points[hull.vertices,0], points[hull.vertices,1], 'r-', lw=2) # plot the hull
                plt.plot(points[hull.vertices[0],0], points[hull.vertices[0],1], 'r-') # closing last point of the hull

    else:
        im = plt.scatter(x, y, marker='o', s=s, c=c, edgecolors='none', alpha=1, **kwargs)

        if c is not None:
            cb = plt.colorbar(im)
            cb.set_label(clabel)

    plt.title(title,y=1.08)
    plt.grid(True)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
