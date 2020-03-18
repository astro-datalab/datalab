"""Data Lab utility helper functions."""

__authors__ = 'Robert Nikutta <nikutta@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20200318' # yyyymmdd

# std lib
import time
from functools import partial
from io import BytesIO
from contextlib import contextmanager

# 3rd party
from collections import OrderedDict
import numpy as np
from pandas import read_csv
from astropy.table import Table
from astropy.io.votable import parse_single_table
from astropy.coordinates import SkyCoord, name_resolve
from astropy.utils.data import get_readable_fileobj
import astropy.units as u

# Turn off some annoying astropy warnings
import warnings
from astropy.utils.exceptions import AstropyWarning
warnings.simplefilter('ignore', AstropyWarning)

from .. import storeClient
from .. import authClient
#from .. import queryClient


def async_query_status(jobid,wait=3):

    """Loop until an async job has completed.
    
    Parameters
    ----------
    jobid : str
        The job ID string of a submitted query job.
        
    wait : int | float
        Wait for `wait` seconds before checking status again. Default: 3 (seconds)
    """
    
    print("Checking status of async job '%s' " % jobid)
    while True:
        status = queryClient.status(jobid)
        print(status)
        if status in ('QUEUED','EXECUTING'):
            print('Waiting %g seconds...' % wait)
            time.sleep(wait)
        elif status == 'COMPLETED':
            print('Async job finished. Retrieve rows with:\n  result = queryClient.results(jobid)')
            break
        else:
            raise Exception('Async query error.',status)


def resolve(name=None):

    """Resolve object name to coordinates.

    Parameters
    ----------
    name : str or None
        If str, it is the name of the object to resolve. If None
        (default), a primpt for the object name will be presented.

    Returns
    -------
    sc : instance
        Instance of SkyCoord from astropy. Get e.g. RA via sc.ra (with
        units), or sc.ra.value (without units). Or explictly in a
        different coordinate system, e.g. sc.galactic.b, etc.

    """

    if name is None:
        name = input("Object name (+ENTER): ")

    try:
        coords = name_resolve.get_icrs_coordinates(name)
    except Exception as e:
        raise

    return coords


def convert(inp,outfmt='pandas',verbose=False,**kwargs):

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

    verbose : bool
        If True, print status message after conversion. Default: False

    kwargs : optional params
        Will be passed as **kwargs to the converter method.


    Example
    -------
    Convert a CSV-formatted string to a Pandas dataframe

    .. code-block:: python

       arr = convert(inp,'array')
       arr.shape  # arr is a Numpy array

       df = convert(inp,outfmt='pandas')
       df.head()  # df is as Pandas dataframe, with all its methods

       df = convert(inp,'pandas',na_values='Infinity') # na_values is a kwarg; adds 'Infinity' to list of values converter to np.inf

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

    if isinstance(inp,bytes):
        b = BytesIO(inp)
    elif isinstance(inp,str):
        b = BytesIO(inp.encode())
    else:
        raise TypeError('Input must be of bytes or str type.')

    output = mapping[outfmt][2](b,**kwargs)

    if isinstance(output,bytes):
        output = output.decode()

    if verbose:
        print("Returning %s" % mapping[outfmt][1])

    return output


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


@contextmanager
def vospace_readable_fileobj(name_or_obj, token=None, **kwargs):
    """Read data from VOSpace or some other place.

    Notes
    -----
    Most of the heavy lifting is done with
    :func:`~astropy.io.data.get_readable_fileobj`.  Any additional keywords
    passed to this function will get passed directly to that function.

    Parameters
    ----------
    name_or_obj : :class:`str` or file-like object
        The filename of the file to access (if given as a string), or
        the file-like object to access.

        If a file-like object, it must be opened in binary mode.

    token : :class:`str`
        A token granting access to VOSpace.

    Returns
    -------
    file
        A readable file-like object.
    """
    fileobj = name_or_obj
    close_fileobj = False
    if (isinstance(name_or_obj, (str,unicode)) and name_or_obj.find('://') > 0):
        uri = name_or_obj[:name_or_obj.find('://')]
        if authClient.isValidUser(uri):
            # VOSpace call
            fileobj = BytesIO(storeClient.get(name_or_obj, mode='binary'))
            close_fileobj = True

    with get_readable_fileobj(fileobj, **kwargs) as f:
        try:
            yield f
        finally:
            if close_fileobj:
                fileobj.close()
