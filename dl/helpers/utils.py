"""Data Lab utility helper functions."""

# std lib
from __future__ import print_function

__authors__ = 'Robert Nikutta <nikutta@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20171219' # yyyymmdd

from functools import partial

try:
    from cStringIO import StringIO   # python 2
except ImportError:
    from io import StringIO          # python 3

# 3rd party
from collections import OrderedDict
import numpy as np
from pandas import read_csv
from astropy.table import Table
from astropy.io.votable import parse_single_table
from astropy.coordinates import SkyCoord
import astropy.units as u


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
