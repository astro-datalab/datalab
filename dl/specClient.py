#!/usr/bin/env python
#
# SPECCLIENT -- Client methods for the Spectroscopic Data Service
#

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>'
__version__ = 'v1.2.0'


'''
    Client methods for the Spectroscopic Data Service.

    Spectro Client Interface
    ------------------------

        client = getClient  (context='<context>', profile='<profile>')

          status = isAlive  (svc_url=DEF_SERVICE_URL, timeout=2)

               set_svc_url  (svc_url)
     svc_url = get_svc_url  ()

               set_context  (context)
         ctx = get_context  ()
      ctxs = list_contexts  (context, fmt='text')
      ctxs = list_contexts  (context=None, fmt='text')

               set_profile  (profile)
        prof = get_profile  ()
     profs = list_profiles  (profile, fmt='text')
     profs = list_profiles  (profile=None, fmt='text')

       catalogs = catalogs  (context='default', profile='default')

    QUERY INTERFACE:
            id_list = query (<region> | <coord, size> | <ra, dec, size>,
                             constraint=<sql_where_clause>,
                             context=None, profile=None, **kw)
    ACCESS INTERFACE:
            list = getSpec  (id_list, fmt='numpy',
                             out=None, align=False, cutout=None,
                             context=None, profile=None, **kw)
    PLOT  INTERFACE:
                      plot  (spec, context=None, profile=None, out=None, **kw)
         status = prospect  (spec, context=context, profile=profile, **kw)
           image = preview  (id, context=context, profile=profile, **kw)
          image = plotGrid  (id_list, nx, ny, page=<N>,
                             context=context, profile=profile, **kw)
      image = stackedImage  (id_list, fmt='png|numpy',
                             align=False, yflip=False,
                             context=context, profile=profile, **kw)
    UTILITY METHODS:
            df = to_pandas  (npy_data)
    spec1d = to_Spectrum1D  (npy_data)
            tab = to_Table  (npy_data)

Import via

.. code-block:: python

    from dl import specClient
'''

import os
import sys
import socket
import json
import numpy as np
import pandas as pd
from io import BytesIO

from PIL import Image

# Turn off some annoying astropy warnings

import warnings
from astropy.utils.exceptions import AstropyWarning
warnings.simplefilter('ignore', AstropyWarning)

import logging
logging.disable(logging.WARNING)
logging.getLogger("specutils").setLevel(logging.CRITICAL)
from specutils import Spectrum1D
from specutils import SpectrumCollection

from astropy import units as u
#from astropy.nddata import StdDevUncertainty
from astropy.nddata import InverseVariance
from astropy.table import Table
from matplotlib import pyplot as plt      	# visualization libs

try:
    import pycurl_requests as requests		# faster 'requests' lib
except ImportError:
    import requests				# fall-back 'requests' lib
import pycurl					# low-level interface
from urllib.parse import quote_plus		# URL encoding

# Data Lab imports.
#from dl import queryClient
from dl import storeClient
from dl.Util import def_token
from dl.Util import multimethod
from dl.helpers.utils import convert


# Python version check.
is_py3 = sys.version_info.major == 3


# The URL of the service to access.  This may be changed by passing a new
# URL into the set_svc_url() method before beginning.
DEF_SERVICE_ROOT = "https://datalab.noao.edu"

# Allow the service URL for dev/test systems to override the default.
THIS_HOST = socket.gethostname()
if THIS_HOST[:5] == 'dldev':
    DEF_SERVICE_ROOT = "http://dldev.datalab.noao.edu"
elif THIS_HOST[:6] == 'dltest':
    DEF_SERVICE_ROOT = "http://dltest.datalab.noao.edu"
#elif THIS_HOST[:5] == 'munch':                          # DELETE ME
#    DEF_SERVICE_ROOT = "http://localhost:6998"          # DELETE ME


# Allow the service URL for dev/test systems to override the default.
sock = socket.socket(type=socket.SOCK_DGRAM)     # host IP address
sock.connect(('8.8.8.8', 1))        # Example IP address, see RFC 5737
THIS_IP, _ = sock.getsockname()

DEF_SERVICE_URL = DEF_SERVICE_ROOT + "/spec"
SM_SERVICE_URL = DEF_SERVICE_ROOT + "/storage"
QM_SERVICE_URL = DEF_SERVICE_ROOT + "/query"

# Use cURL for requests when possible.
USE_CURL = True

# The requested service "profile".  A profile refers to the specific
# machines and services used by the service.

DEF_SERVICE_PROFILE = "default"

# The requested dataset "context". A context refers to the specific dataset
# being served.  This determines what is allowed within certain methods.

DEF_SERVICE_CONTEXT = "default"

# Use a /tmp/AM_DEBUG file as a way to turn on debugging in the client code.
DEBUG = os.path.isfile('/tmp/SPEC_DEBUG')
VERBOSE = os.path.isfile('/tmp/SPEC_VERBOSE')


# ######################################################################
#
#  Spectroscopic Data Client Interface
#
#  This API provides convenience methods that allow an application to
#  import the Client class without having to explicitly instantiate a
#  class object.  The parameter descriptions and example usage is given
#  in the comments for the class methods.  Module methods have their
#  docstrings patched below.
#
# ######################################################################


# ###################################
#  Spectroscopic Data error class
# ###################################

class dlSpecError(Exception):
    '''A throwable error class.
    '''
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


# ###################################
#  Py2/Py3 Compatability Utilities
# ###################################

def spcToString(s):
    '''spcToString -- Force a return value to be type 'string' for all
                      Python versions.
    '''
    if is_py3:
        if isinstance(s, bytes):
            strval = str(s.decode())
        elif isinstance(s, str):
            strval = s
    else:
        if isinstance(s, bytes) or isinstance(s, unicode):
            strval = str(s)
        else:
            strval = s

    return strval


# -----------------------------
#  Utility Methods
# -----------------------------

# --------------------------------------------------------------------
# SET_SVC_URL -- Set the ServiceURL to call.
#
def set_svc_url(svc_url):

    return sp_client.set_svc_url(svc_url.strip('/'))


# --------------------------------------------------------------------
# GET_SVC_URL -- Get the ServiceURL to call.
#
def get_svc_url():

    return sp_client.get_svc_url()


# --------------------------------------------------------------------
# SET_PROFILE -- Set the service profile to use.
#
def set_profile(profile):

    return sp_client.set_profile(profile)


# --------------------------------------------------------------------
# GET_PROFILE -- Get the service profile to use.
#
def get_profile():

    return sp_client.get_profile()


# --------------------------------------------------------------------
# SET_CONTEXT -- Set the dataset context to use.
#
def set_context(context):

    return sp_client.set_context(context)


# --------------------------------------------------------------------
# GET_CONTEXT -- Get the dataset context to use.
#
def get_context():

    return sp_client.get_context()


# --------------------------------------------------------------------
# ISALIVE -- Ping the service to see if it responds.
#
def isAlive(svc_url=DEF_SERVICE_URL, timeout=5):

    return sp_client.isAlive(svc_url=svc_url, timeout=timeout)


# --------------------------------------------------------------------
# LIST_PROFILES -- List the available service profiles.
#
@multimethod('spc', 1, False)
def list_profiles(profile, fmt='text'):

    return sp_client._list_profiles(profile=profile, fmt=fmt)


@multimethod('spc', 0, False)
def list_profiles(profile=None, fmt='text'):

    '''Retrieve the profiles supported by the spectro data service.

    Usage:
        list_profiles ([profile], fmt='text')

    MultiMethod Usage:
    ------------------
            specClient.list_profiles (profile)
            specClient.list_profiles ()

    Parameters
    ----------
    profile: str
        A specific profile configuration to list. If None, a list of
        available profiles is returned.

    format: str
        Result format: One of 'text' or 'json'

    Returns
    -------
    profiles: list/dict
        A list of the names of the supported profiles or a dictionary of
        the specific profile

    Example
    -------
    .. code-block:: python

        profiles = specClient.list_profiles(profile)
        profiles = specClient.list_profiles()
    '''
    return sp_client._list_profiles(profile=profile, fmt=fmt)


# --------------------------------------------------------------------
# LIST_CONTEXTS -- List the available dataset contexts.
#
@multimethod('spc',1,False)
def list_contexts(context, fmt='text'):
    return sp_client._list_contexts(context=context, fmt=fmt)


@multimethod('spc',0,False)
def list_contexts(context=None, fmt='text'):
    '''Retrieve the contexts supported by the spectro data service.

    Usage:
        list_contexts ([context], fmt='text')

    MultiMethod Usage:
    ------------------
            specClient.list_contexts (context)
            specClient.list_contexts ()

    Parameters
    ----------
    contexts: str
        A specific contexts configuration to list.  If None, a list of
        available contexts is returned.

    format: str
        Result format: One of 'text' or 'json'

    Returns
    -------
    contexts: list/dict
        A list of the names of the supported contexts or a dictionary of
        the specific contexts

    Example
    -------
    .. code-block:: python

        contexts = specClient.list_contexts(context)
        contexts = specClient.list_contexts()
    '''
    return sp_client._list_contexts(context=context, fmt=fmt)


# --------------------------------------------------------------------
# CATALOGS -- List available catalogs for a given dataset context
#
def catalogs(context='default', profile='default', fmt='text'):
    '''List available catalogs for a given dataset context
    '''
    return sp_client.catalogs(context=context, profile=profile, fmt=fmt)


# --------------------------------------------------------------------
# TO_SPECTRUM1D -- Utility method to convert a Numpy array to Spectrum1D
#
def to_Spectrum1D(npy_data):
    '''Utility method to convert a Numpy array to Spectrum1D
    '''
    return sp_client.to_Spectrum1D(npy_data)


# --------------------------------------------------------------------
# TO_PANDAS -- Utility method to convert a Numpy array to a Pandas DataFrame
#
def to_pandas(npy_data):
    '''Utility method to convert a Numpy array to a Pandas DataFrame
    '''
    return sp_client.to_pandas(npy_data)


# --------------------------------------------------------------------
# TO_TABLE -- Utility method to convert a Numpy array to an Astropy Table
#
def to_Table(npy_data):
    '''Utility method to convert a Numpy array to an Astropy Table object.
    '''
    return sp_client.to_Table(npy_data)




#######################################
# Spectroscopic Data Client Methods
#######################################

# --------------------------------------------------------------------
# QUERY -- Query for spectra by position.
#
@multimethod('spc',3,False)
def query(ra, dec, size, constraint=None, out=None,
          context=None, profile=None, **kw):

    return sp_client._query(ra=ra, dec=dec, size=size,
                            pos=None,
                            region=None,
                            constraint=constraint,
                            out=out,
                            context=context, profile=profile, **kw)


@multimethod('spc',2,False)
def query(pos, size, constraint=None, out=None,
          context=None, profile=None, **kw):

    return sp_client._query(ra=None, dec=None, size=size,
                            pos=pos,
                            region=None,
                            constraint=constraint,
                            out=out,
                            context=context, profile=profile, **kw)


@multimethod('spc',1,False)
def query(region, constraint=None, out=None,
          context=None, profile=None, **kw):

    return sp_client._query(ra=None, dec=None, size=None,
                            pos=None,
                            region=region,
                            constraint=constraint,
                            out=out,
                            context=context, profile=profile, **kw)


@multimethod('spc',0,False)
def query(constraint=None, out=None, context=None, profile=None, **kw):

    '''Query for a list of spectrum IDs that can then be retrieved from
        the service.

    Usage:
        id_list = query(ra, dec, size, constraint=None, out=None,
                        context=None, profile=None, **kw)
        id_list = query(pos, size, constraint=None, out=None,
                        context=None, profile=None, **kw)
        id_list = query(region, constraint=None, out=None,
                        context=None, profile=None, **kw)
        id_list = query(constraint=None, out=None,
                        context=None, profile=None, **kw)

    Parameters
    ----------
    ra: float
        RA search center specified in degrees.

    dec: float
        Dec of search center specified in degrees.

    size: float
        Size of search center specified in degrees.

    pos: Astropy Coord object
        Coordinate of search center specified as an Astropy Coord object.

    region: float
        Array of polygon vertices (in degrees) defining a search region.

    constraint: str
        A valid SQL syntax that can be used as a WHERE constraint in the
        search query.

    out: str
        Output filename to create.  If None or an empty string the query
        results are returned directly to the client.  Otherwise, results
        are writeen to the named file with one identifier per line.  A
        Data Lab 'vos://' prefix will save results to the named virtual
        storage file.

    context: str
        Dataset context.

    profile: str
        Data service profile.

    **kw: dict
        Optional keyword arguments.  Supported keywords currently include:

        For context='sdss_dr16' | 'default':
           fields:
               specobjid           # or 'bestobjid', etc
               tuple               # a plate/mjd/fiber tuple

               Service will always return array of 'specobjid'
               value, the p/m/f tuple is extracted from the
               bitmask value by the client.

           catalog:
               <schema>.<table>    # alternative catalog to query e.g. a
                                   # VAC from earlier DR (must support an
                                   # ra/dec search and return a specobjid-
                                   # like value)
        For all contexts:
           verbose = False
               Print verbose messages during retrieval
           debug = False
               Print debug messages during retrieval

    Returns
    -------
    result: array
        An array of spectrum IDs appropriate for the dataset context.

    Example
    -------
       1) Query by position:

        .. code-block:: python
            id_list = spec.query (0.125, 12.123, 0.1)
    '''
    return sp_client._query(ra=None, dec=None, size=None,
                            pos=None,
                            region=None,
                            constraint=constraint,
                            out=out,
                            context=context, profile=profile, **kw)


# --------------------------------------------------------------------
# GETSPEC -- Retrieve spectra for a list of objects.
#
def getSpec(id_list, fmt='numpy', out=None, align=False, cutout=None,
            context=None, profile=None, **kw):
    '''Get spectra for a list of object IDs.

    Usage:
        getSpec(id_list, fmt='numpy', out=None, align=False, cutout=None,
                context=None, profile=None, **kw)

    Parameters
    ----------
    id_list: list object
        List of object identifiers.

    fmt: str
        Return format of spectra

    out:
        Output file or return to caller if None

    align:
        Align spectra to common wavelength grid with zero-padding

    cutout:
        Wavelength cutout range (as "<start>-<end>")

    context: str
        Dataset context.

    profile: str
        Data service profile.

    **kw: dict
        Optional keyword arguments.  Supported keywords currently include:

           values = None
               Spectrum vectors to return.
           token = None
               Data Lab auth token.
           id_col = None
               Name of ID column in input table.
           verbose = False
               Print verbose messages during retrieval
           debug = False
               Print debug messages during retrieval

    Returns
    -------
    result: object or array of objects or 'OK' string

    Example
    -------
       1) Retrieve spectra individually:

        .. code-block:: python
            id_list = spec.query (0.125, 12.123, 0.1)
            for id in id_list:
                spec = spec.getSpec (id)
                .... do something

        2) Retrieve spectra in bulk:

        .. code-block:: python
            spec = spec.getSpec (id_list, fmt='numpy')
            .... 'spec' is an array of NumPy objects that may be
                 different sizes
    '''
    return sp_client.getSpec(id_list=id_list, fmt=fmt, out=out,
                             align=align, cutout=cutout,
                             context=context, profile=profile, **kw)


# --------------------------------------------------------------------
# PLOT -- Utility to batch plot a single spectrum, display plot directly.
#
def plot(spec, context=None, profile=None, out=None, **kw):
    '''Utility to batch plot a single spectrum.

    Usage:
        spec.plot(id, context=None, profile=None, **kw)

    Parameters
    ----------
    spec: object ID or data array
        Spectrum to be plotted.  If 'spec' is a numpy array or a
        Spectrum1D object the data are plotted directly, otherwise
        the value is assumed to be an object ID that will be retrieved
        from the service.

    out: str
        Output filename.  If specified, plot saved as PNG.

    context: str
        Dataset context.

    profile: str
        Data service profile.

    **kw: dict
        Optional keyword arguments.  Supported keywords currently include:

            rest_frame - Whether or not to plot the spectra in the
                         rest-frame  (def: True)
                     z - Redshift value
                  xlim - Set the xrange of the plot
                  ylim - Set the yrange of the plot

                values - A comma-delimited string of which values to plot,
                         a combination of 'flux,model,sky,ivar'
            mark_lines - Which lines to mark.  No lines marked if None or
                         an empty string, otherwise one of 'em|abs|all|both'
                  grid - Plot grid lines (def: True)
                  dark - Dark-mode plot colors (def: True)
              em_lines - List of emission lines to plot.  If not given,
                         all the lines in the default list will be plotted.
             abs_lines - Lines of absorption lines to plot.  If not given,
                         all the lines in the default list will be plotted.
             spec_args - Plotting kwargs for the spectrum
            model_args - Plotting kwargs for the model
             ivar_args - Plotting kwargs for the ivar
              sky_args - Plotting kwargs for the sky

    Returns
    -------
        Nothing

    Example
    -------
       1) Plot a single spectrum, save to a virtual storage file

        .. code-block:: python
            spec.plot (specID, context='sdss_dr16', out='vos://spec.png')

    '''
    return sp_client.plot(spec, context=context, profile=profile,
                          out=None, **kw)


# --------------------------------------------------------------------
# PROSPECT -- Utility wrapper to launch the interactive PROSPECT tool.
#
def prospect(spec, context=None, profile=None, **kw):
    '''Utility wrapper to launch the interactive PROSPECT tool.

    Usage:
        stat =  prospect(spec, context=None, profile=None, **kw)

    Parameters
    ----------
    spec: object ID or data array
        Spectrum to be plotted.  If 'spec' is a numpy array or a
        Spectrum1D object the data are plotted directly, otherwise
        the value is assumed to be an object ID that will be retrieved
        from the service.

    context: str
        Dataset context.

    profile: str
        Data service profile.

    **kw: dict
        Optional keyword arguments.  Supported keywords currently include:

           TBD

    Returns
    -------
    result: str
        Status 'OK' string or error message.

    Example
    -------
       1) Plot ....

        .. code-block:: python
            stat = spec.prospect (specID)

    '''
    pass


# --------------------------------------------------------------------
# PREVIEW -- Get a preview plot of a spectrum
#
def preview(spec, context=None, profile=None, **kw):
    '''Get a preview plot of a spectrum

    Usage:
        spec.preview(spec, context=None, profile=None, **kw):

    Parameters
    ----------
    id_list: list object
        List of object identifiers.

    context: str
        Dataset context.

    profile: str
        Data service profile.

    **kw: dict
        Optional keyword arguments.  Supported keywords currently include:

           N/A

    Returns
    -------
    image: A PNG image object

    Example
    -------
       1) Display a preview plot a given spectrum:

        .. code-block:: python
            from IPython.display import display, Image
            display(Image(spec.preview(id),
                    format='png', width=400, height=100, unconfined=True))
    '''
    pass
    return sp_client.preview(spec, context=context, profile=profile, **kw)


# --------------------------------------------------------------------
# PLOTGRID -- Get a grid of preview plots of a spectrum list.
#
def plotGrid(id_list, nx, ny, page=0, context=None, profile=None, **kw):
    '''Get a grid of preview plots of a spectrum list.

    Usage:
        image = plotGrid(id_list, nx, ny, page=0,
                         context=None, profile=None, **kw):

    Parameters
    ----------
    id_list: list object
        List of object identifiers.

    nx: int
        Number of plots in the X dimension

    ny: int
        Number of plots in the Y dimension

    page: int
        Dataset context.

    context: str
        Dataset context.

    profile: str
        Data service profile.

    **kw: dict
        Optional keyword arguments.  Supported keywords currently include:

           verbose = False
               Print verbose messages during retrieval
           debug = False
               Print debug messages during retrieval

    Returns
    -------
    image: A PNG image object

    Example
    -------
       1) Display a 5x5 grid of preview plots for a list:

        .. code-block:: python
            npages = np.round((len(id_list) / 25) + (25 / len(id_list))
            for pg in range(npages):
                data = spec.getGridPlot(id_list, 5, 5, page=pg)
                display(Image(data, format='png',
                        width=400, height=100, unconfined=True))
    '''
    return sp_client.plotGrid(id_list, nx, ny, page=page,
                              context=context, profile=profile, **kw)


# --------------------------------------------------------------------
# STACKEDIMAGE -- Get a stacked image of a list of spectra.
#
def stackedImage(id_list, align=False, yflip=False,
                 context=None, profile=None, **kw):
    '''Get ...

    Usage:

    Parameters
    ----------
    id_list: list object
        List of spectrum identifiers.

    context: str
        Dataset context.

    profile: str
        Data service profile.

    **kw: dict
        Optional keyword arguments.  Supported keywords currently include:

           verbose = False
               Print verbose messages during retrieval
           debug = False
               Print debug messages during retrieval

    Returns
    -------
    result: ....

    Example
    -------
       1) Query ....

        .. code-block:: python
            id_list = spec.query (0.125, 12.123, 0.1)

    '''
    pass
    return sp_client.stackedImage(id_list, align=align, yflip=yflip,
                                  context=context, profile=profile, **kw)


#######################################
# Spectroscopic Data Client Class
#######################################

class specClient(object):
    '''
         SPECCLIENT -- Client-side methods to access the Data Lab
                       Spectroscopic Data Service.
    '''

    def __init__(self, context='default', profile='default'):

        '''Initialize the specClient class.
        '''
        self.svc_url = DEF_SERVICE_URL          # service URL
        self.qm_svc_url = QM_SERVICE_URL        # Query Manager service URL
        self.sm_svc_url = SM_SERVICE_URL        # Storage Manager service URL
        self.auth_token = def_token(None)       # default auth token (not used)
        self.svc_profile = profile              # service profile
        self.svc_context = context              # dataset context

        self.hostip = THIS_IP
        self.hostname = THIS_HOST
        self.debug = DEBUG                      # interface debug flag
        self.verbose = VERBOSE                  # interface verbose flag

        # Get the server-side config for the context.  Note this must also
        # be updated whenever we do a set_svc_url() or set_context().
        self.context = self._list_contexts(context)


    # Standard Data Lab service methods.
    #
    def set_svc_url(self, svc_url):

        '''Set the URL of the Spectroscopic Data Service to be used.

        Parameters
        ----------
        svc_url: str
            Spectroscopic Data service base URL to call.

        Returns
        -------
        Nothing

        Example
        -------
        .. code-block:: python

            from dl import specClient
            specClient.set_svc_url("http://localhost:7001/")
        '''
        self.svc_url = spcToString(svc_url.strip('/'))
        self.context = self._list_contexts(context=self.svc_context)

    def get_svc_url(self):
        '''Return the currently-used Spectroscopic Data Service URL.

        Parameters
        ----------
        None

        Returns
        -------
        service_url: str
            The currently-used Spectroscopic Data Service URL.

        Example
        -------
        .. code-block:: python

            from dl import specClient
            service_url = specClient.get_svc_url()
        '''
        return spcToString(self.svc_url)

    def set_profile(self, profile):
        '''Set the requested service profile.

        Parameters
        ----------
        profile: str
            Requested service profile string.

        Returns
        -------
        Nothing

        Example
        -------
        .. code-block:: python

            from dl import specClient
            profile = specClient.client.set_profile("dev")
        '''
        url = self.svc_url + '/validate?what=profile&value=%s' % profile
        if spcToString(self.curl_get(url)) == 'OK':
            self.svc_profile = spcToString(profile)
            return 'OK'
        else:
            raise Exception('Invalid profile "%s"' % profile)
        return 'OK'

    def get_profile(self):
        '''Get the requested service profile.

        Parameters
        ----------
        None

        Returns
        -------
        profile: str
            The currently requested service profile.

        Example
        -------
        .. code-block:: python

            from dl import specClient
            profile = specClient.client.get_profile()
        '''
        return spcToString(self.svc_profile)

    def set_context(self, context):
        '''Set the requested dataset context.

        Parameters
        ----------
        context: str
            Requested dataset context string.

        Returns
        -------
        Nothing

        Example
        -------
        .. code-block:: python

            from dl import specClient
            context = specClient.client.set_context("dev")
        '''
        url = self.svc_url + '/validate?what=context&value=%s' % context
        if spcToString(self.curl_get(url)) == 'OK':
            self.svc_context = spcToString(context)
            self.context = self._list_contexts(context=self.svc_context)
            return 'OK'
        else:
            raise Exception('Invalid context "%s"' % context)

    def get_context(self):
        '''Get the requested dataset context.

        Parameters
        ----------
        None

        Returns
        -------
        context: str
            The currently requested dataset context.

        Example
        -------
        .. code-block:: python

            from dl import specClient
            context = specClient.client.get_context()
        '''
        return spcToString(self.svc_context)

    def isAlive(self, svc_url=DEF_SERVICE_URL):
        '''Check whether the service at the given URL is alive and responding.
           This is a simple call to the root service URL or ping() method.

        Parameters
        ----------
        service_url: str
            The Query Service URL to ping.

        Returns
        -------
        result: bool
            True if service responds properly, False otherwise

        Example
        -------
        .. code-block:: python

            from dl import specClient
            if specClient.isAlive():
                print("Spec Server is alive")
        '''
        url = svc_url
        try:
            r = requests.get(url, timeout=2)
            resp = r.text

            if r.status_code != 200:
                return False
            elif resp is not None and r.text.lower()[:11] != "hello world":
                return False
        except Exception:
            return False

        return True


    ###################################################
    #  UTILITY METHODS
    ###################################################

    @multimethod('_spc',1,True)
    def list_profiles(self, profile, fmt='text'):
        '''Usage:  specClient.client.list_profiles (profile, ...)
        '''
        return self._list_profiles(profile=profile, fmt=fmt)

    @multimethod('_spc',0,True)
    def list_profiles(self, profile=None, fmt='text'):
        '''Usage:  specClient.client.list_profiles (...)
        '''
        return self._list_profiles(profile=profile, fmt=fmt)

    def _list_profiles(self, profile=None, fmt='text'):
        '''Implementation of the list_profiles() method.
        '''
        headers = self.getHeaders(def_token(None))

        svc_url = '%s/profiles?' % self.svc_url
        svc_url += "profile=%s&" % profile
        svc_url += "format=%s" % fmt

        r = requests.get(svc_url, headers=headers)
        profiles = spcToString(r.content)
        if '{' in profiles:
            profiles = json.loads(profiles)

        return profiles



    @multimethod('_spc',1,True)
    def list_contexts(self, context, fmt='text'):
        '''Usage:  specClient.client.list_contexts (context, ...)
        '''
        return self._list_contexts(context=context, fmt=fmt)

    @multimethod('_spc',0,True)
    def list_contexts(self, context=None, fmt='text'):
        '''Usage:  specClient.client.list_contexts (...)
        '''
        return self._list_contexts(context=context, fmt=fmt)

    def _list_contexts(self, context=None, fmt='text'):
        '''Implementation of the list_contexts() method.
        '''
        headers = self.getHeaders(def_token(None))

        svc_url = '%s/contexts?' % self.svc_url
        svc_url += "context=%s&" % context
        svc_url += "format=%s" % fmt

        r = requests.get(svc_url, headers=headers)
        contexts = spcToString(r.content)
        if '{' in contexts:
            contexts = json.loads(contexts)

        return contexts


    def catalogs(self, context='default', profile='default', fmt='text'):
        '''Usage:  specClient.client.catalogs (...)
        '''
        headers = self.getHeaders(None)

        svc_url = '%s/catalogs?' % self.svc_url
        svc_url += "context=%s&" % context
        svc_url += "profile=%s&" % profile
        svc_url += "format=%s" % fmt

        r = requests.get(svc_url, headers=headers)
        catalogs = spcToString(r.text)
        if '{' in catalogs:
            catalogs = json.loads(catalogs)

        return spcToString(catalogs)


    # --------------------------------------------------------------------
    # TO_SPECTRUM1D -- Utility method to convert a Numpy array to Spectrum1D
    #
    def to_Spectrum1D(self, npy_data):

        ''' Convert a Numpy spectrum array to a Spectrum1D object.
        '''
        if npy_data.ndim == 2:
            lamb = 10**npy_data['loglam'][0] * u.AA
        else:
            lamb = 10**npy_data['loglam'] * u.AA
        flux = npy_data['flux'] * 10**-17 * u.Unit('erg cm-2 s-1 AA-1')
        mask = npy_data['flux'] == 0
        flux_unit = u.Unit('erg cm-2 s-1 AA-1')
        uncertainty = InverseVariance(npy_data['ivar'] / flux_unit**2)

        spec1d = Spectrum1D(spectral_axis=lamb, flux=flux,
                            uncertainty=uncertainty, mask=mask)

        spec1d.meta['sky'] = npy_data['sky'] * 10**-17 * flux_unit
        spec1d.meta['model'] = npy_data['model'] * 10**-17 * flux_unit
        spec1d.meta['ivar'] = npy_data['ivar']

        return spec1d


    # --------------------------------------------------------------------
    # TO_PANDAS -- Utility method to convert a Numpy array to a Pandas DataFrame
    #
    def to_pandas(self, npy_data):

        '''Utility method to convert a Numpy array to a Pandas DataFrame
        '''
        return pd.DataFrame(data=npy_data, columns=npy_data.dtype.names)


    # --------------------------------------------------------------------
    # TO_TABLE -- Utility method to convert a Numpy array to an Astropy Table
    #
    def to_Table(self, npy_data):

        '''Utility method to convert a Numpy array to an Astropy Table object.
        '''
        return Table(data=npy_data, names=npy_data.dtype.names)



    ###################################################
    #  SERVICE METHODS
    ###################################################

    # --------------------------------------------------------------------
    # QUERY -- Query for spectra by position.
    #
    @multimethod('_spc',3,True)
    def query(self, ra, dec, size, constraint=None, out=None,
              context=None, profile=None, **kw):
        return self._query(ra=ra, dec=dec, size=size,
                           pos=None,
                           region=None,
                           constraint=constraint,
                           out=out,
                           context=context, profile=profile, **kw)

    @multimethod('_spc',2,True)
    def query(self, pos, size, constraint=None, out=None,
              context=None, profile=None, **kw):
        return self._query(ra=None, dec=None, size=None,
                           pos=pos,
                           region=None,
                           constraint=constraint,
                           out=out,
                           context=context, profile=profile, **kw)

    @multimethod('_spc',1,True)
    def query(self, region, constraint=None, out=None,
              context=None, profile=None, **kw):
        return self._query(ra=None, dec=None, size=None,
                           pos=None,
                           region=region,
                           constraint=constraint,
                           out=out,
                           context=context, profile=profile, **kw)

    @multimethod('_spc',0,True)
    def query(self, constraint=None, out=None,
              context=None, profile=None, **kw):
        '''Query for a list of spectrum IDs that can then be retrieved from
            the service.

        Usage:
            id_list = query(ra, dec, size, constraint=None, out=None,
                            context=None, profile=None, **kw)
            id_list = query(pos, size, constraint=None, out=None,
                            context=None, profile=None, **kw)
            id_list = query(region, constraint=None, out=None,
                            context=None, profile=None, **kw)
            id_list = query(constraint=None, out=None,
                            context=None, profile=None, **kw)

        Parameters
        ----------
        ra: float
            RA search center specified in degrees.

        dec: float
            Dec of search center specified in degrees.

        size: float
            Size of search center specified in degrees.

        pos: Astropy Coord object
            Coordinate of search center specified as an Astropy Coord object.

        region: float
            Array of polygon vertices (in degrees) defining a search region.

        out: str
            Save query results to output filename.  May be a 'vos://' URI or
            local filename.  If set to an empty string, the ID list is
            returned as an ascii string.

        constraint: str
            A valid SQL syntax that can be used as a WHERE constraint in the
            search query.

        context: str
            Dataset context.

        profile: str
            Data service profile.

        **kw: dict
            Optional keyword arguments.  Supported keywords currently include:

            For context='sdss_dr16' | 'default':
               fields:
                   specobjid           # or 'bestobjid', etc
                   tuple               # a plate/mjd/fiber/run2d tuple

                   Service will always return array of 'specobjid'
                   value, the p/m/f tuple is extracted from the
                   bitmask value by the client.

               catalog:
                   <schema>.<table>    # alternative catalog to query e.g. a
                                       # VAC from earlier DR (must support an
                                       # ra/dec search and return a specobjid-
                                       # like value)
            For all contexts:
               timeout = 600           # Query timeout
               token = None            # User auth token
               verbose = False         # Print verbose output
               debug = False           # Print debug messages

        Returns
        -------
        result: array
            An array of spectrum IDs appropriate for the dataset context.

        Example
        -------
           1) Query by position:

            .. code-block:: python
                id_list = spec.query (0.125, 12.123, 0.1)
        '''
        return self._query(ra=None, dec=None, size=None,
                           pos=None,
                           region=None,
                           constraint=constraint,
                           out=out,
                           context=context, profile=profile, **kw)


    def _query(self,
               ra=None, dec=None, size=None,
               pos=None,
               region=None,
               constraint=None, out=None,
               context=None, profile=None, **kw):
        '''Implementation of the query() method.
        '''

        if context in [None, '']:
            context = self.svc_context
        if profile in [None, '']:
            rofile = self.svc_profile

        # Process optional keyword arguments.
        ofields = kw['fields'] if 'fields' in kw else self.context['id_main']
        catalog = kw['catalog'] if 'catalog' in kw else self.context['catalog']
        if context == 'default' or context.startswith('sdss'):
            if ofields == 'tuple':
                ofields = 'plate,mjd,fiberid,run2d'

        timeout = kw['timeout'] if 'timeout' in kw else 600
        token = kw['token'] if 'token' in kw else self.auth_token
        verbose = kw['verbose'] if 'verbose' in kw else self.verbose
        debug = kw['debug'] if 'debug' in kw else self.debug

        # Build the query URL constraint clause.
        _size = size
        if region is not None:
            pquery = 'q3c_poly_query(ra,dec,ARRAY%s)' % region
        elif pos is not None:
            pquery = 'q3c_radial_query(ra,dec,%f,%f,%f)' % \
                         (pos.ra.degree, pos.dec.degree, _size)
        elif ra is not None and dec is not None:
            pquery = 'q3c_radial_query(ra,dec,%f,%f,%f)' % (ra, dec, _size)
        else:
            pquery = ''

        # Create the query string for the IDs, adding any user-defined
        # fields or constraints.
        cond = pquery
        if constraint not in [None, '']:
            if constraint.strip()[:5].lower() in ['limit', 'order'] or pquery == '':
                cond += ' %s' % constraint
            else:
                cond += ' AND %s' % constraint

        # Set service call headers.
        headers = self.getHeaders(None)

        # Query for the ID/fields.
        _svc_url = '%s/query?' % self.svc_url           # base service URL
        _svc_url += 'id=&'	                        # no ID value
        _svc_url += 'fields=%s&' % quote_plus(ofields)  # fields to retrieve
        _svc_url += 'catalog=%s&' % quote_plus(catalog) # catalog to query
        _svc_url += 'cond=%s&' % quote_plus(cond)       # WHERE condition
        _svc_url += 'context=%s&' % context             # dataset context
        _svc_url += 'profile=%s&' % profile             # service profile
        _svc_url += 'debug=%s&' % debug                 # system debug flag
        _svc_url += 'verbose=%s' % False           # system verbose flag
        r = requests.get(_svc_url, headers=headers)
        _res = spcToString(r.content)

        # Query result is in CSV, convert to a named table.
        res = convert(_res, outfmt='table')

        if out in [None, '']:
            if ofields.count(',') > 0:
                return res
            else:
                return np.array(res[self.context['id_main']])
        else:
            # Note:  memory expensive for large lists .....
            csv_text = _res
            if out.startswith('vos://'):
                return storeClient.saveAs(csv_text, out)[0]
            else:
                with open(out, "w") as fd:
                    fd.write(csv_text)
                    fd.write('\n')
                return 'OK'


    # --------------------------------------------------------------------
    # GETSPEC -- Retrieve spectra for a list of objects.
    #
    def getSpec(self, id_list, fmt='numpy', out=None, align=False,
                cutout=None, context=None, profile=None, **kw):

        '''Get spectra for a list of object IDs.

        Usage:
            getSpec(id_list, fmt='numpy', out=None, align=False, cutout=None,
                    context=None, profile=None, **kw)

        Parameters
        ----------
        id_list: list object
            List of object identifiers.

        format:
            Return format of spectra

        out:
            Output filename or return to caller if None

        align:
            Align spectra to common wavelength grid with zero-padding

        cutout:
            Wavelength cutout range (as "<start>-<end>")

        context: str
            Dataset context.

        profile: str
            Data service profile.

        **kw: dict
            Optional keyword arguments.  Supported keywords currently include:

               values = None
                   Spectrum vectors to return.
               token = None
                   Data Lab auth token.
               id_col = None
                   Name of ID column in input table.
               verbose = False
                   Print verbose messages during retrieval
               debug = False
                   Print debug messages during retrieval

        Returns
        -------
        result: object or array of objects or 'OK' string

        Example
        -------
           1) Retrieve spectra individually:

            .. code-block:: python
                id_list = spec.query (0.125, 12.123, 0.1)
                for id in id_list:
                    spec = spec.getSpec (id)
                    .... do something

            2) Retrieve spectra in bulk:

            .. code-block:: python
                spec = spec.getSpec (id_list, fmt='numpy')
                .... 'spec' is an array of NumPy objects that may be
                     different sizes
        '''

        if context in [None, '']:
            context = self.svc_context
        if profile in [None, '']:
            profile = self.svc_profile

        # Process optional parameters.
        values = kw['values'] if 'values' in kw else 'all'
        token = kw['token'] if 'token' in kw else self.auth_token
        id_col = kw['id_col'] if 'id_col' in kw else None
        verbose = kw['verbose'] if 'verbose' in kw else self.verbose
        debug = kw['debug'] if 'debug' in kw else self.debug

        # Set service call headers.
        headers = {'Content-Type': 'application/x-www-form-urlencoded',
                   'X-DL-ClientVersion': __version__,
                   'X-DL-OriginIP': self.hostip,
                   'X-DL-OriginHost': self.hostname,
                   'X-DL-AuthToken': def_token(None)}

        if debug:
            print('getSpec(): in ty id_list = ' + str(type(id_list)))

        # Extract the ID list from the input value.
        ids = self.extractIDList(id_list)

        # Force alignment for SpectrumCollection format.
        if fmt.lower() == 'spectrumcollection':
            align = True

        if debug:
            print('ty ids: ' + str(type(ids)))

        # Initialize the payload.
        data = {'id_list': ids,
                'values': values,
                'format': fmt,
                'align': align,
                'cutout': cutout,
                'w0': 0.0,
                'w1': 0.0,
                'context': context,
                'profile': profile,
                'debug': debug,
                'verbose': verbose
              }

        # Get the limits of the collection
        url = '%s/listSpan' % self.svc_url
        resp = requests.post(url, data=data, headers=headers)
        v = json.loads(resp.text)
        data['w0'], data['w1'] = v['w0'], v['w1']

        url = '%s/getSpec' % self.svc_url
        if align:
            # If we're aligning columns, the server will pad the values
            # and return a common array size.
            resp = requests.post(url, data=data, headers=headers)
            _data = np.load(BytesIO(resp.content), allow_pickle=False)
        else:
            # If not aligning columns, request each spectrum individually
            # so we can return a list object.
            _data = []
            for id in ids:
                data['id_list'] = id
                resp = requests.post(url, data=data, headers=headers)
                if fmt.lower() == 'fits':
                    _data.append(resp.content)
                else:
                    np_data = np.load(BytesIO(resp.content), allow_pickle=False)
                    _data.append(np_data)
            if fmt.lower() != 'fits':
                _data = np.array(_data)

        if fmt.lower() == 'fits':
            # Note: assumes a single file is requested.
            if len(_data) == 1:
                return _data[0]
            else:
                return _data
        else:
            if fmt.lower()[:5] == 'numpy':		# NUMPY array
                if len(_data) == 1:
                    return _data[0]
                else:
                    return _data
            elif fmt.lower()[:6] == 'pandas':		# Pandas DataFrame
                if len(_data) == 1:
                    return self.to_pandas(_data[0])
                else:
                    pd_data = []
                    for d in _data:
                        pd_data.append(self.to_pandas(d))
                    return pd_data
            elif fmt.lower()[:6] == 'tables':		# Astropy Table
                if len(_data) == 1:
                    return self.to_Table(_data[0])
                else:
                    tb_data = []
                    for d in _data:
                        tb_data.append(self.to_Table(d))
                    return tb_data
            elif fmt.lower()[:8] == 'spectrum':		# Spectrum1D
                if len(_data) == 1:
                    return self.to_Spectrum1D(_data[0])
                elif align:
                    return self.to_Spectrum1D(_data)
                else:
                    sp_data = []
                    for i in range(len(_data)):
                        sp_data.append(self.to_Spectrum1D(_data[i]))

                    # Convert to a SpectrumCollection object if requested.
                    if fmt.lower() == 'spectrumcollection':
                        return SpectrumCollection.from_spectra(sp_data)
                    else:
                        return sp_data
            else:
                raise Exception("Unknown return format '%s'" % fmt)


    # --------------------------------------------------------------------
    # PLOT -- Utility to batch plot a single spectrum, display plot directly.
    #
    def plot(self, spec, context=None, profile=None, out=None, **kw):

        '''Utility to batch plot a single spectrum.

        Usage:
            spec.plot(id, context=None, profile=None, **kw)

        Parameters
        ----------
        spec: object ID or data array
            Spectrum to be plotted.  If 'spec' is a numpy array or a
            Spectrum1D object the data are plotted directly, otherwise
            the value is assumed to be an object ID that will be retrieved
            from the service.

        out: str
            Output filename.  If specified, plot saved as PNG.

        context: str
            Dataset context.

        profile: str
            Data service profile.

        **kw: dict
            Optional keyword arguments.  Supported keywords currently include:

                rest_frame - Whether or not to plot the spectra in the
                             rest-frame. If True, the wavelengths are assumed
                             to have been already shifted and no 'z' value is
                             required.  If False, wavelengths are assumed to
                             be in observed frame and a 'z' value is required
                             to overplot absorption/emission lines. (def: True)
                         z - Redshift value (def: None)
                      xlim - Set the xrange of the plot
                      ylim - Set the yrange of the plot
                     title - Plot title (def: object ID)
                    xlabel - Plot x-axis label (def: wavelength)
                    ylabel - Plot y-axis label (def: flux units)
                        out - Saved output filename.

                    values - A comma-delimited string of which values to plot,
                             a combination of 'flux,model,sky,ivar'
                mark_lines - Which lines to mark.  No lines marked if None or
                             an empty string, otherwise one of 'em|abs|all|both'
                      grid - Plot grid lines (def: True)
                      dark - Dark-mode plot colors (def: True)
                  em_lines - List of emission lines to plot.  If not given,
                             all the lines in the default list will be plotted.
                 abs_lines - Lines of absorption lines to plot.  If not given,
                             all the lines in the default list will be plotted.
                 spec_args - Plotting kwargs for the spectrum
                model_args - Plotting kwargs for the model
                 ivar_args - Plotting kwargs for the ivar
                  sky_args - Plotting kwargs for the sky

        Returns
        -------
            Nothing

        Example
        -------
           1) Plot a single spectrum, save to a virtual storage file

            .. code-block:: python
                spec.plot (specID, context='sdss_dr16', out='vos://spec.png')
        '''

        verbose = kw['verbose'] if 'verbose' in kw else self.verbose
        debug = kw['debug'] if 'debug' in kw else self.debug

        if context in [None, '']:
            context = self.svc_context
        if profile in [None, '']:
            profile = self.svc_profile

        # See whether we've been passed a spectrum ID or a data.
        _id = None
        if isinstance(spec, int) or \
           isinstance(spec, np.int64) or isinstance(spec, np.uint64) or \
           isinstance(spec, tuple) or \
           isinstance(spec, str):
               _id = spec
               dlist = sp_client.getSpec(spec, context=context, profile=profile)
               data = dlist
               wavelength = 10.0**data['loglam']
               flux = data['flux']
               model = data['model']
               sky = data['sky']
               ivar = data['ivar']
        else:
            if isinstance(spec, np.ndarray) or \
               isinstance(spec, pd.core.frame.DataFrame):
                   wavelength = 10.0**spec['loglam']
                   flux = spec['flux']
                   model = spec['model']
                   sky = spec['sky']
                   ivar = spec['ivar']
            elif isinstance(spec, Spectrum1D):
                wavelength = np.array(spec.spectral_axis.value)
                flux = spec.flux
                model = spec.meta['model']
                sky = spec.meta['sky']
                ivar = spec.meta['ivar']

        # Get the wavelength frame and redshift for the plot.
        if 'z' in kw:
            z = float(kw['z']) 			# use supplied value
            del(kw['z'])
        else:
            z = None
            if _id is not None:
                # Query for the redshift field of the catalog.
                headers = self.getHeaders(None)
                _svc_url = '%s/query?' % self.svc_url      # base service URL
                _svc_url += "id=%s&" % str(_id)
                _svc_url += "fields=%s&" % self.context['redshift']
                _svc_url += "catalog=%s&" % self.context['catalog']
                _svc_url += "cond=&"
                _svc_url += "context=%s&" % context
                _svc_url += "profile=%s&" % profile
                _svc_url += "debug=%s&" % debug
                _svc_url += "verbose=%s" % verbose
                r = requests.get(_svc_url, headers=headers)
                if r.status_code == 200:
                    _val = spcToString(r.content).split('\n')[1:-1][0]
                    z = float(_val)

        if 'rest_frame' in kw:
            rest_frame = (str(kw['rest_frame']).lower() == 'true')
            del(kw['rest_frame'])
        else:
            rest_frame = True

        if self.context['rest_frame'] == 'false':
            # Data is in the observed rest frame, convert to rest frame if we
            # have a redshift.
            if rest_frame:
                if z is not None:
                    wavelength /= (1 + z)
                else:
                    warnings.warn('Redshift needed to plot in rest frame.')
                    rest_frame = False
        else:
            # Data is already in rest frame, convert to observed frame if we
            # have a redshift.
            if not rest_frame:
                if z is not None:
                    wavelength *= (1 + z)
                else:
                    warning.warn('Redshift needed to plot in observed frame.')
                    rest_frame = True

        self._plotSpec(wavelength, flux, model=model, sky=sky, ivar=ivar,
                       rest_frame=rest_frame, z=z, **kw)


    # --------------------------------------------------------------------
    # PROSPECT -- Utility wrapper to launch the interactive PROSPECT tool.
    #
    def prospect(self, spec, context=None, profile=None, **kw):

        '''Utility wrapper to launch the interactive PROSPECT tool.

        Usage:
            stat =  prospect(spec, context=None, profile=None, **kw)

        Parameters
        ----------
        spec: object ID or data array
            Spectrum to be plotted.  If 'spec' is a numpy array or a
            Spectrum1D object the data are plotted directly, otherwise
            the value is assumed to be an object ID that will be retrieved
            from the service.

        context: str
            Dataset context.

        profile: str
            Data service profile.

        **kw: dict
            Optional keyword arguments.  Supported keywords currently include:

               TBD

        Returns
        -------
        result: str
            Status 'OK' string or error message.

        Example
        -------
           1) Plot ....

            .. code-block:: python
                stat = spec.prospect (specID)
        '''

        if context in [None, '']:
            context = self.svc_context
        if profile in [None, '']:
            profile = self.svc_profile

        pass


    # --------------------------------------------------------------------
    # PREVIEW -- Get a preview plot of a spectrum
    #
    def preview(self, spec, context=None, profile=None, **kw):

        '''Get a preview plot of a spectrum

        Usage:
            spec.preview(spec, context=None, profile=None, **kw):

        Parameters
        ----------
        spec: objectID
            Object identifiers.

        context: str
            Dataset context.

        profile: str
            Data service profile.

        **kw: dict
            Optional keyword arguments.  Supported keywords currently include:

               N/A

        Returns
        -------
        image: A PNG image object

        Example
        -------
           1) Display a preview plot a given spectrum:

            .. code-block:: python
                from IPython.display import display, Image
                display(Image(spec.preview(id),
                        format='png', width=400, height=100, unconfined=True))
        '''

        if context in [None, '']:
            context = self.svc_context
        if profile in [None, '']:
            profile = self.svc_profile

        url = self.svc_url + '/preview?id=%s' % str(spec)
        url = url + '&context=%s&profile=%s' % (context, profile)
        try:
            if USE_CURL:
                return Image.open(BytesIO(self.curl_get(url)))
            else:
                return Image.open(BytesIO(requests.get(url, timeout=2).content))
        except Exception as e:
            raise Exception("Error getting plot data: " + str(e))



    # --------------------------------------------------------------------
    # PLOTGRID -- Get a grid of preview plots of a spectrum list.
    #
    def plotGrid(self, id_list, nx, ny, page=0,
                 context=None, profile=None, **kw):

        '''Get a grid of preview plots of a spectrum list.

        Usage:
            image = plotGrid(id_list, nx, ny, page=0,
                             context=None, profile=None, **kw):

        Parameters
        ----------
        id_list: list object
            List of object identifiers.

        nx: int
            Number of plots in the X dimension

        ny: int
            Number of plots in the Y dimension

        page: int
            Dataset context.

        context: str
            Dataset context.

        profile: str
            Data service profile.

        **kw: dict
            Optional keyword arguments.  Supported keywords currently include:

               verbose = False
                   Print verbose messages during retrieval
               debug = False
                   Print debug messages during retrieval
        Returns
        -------
        image: A PNG image object

        Example
        -------
           1) Display a 5x5 grid of preview plots for a list:

            .. code-block:: python
                npages = np.round((len(id_list) / 25) + (25 / len(id_list))
                for pg in range(npages):
                    data = spec.getGridPlot(id_list, 5, 5, page=pg)
                    display(Image(data, format='png',
                            width=400, height=100, unconfined=True))
        '''

        if context in [None, '']:
            context = self.svc_context
        if profile in [None, '']:
            profile = self.svc_profile

        # Process optional parameters.
        token = kw['token'] if 'token' in kw else self.auth_token
        verbose = kw['verbose'] if 'verbose' in kw else self.verbose
        debug = kw['debug'] if 'debug' in kw else self.debug
        fmt = kw['fmt'] if 'fmt' in kw else 'png'

        # Set service call headers.
        headers = {'Content-Type': 'application/x-www-form-urlencoded',
                   'X-DL-ClientVersion': __version__,
                   'X-DL-OriginIP': self.hostip,
                   'X-DL-OriginHost': self.hostname,
                   'X-DL-AuthToken': token}

        # Build the query URL string.
        url = '%s/plotGrid' % self.svc_url

        if isinstance(id_list, list) or isinstance(id_list, np.ndarray):
            n_ids = len(id_list)
            sz_grid = nx * ny
            if sz_grid >= n_ids:         # Use the whole list.
                ids = id_list
                p_start = 0
                p_end = len(id_list) - 1
            else:
                p_start = page * sz_grid
                p_end = min(n_ids, p_start + sz_grid)
                ids = id_list[p_start:p_end]
        else:
            ids = id_list

        # Initialize the payload.
        data = {'id_list': list(ids),
                'ncols': ny,
                'context': context,
                'profile': profile,
                'debug': debug,
                'verbose': verbose
              }

        resp = requests.post(url, data=data, headers=headers)
        if fmt == 'png':
            return Image.open(BytesIO(resp.content))
        else:
            return resp.content


    # --------------------------------------------------------------------
    # STACKEDIMAGE -- Get a stacked image of a list of spectra.
    #
    def stackedImage(self, id_list, align=False, yflip=False,
                     context=None, profile=None, **kw):

        '''Get ...

        Usage:

        Parameters
        ----------
        id_list: list object
            List of spectrum identifiers.

        context: str
            Dataset context.

        profile: str
            Data service profile.

        **kw: dict
            Optional keyword arguments.  Supported keywords currently include:

               verbose = False
                   Print verbose messages during retrieval
               debug = False
                   Print debug messages during retrieval
        Returns
        -------
        result: ....

        Example
        -------
           1) Query ....

            .. code-block:: python
                id_list = spec.query (0.125, 12.123, 0.1)
        '''

        if context in [None, '']:
            context = self.svc_context
        if profile in [None, '']:
            profile = self.svc_profile

        # Process optional parameters.
        scale = kw['scale'] if 'scale' in kw else (1.0, 1.0)
        if isinstance(scale, float):
            xscale = yscale = scale
        else:
            xscale = scale[0]
            yscale = scale[1]
        thickness = kw['thickness'] if 'thickness' in kw else 1
        inverse = kw['inverse'] if 'inverse' in kw else False
        cmap = kw['cmap'] if 'cmap' in kw else 'gray'
        width = kw['width'] if 'width' in kw else 0
        height = kw['height'] if 'height' in kw else 0
        token = kw['token'] if 'token' in kw else self.auth_token
        verbose = kw['verbose'] if 'verbose' in kw else self.verbose
        debug = kw['debug'] if 'debug' in kw else self.debug
        fmt = kw['fmt'] if 'fmt' in kw else 'png'

        # Set service call headers.
        headers = {'Content-Type': 'application/x-www-form-urlencoded',
                   'X-DL-ClientVersion': __version__,
                   'X-DL-OriginIP': self.hostip,
                   'X-DL-OriginHost': self.hostname,
                   'X-DL-AuthToken': token}

        # Build the query URL string.
        url = '%s/stackedImage' % self.svc_url

        # Initialize the payload.
        data = {'id_list': list(id_list),
                'context': context,
                'xscale': xscale,
                'yscale': yscale,
                'thickness': thickness,
                'cmap': cmap,
                'inverse': inverse,
                'width': width,
                'height': height,
                'profile': profile,
                'debug': debug,
                'verbose': verbose
              }

        resp = requests.post(url, data=data, headers=headers)
        if fmt == 'png':
            return Image.open(BytesIO(resp.content))
        else:
            return resp.content


    ###################################################
    #  STATIC UTILITY METHODS
    ###################################################

    # --------------------------------------------------------------------
    # _PLOTSPEC -- Plot a spectrum.
    #
    @staticmethod
    def _plotSpec(wavelength, flux, model=None, sky=None, ivar=None,
                  rest_frame=True, z=0.0, xlim=None, ylim=None,
                  title=None, xlabel=None, ylabel=None, out=None, **kw):
        """Plot a spectrum.

        Inputs:
            * wavelength - Array of spectrum wavelength values to plot.
            * flux -  Array of spectrum flux values to plot.
            * model -  Array of model spectrum values to plot (if not None).
            * sky -  Array of sky spectrum values to plot (if not None).
            * ivar -  Array of inverse-variance values to plot (if not None).

            * rest_frame - Whether or not to plot the spectra in the
                           rest-frame. If True, the wavelengths are assumed
                           to have been already shifted and no 'z' value is
                           required.  If False, wavelengths are assumed to
                           be in observed frame and a 'z' value is required
                           to overplot absorption/emission lines. (def: True)
            * z - Redshift (def: None)
            * xlim - Setting the xrange of the plot (e.g. '[5000.0,6000.0]')
            * ylim - Setting the yrange of the plot (e.g. '[0.0,25.0]')
            * title - Plot title (def: object ID)
            * xlabel - Plot x-axis label (def: wavelength)
            * ylabel - Plot y-axis label (def: flux units)
            * out - Saved output filename.

        Optional kwargs:
            * values - A comma-delimited string of which values to plot, a
                      combination of 'flux,model,sky,ivar'
            * mark_lines - Which lines to mark.  No lines marked if None or
                           an empty string, otherwise one of 'em|abs|all|both'
            * grid - Plot grid lines (def: True)
            * dark - Dark-mode plot colors (def: True)
            * em_lines - List of emission lines to plot.  If not given, all
                         the lines in the default list will be plotted.
            * abs_lines - Lines of absorption lines to plot.  If not given,
                          all the lines in the default list will be plotted.
            * spec_args - Plotting kwargs for the spectrum.
            * model_args - Plotting kwargs for the model.
            * ivar_args - Plotting kwargs for the ivar.
            * sky_args - Plotting kwargs for the sky.
        """

        def labelLines(lines, ax, color, yloc):
            '''Select only those lines that are visible in
               the x-range of the plot.
            '''
            if rest_frame is False and z is None:
                warnings.warn(
                         'Redshift required to mark lines in observed frame')
                return

            for ii in range(len(lines)):
                # If rest_frame=False, shift lines to the observed frame.
                lam = lines[ii]['lambda']
                if rest_frame is False:
                    lam = lam * (1+z)
                # Plot only lines within the x-range of the plot.
                if ((lam > xbounds[0]) & (lam < xbounds[1])):
                    ax.axvline(lam, color=color, lw=1.0, linestyle=':')
                    ax.annotate(lines[ii]['label'], xy=(lam, yloc),
                                xycoords=ax.get_xaxis_transform(),
                                fontsize=12, rotation=90, color=color)

        # Process the optional kwargs.
        dark = kw['dark'] if 'dark' in kw else True
        grid = kw['grid'] if 'grid' in kw else True
        mark_lines = kw['mark_lines'] if 'mark_lines' in kw else 'all'
        em_lines = kw['em_lines'] if 'em_lines' in kw else None
        abs_lines = kw['abs_lines'] if 'abs_lines' in kw else None
        values = kw['values'] if 'values' in kw else 'flux,model'

        if 'spec_args' in kw:
            spec_args = kw['spec_args']
        else:
            spec_args = {'color': '#ababab', 'linewidth': 1.0, 'alpha': 1.0}
        if 'model_args' in kw:
            model_args = kw['model_args']
        else:
            model_args = {'color': 'red', 'linewidth': 1.2}
        if 'sky_args' in kw:
            sky_args = kw['sky_args']
        else:
            sky_args = {'color': 'brown', 'linewidth': 1.0}
        if 'ivar_args' in kw:
            ivar_args = kw['ivar_args']
        else:
            ivar_args = {'color': 'blue', 'linewidth': 1.0}

        # Setting up the plot
        if dark:
            fig = plt.figure(dpi=100, figsize=(12, 5), facecolor='#2F4F4F')
            plt.rcParams['axes.facecolor'] = '#121212'
            plt.rcParams['axes.edgecolor'] = '#00FFFF'
        else:
            fig = plt.figure(dpi=100, figsize=(12, 5))
            plt.rcParams['axes.facecolor'] = '#FFFFFF'

        ax = fig.add_subplot(111)
        if 'flux' in values:
            if ivar is None:
                ax.plot(wavelength, flux, label='Flux', **spec_args)
            else:
                ax.plot(wavelength, flux * (ivar > 0), label='Flux',
                        **spec_args)
        if 'model' in values and model is not None:
            if ivar is None:
                ax.plot(wavelength, model, label='Model', **model_args)
            else:
                ax.plot(wavelength, model * (ivar > 0), label='Model',
                        **model_args)
        if 'sky' in values and sky is not None and ivar is not None:
            if ivar is None:
                ax.plot(wavelength, sky, label='Sky', **model_args)
            else:
                ax.plot(wavelength, sky * (ivar > 0), label='Sky', **sky_args)
        if 'ivar' in values and ivar is not None:
            ax.plot(wavelength, ivar * (ivar > 0), label='Ivar', **ivar_args)

        plt.xlim(xlim)
        plt.ylim(ylim)
        am_color = ('#00FF00' if dark else 'black')
        if ylabel is None:
            if rest_frame:
                plt.xlabel('Rest Wavelength ($\AA$)', color=am_color)
            else:
                if z is not None:
                    plt.xlabel('Observed Wavelength ($\AA$)    z=%.3g' % z,
                               color=am_color)
                else:
                    plt.xlabel('Observed Wavelength ($\AA$)    z=(unknown)',
                               color=am_color)
        else:
            plt.xlabel(xlabel, color=am_color)
        if ylabel is None:
            ylab = '$F_{\lambda}$ ($10^{-17}~ergs~s^{-1}~cm^{-2}~\AA^{-1}$)'
            plt.ylabel(ylab, color=am_color)
        else:
            plt.ylabel(ylabel, color=am_color)

        if dark:
            ax.tick_params(color='cyan', labelcolor='yellow')
        if grid:
            plt.grid(color='gray', linestyle='dashdot', linewidth=0.5)

        if title not in [None, '']:
            ax.set_title(title, c=am_color)

        # Plotting Absorption/Emission lines - only works if either of the
        # lines is set to True
        if mark_lines not in [None, '']:
            if mark_lines.lower() in ['all', 'both']:
                opt = 'ea'
            else:
                opt = mark_lines.lower()

            # Select any lines listed by the user.
            e_lines = _em_lines
            if em_lines is not None:
                e_lines = list(filter(lambda x: x['name'] in em_lines,
                               _em_lines))
            a_lines = _abs_lines
            if abs_lines is not None:
                a_lines = list(filter(lambda x: x['name'] in abs_lines,
                               _abs_lines))
            xbounds = ax.get_xbound()   # Getting the x-range of the plot

            lcol = ['#FFFF00', '#00FFFF'] if dark else ['#FF0000', '#0000FF']
            if 'e' in opt:
                labelLines(e_lines, ax, lcol[0], 0.875)
            if 'a' in opt:
                labelLines(a_lines, ax, lcol[1], 0.05)

        leg = ax.legend()
        if dark:
            for text in leg.get_texts():
                plt.setp(text, color='w')

        if out is not None:
            plt.savefig(out)
        else:
            plt.show()

    ###################################################
    #  PRIVATE UTILITY METHODS
    ###################################################

    def debug(self, debug_val):
        '''Toggle debug flag.
        '''
        self.debug = debug_val

    def retBoolValue(self, url):
        '''Utility method to call a boolean service at the given URL.
        '''
        response = ""
        try:
            # Add the auth token to the reauest header.
            if self.auth_token != None:
                headers = {'X-DL-AuthToken': self.auth_token}
                r = requests.get(url, headers=headers)
            else:
                r = requests.get(url)
            response = spcToString(r.content)

            if r.status_code != 200:
                raise Exception(r.content)

        except Exception:
            return spcToString(r.content)
        else:
            return response

    def getHeaders(self, token):
        '''Get default tracking headers.
        '''
        tok = def_token(token)
        user, uid, gid, hash = tok.strip().split('.', 3)
        hdrs = {'Content-Type': 'text/ascii',
                'X-DL-ClientVersion': __version__,
                'X-DL-OriginIP': self.hostip,
                'X-DL-OriginHost': self.hostname,
                'X-DL-User': user,
                'X-DL-AuthToken': tok}
        return hdrs

    def getFromURL(self, svc_url, path, token):
        '''Get something from a URL.  Return a 'response' object.
        '''
        try:
            hdrs = self.getHeaders(token)
            resp = requests.get("%s%s" % (svc_url, path), headers=hdrs)

        except Exception as e:
            raise dlSpecError(str(e))
        return resp

    def curl_get(self, url):
        '''Utility routine to use cURL to return a URL
        '''
        b_obj = BytesIO()
        crl = pycurl.Curl()
        crl.setopt(crl.URL, url)
        crl.setopt(crl.WRITEDATA, b_obj)
        crl.perform()
        crl.close()
        return b_obj.getvalue()

    def extractIDList(self, id_list, id_col=None):
        '''Extract a 1-D array or single identifier from an input ID type.
        '''
        if isinstance(id_list, str):
            # Input is a string.  This may be a text string of identifiers,
            # a filename or a CSV table.
            if os.path.exists(id_list):
                # Read list from a local file.
                with open(id_list, 'r') as fd:
                    _list = fd.read()
                if _list.startswith(self.context['id_main']):   # CSV string?
                    ids = _list.split('\n')[1:-1]
                else:
                    ids = _list.split('\n')[:-1]
            elif id_list.startswith('vos://'):
                # Read list from virtual storage.
                ids = storeClient.get(id_list).split('\n')[:-1]
            elif id_list.find(',') > 0 or \
                 id_list.startswith(self.context['id_main']):   # CSV string?
                     pdata = convert(id_list, outfmt='pandas')
                     ids = np.array(pdata[self.context['id_main']])
            else:
                ids = np.array([id_list])

            el = ids[0]
            if isinstance(el, str):
                cnv_list = []
                if el[0] == '(':      # Assume a tuple
                    for el in ids:
                        if el != '':
                            cnv_list.append(el[1:-1])
                else:
                    for el in ids:
                        if el != '':
                            cnv_list.append(int(el))
                ids = np.array(cnv_list)

        elif isinstance(id_list, int) or \
             isinstance(id_list, np.int64) or \
             isinstance(id_list, np.uint64) or\
             isinstance(id_list, tuple):
                 # Input is a single integer or tuple type (e.g. a specobjid
                 # or a (plate,mjd,fiber), simply convert it to a list.
                 ids = [id_list]

        elif isinstance(id_list, list):
            # Input is already a list type, just return it.
            ids = id_list

        elif isinstance(id_list, pd.core.frame.DataFrame):
            try:
                ids = id_list[self.context['id_main']].tolist()
            except KeyError as e:
                ids = None

        elif isinstance(id_list, np.ndarray):
            # Input is a numpy array.  If it's a 1-D array assume it contains
            # identifiers and convert to a list, otherwise try to extract the
            # id column.
            if id_list.ndim == 1 and id_list.dtype.names is None:
                ids = id_list.tolist()
            else:
                try:
                    if id_list.dtype.names is not None:
                        # structured array
                        ids = id_list[self.context['id_main']].tolist()
                    else:
                        # ndarray, use first column
                        ids = id_list[:, 0].tolist()
                except ValueError as e:
                    ids = None

        else:
            ids = np.array(id_list[self.context['id_main']])

        return ids


# ###################################
#  Spectroscopic Data Client Handles
# ###################################

def getClient(context='default', profile='default'):
    '''Get a new instance of the specClient client.

    Parameters
    ----------
    context: str
        Dataset context

    profile: str
        Service profile

    Returns
    -------
    client: specClient
        An specClient object

    Example
    -------
    .. code-block:: python
        new_client = specClient.getClient()
    '''
    return specClient(context=context, profile=profile)


# Get the default client object.
sp_client = client = getClient(context='default', profile='default')


# ##########################################
#  Patch the docstrings for module functions
# ##########################################

set_svc_url.__doc__ = sp_client.set_svc_url.__doc__
get_svc_url.__doc__ = sp_client.get_svc_url.__doc__
set_profile.__doc__ = sp_client.set_profile.__doc__
get_profile.__doc__ = sp_client.get_profile.__doc__
set_context.__doc__ = sp_client.set_context.__doc__
get_context.__doc__ = sp_client.get_context.__doc__


# Define a set of spectral lines.
#
# This is the set of emission lines from the SDSS spZline files.
# Wavelengths are in air for lambda > 2000, vacuum for lambda < 2000.
#
# Emission Lines
_em_lines = [
    {"name": "Ly-alpha",       "lambda": 1215.67,  "label": "Ly$\\alpha$"},
    {"name": "N V 1240",       "lambda": 1240.81,  "label": "N V"},
    {"name": "C IV 1549",      "lambda": 1549.48,  "label": "C IV"},
    {"name": "He II 1640",     "lambda": 1640.42,  "label": "He II"},
    {"name": "C III] 1908",    "lambda": 1908.734, "label": "C III]"},
    {"name": "Mg II 2799",     "lambda": 2800.315, "label": "Mg II"},
    {"name": "[O II] 3725",    "lambda": 3727.092, "label": " "},
    {"name": "[O II] 3727",    "lambda": 3729.875, "label": "[O II]"},
    {"name": "[Ne III] 3868",  "lambda": 3869.857, "label": "[Ne III]"},
    {"name": "H-zeta",         "lambda": 3890.151, "label": "H$\\zeta$"},
    {"name": "[Ne III] 3970",  "lambda": 3971.123, "label": "[Ne III]"},
    {"name": "H-epsilon",      "lambda": 3971.195, "label": "H$\\epsilon$"},
    {"name": "H-delta",        "lambda": 4102.892, "label": "H$\\delta$"},
    {"name": "H-gamma",        "lambda": 4341.684, "label": "H$\\beta$"},
    {"name": "[O III] 4363",   "lambda": 4364.435, "label": "[O III]"},
    {"name": "He II 4685",     "lambda": 4686.991, "label": "He II"},
    {"name": "H-beta",         "lambda": 4862.683, "label": "H$\\beta$"},
    {"name": "[O III] 4959",   "lambda": 4960.294, "label": "[O III]"},
    {"name": "[O III] 5007",   "lambda": 5008.239, "label": "[O III]"},
    {"name": "He II 5411",     "lambda": 5413.025, "label": "He II"},
    {"name": "[O I] 5577",     "lambda": 5578.888, "label": "[O I]"},
    {"name": "[N II] 5755",    "lambda": 5756.186, "label": "[Ne II]"},
    {"name": "He I 5876",      "lambda": 5877.308, "label": "He I"},
    {"name": "[O I] 6300",     "lambda": 6302.046, "label": "[O I]"},
    {"name": "[S III] 6312",   "lambda": 6313.806, "label": "[S III]"},
    {"name": "[O I] 6363",     "lambda": 6365.535, "label": "[O I]"},
    {"name": "[N II] 6548",    "lambda": 6549.859, "label": "[N II]"},
    {"name": "H-alpha",        "lambda": 6564.614, "label": "H$\\alpha$"},
    {"name": "[N II] 6583",    "lambda": 6585.268, "label": "[N II]"},
    {"name": "[S II] 6716",    "lambda": 6718.294, "label": "[S II]"},
    {"name": "[S II] 6730",    "lambda": 6732.678, "label": "[S II]"},
    {"name": "[Ar III] 7135",  "lambda": 7137.758, "label": "[Ar III]"}
]

# Absorption lines
_abs_lines = [
    {"name": "H12",            "lambda": 3751.22,  "label": "H12"},
    {"name": "H11",            "lambda": 3771.70,  "label": "H11"},
    {"name": "H10",            "lambda": 3798.98,  "label": "H10"},
    {"name": "H9",             "lambda": 3836.48,  "label": "H9"},
    {"name": "H-zeta",         "lambda": 3890.151, "label": "H$\\zeta$"},
    {"name": "K (Ca II 3933)", "lambda": 3934.814, "label": "K (Ca II)"},
    {"name": "H (Ca II 3968)", "lambda": 3969.623, "label": "H (Ca II)"},
    {"name": "H-epsilon",      "lambda": 3971.195, "label": "H$\\epsilon$"},
    {"name": "H-delta",        "lambda": 4102.892, "label": "H$\\delta$"},
    {"name": "G (Ca I 4307)",  "lambda": 4308.952, "label": "G (Ca I)"},
    {"name": "H-gamma",        "lambda": 4341.684, "label": "H$\\gamma$"},
    {"name": "H-beta",         "lambda": 4862.683, "label": "H$\\beta$"},
    {"name": "Mg I 5183",      "lambda": 5185.048, "label": " "},
    {"name": "Mg I 5172",      "lambda": 5174.125, "label": " "},
    {"name": "Mg I 5167",      "lambda": 5168.762, "label": "Mg I"},
    {"name": "D2 (Na I 5889)", "lambda": 5891.582, "label": " "},
    {"name": "D1 (Na I 5895)", "lambda": 5897.554, "label": "D1,2 (Na I)"},
    {"name": "H-alpha",        "lambda": 6564.614, "label": "H$\\alpha$"}
]


def airtovac(l):
    '''Convert air wavelengths (greater than 2000A) to vacuum wavelengths.
    '''
    if l < 2000.0:
        return l
    vac = l
    for iter in range(2):
        sigma2 = (1.0e4 / vac) * (1.0e4 / vac)
        fact = 1.0 + 5.792105e-2 / (238.0185 - sigma2) + \
            1.67917e-3 / (57.362 - sigma2)
        vac = l * fact
    return vac
