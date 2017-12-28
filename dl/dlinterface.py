#!/usr/bin/env python
#
# DLINTERFACE.PY -- Python interactive interface to the Data Lab services.
#

#from __future__ import print_function

__authors__ = 'David Nidever <dnidever@noao.edu>, Matthew Graham <graham@noao.edu>, Mike Fitzpatrick <fitz@noao.edu>, \
                 Robert Nikutta <rnikutta@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20170531'  # yyyymmdd


"""
    Python interactive interface to the Data Lab services.

Import via

.. code-block:: python

    from dl.dlinterface import Dlinterface
"""

import os
from subprocess import Popen, PIPE
import time
try:
    import ConfigParser
    from urllib import quote_plus, urlencode		# Python 2
    from urllib2 import urlopen, Request                # Python 2
    from cStringIO import StringIO
except ImportError:
    import configparser as ConfigParser                  # Python 2
    from urllib.parse import quote_plus, urlencode      # Python 3
    from urllib.request import urlopen, Request         # Python 3
    from io import StringIO
import requests
from io import BytesIO
    
# std lib imports
import getpass
import xml.etree.ElementTree as ET
import numpy as np
import tempfile
from functools import partial
import warnings
from astropy.utils.exceptions import AstropyWarning

# use this for SIA service for now
from pyvo.dal import sia

# Data Lab Client interfaces
from dl import authClient, storeClient, queryClient

DAEMON_TIMEOUT = 60                             # Mount timeout
CAPS_DIR = "../caps"                            # Capability directory

ANON_TOKEN = "anonymous.0.0.anon_access"        # default tokens

# Service URLs
AM_URL = "http://dlsvcs.datalab.noao.edu/auth"      # Auth Manager
SM_URL = "http://dlsvcs.datalab.noao.edu/storage"   # Storage Manager
QM_URL = "http://dlsvcs.datalab.noao.edu/query"     # Query Manager

# SIA service
#SIA_DEF_ACCESS_URL = "http://datalab.noao.edu/sia/smash"
SIA_DEF_ACCESS_URL = "http://datalab.noao.edu/sia"
SIA_DEF_SIZE = 0.0085  # degrees

def getUserName (self):
    '''  Get the currently logged-in user token.  If we haven't logged in
         return the anonymous username.
    '''
    # could also use self.loginuser
    _user = self.dl.get("login", "user")
    if _user is None or _user == '':
        return "anonymous"
    else:
        return _user

def getUserToken (self):
    '''  Get the currently logged-in user token.  If we haven't logged in
         return the anonymous token.
    '''
    _token = self.dl.get("login", "authtoken")
    if _token is None or _token == '':
        return ANON_TOKEN
    else:
        return _token

def checkLogin (self):
    '''  Check if the user is already logged in.  If not, give a warning message
    '''
    if self.loginstatus != 'loggedin' and self.loginuser != 'anonymous':
        print ("You are not currently logged in.  Please use dl.login() to do so.")
        return False
    else:
        return True

def areSyncQueriesWorking ():
    ''' This checks if the Query Manager is returning proper Sync queries.
    '''
    queryworking = False             # dead until proven alive
    if queryClient.isAlive() is True:
        # Do simple query with timeout
        headers = {'Content-Type': 'text/ascii', 'X-DL-AuthToken': ANON_TOKEN}
        query = quote_plus('select ra,dec from smash_dr1.object limit 2')
        dburl = '%s/query?sql=%s&ofmt=%s&out=%s&async=%s' % (
                "http://dlsvcs.datalab.noao.edu/query", query, "csv", None, False)
        try:
            r = requests.get(dburl, headers=headers, timeout=1)
        except:
            pass
        else:
            # Check that the output looks right
            if type(r.content) == str and len(r.content.split('\n')) == 4 and r.content[0:6] == 'ra,dec':
                queryworking = True

    return queryworking

def isTapWorking ():
    ''' This checks if the TAP service and Tomcat are running.
    '''
    tapworking = True             # True to start and many ways to make it False
    # Check if the availability/tables endpoint is working
    try:
        #request = Request("http://datalab.noao.edu/tap/avail")
        request = Request("http://datalab.noao.edu/tap/tables")
        response = urlopen(request, timeout=2).read()
        # The full read request of tables take a couple seconds, just
        #   try to get the header
        #r = requests.head("http://datalab.noao.edu/tap/tables",timeout=2)
    except:
        tapworking = False
    else:
        tapworking = (tapworking if response is not None else False)
    # Check if the Tomcat service is responding
    try:
        request = Request("http://gp01.datalab.noao.edu:8080/")
        response = urlopen(request, timeout=1).read()
    except:
        tapworking = False
    else:
        tapworking = (tapworking if response is not None else False)

    return tapworking

def areLoginsWorking():
    ''' This checks if the Authentication Manager is returning proper tokens.
    '''
    authworking = False             # dead until proven alive
    if authClient.isAlive() is True:
        # Do simple token request
        url = "http://dlsvcs.datalab.noao.edu/auth/login?"
        query_args = {"username": "datalab", "password": "datalab",
                      "profile": "default", "debug": False}
        try:
            r = requests.get(url, params=query_args, timeout=1)
        except:
            pass
        else:
            # Check that the output looks right
            response = r.text
            if type(r.content) == str and r.content[0:7] == 'datalab':
                authworking = True

    return authworking

def isListWorking():
    ''' This checks if the Storage Manager is returning proper list queries:
    '''
    storeworking = False             # dead until proven alive
    if storeClient.isAlive() is True:
        # Do simple list queries timeout
        url = "http://dlsvcs.datalab.noao.edu/storage/ls?name=vos://&format=csv"
        try:
            r = requests.get(url, headers={'X-DL-AuthToken': ANON_TOKEN}, timeout=1)
        except:
            pass
        else:
            # Check that the output looks right
            if type(r.content) == str:
                storeworking = True

    return storeworking

def addFormatMapping(self):
    ''' Add the format mapping information to the DL object
    '''
    from collections import OrderedDict
    from pandas import read_csv
    from astropy.table import Table
    from astropy.io.votable import parse_single_table

    # map outfmt container types to a tuple:
    # (:func:`queryClient.query()` fmt-value, descriptive title,
    # processing function for the result string)
    mapping = OrderedDict([
            ('csv'         , ('csv',     'CSV formatted table as a string', lambda x: x.getvalue())),
            ('ascii'       , ('ascii',   'Tab-delimited table as a string', lambda x: x.getvalue())),
            ('array'       , ('csv',     'Numpy array',                     partial(np.loadtxt,unpack=False,skiprows=1,delimiter=','))),
            ('structarray' , ('csv',     'Numpy structured / record array', partial(np.genfromtxt,dtype=float,delimiter=',',names=True))),
            ('pandas'      , ('csv',     'Pandas dataframe',                read_csv)),
            ('table'       , ('csv',     'Astropy Table',                   partial(Table.read,format='csv'))),
            ('votable'     , ('votable', 'Astropy VOtable',                 parse_single_table))
        ])
    self.fmtmapping = mapping

def reformatQueryOutput(self, res=None, fmt='csv', verbose=True):
    ''' Reformat the output of a query based on a format.
    '''

    # Not enough inputs
    if res is None:
        print ("Syntax - reformatQueryOutput(dl, results, fmt='csv'")
        return ""
    
    # Add the mapping information if not already loaded
    if self.fmtmapping is None:
        addFormatMapping(self)
    mapping = self.fmtmapping
        
    # Check that this format is supported
    if fmt not in mapping.keys():
        print ("Format %s not supported." % fmt)
        return ""
            
    # Convert to the desired format
    #s = StringIO(res)
    s = BytesIO(res.encode())
    output = mapping[fmt][2](s)
    if verbose is True:
        print ("Returning %s" % mapping[fmt][1])
    return output

def convert_vospace_time_to_seconds(str_date):
    """A convenience method that takes a string from a vospace time field and converts it to seconds since epoch.

    :param str_date: string to parse into a VOSpace time
    :type str_date: str
    :return: A datetime object for the provided string date
    :rtype: datetime
    """
    right = str_date.rfind(":") + 3
    mtime = time.mktime(time.strptime(str_date[0:right], '%Y-%m-%dT%H:%M:%S'))
    #return mtime - time.mktime(time.gmtime()) + time.mktime(time.localtime())  # returns wrong time zome time
    return mtime

def getNodeInfo(self, xnode, lenpathbase, verbose=True):
    ''' Get information on a node.  The input is a "node" element
        of a XML ElementTree.
    '''
    # Gather up all the necessary information for this node
    vals = {'uri':'', 'type':'', 'length':'', 'MD5':'',
            'target':'', 'date':'', 'ispublic':'', 'caps':'',
            'groupread':None, 'groupwrite':None, 'is_public':None,
            'name':'', 'verbosename':'', 'size':'', 'permissions':''}
    vals['uri'] = xnode.get('uri')
    vals['type'] = xnode.get('{http://www.w3.org/2001/XMLSchema-instance}type')
    abspathbase = 'vos://datalab.noao!vospace/'+getUserName(self)+'/'
    lenabspathbase = len(abspathbase)
    vals['name'] = vals['uri'][lenpathbase:]
    # Gather more information for verbose output
    if verbose is True:
        # Loop over properties/accepts/provides/capabilities/nodes
        for p in xnode:
            if (p.tag.endswith('target') is True):
                vals['target'] = p.text
            # Loop over "children"
            for ch in p:
                if (p.tag.endswith('properties') is True) and (len(p) > 0):
                    churi = ch.get('uri')
                    n = churi.split('#')[1]
                    vals[n] = ch.text                          
                if (p.tag.endswith('capabilities') is True) and (len(p) > 0):
                    churi = ch.get('uri')
                    cap = churi.split('#')[1]
                    if vals['caps'] == '':
                        vals['caps'] = cap
                    else:
                        vals['caps'] = vals['caps']+','+cap
        # Parse the information a bit more
        vals['verbosename'] = vals['name']
        if vals['type'] == 'vos:ContainerNode':
            vals['verbosename'] += '/'
        if vals['type'] == 'vos:LinkNode':
            target = vals['target'][lenabspathbase:]
            vals['verbosename'] += ' -> /'+target   # absolute path
        size = vals['length']
        if (type(size) is int) or (type(size) is str and size.isdigit() is True):
            size = storeClient.sizeof_fmt(int(size))
        vals['size'] = size
        # Better date
        modified_time = convert_vospace_time_to_seconds(vals['date'])
        vals['time'] = time.strftime("%d %b %Y %H:%M:%S", time.localtime(modified_time))
        # Create the permissions string
        perm = []
        for i in range(10):
            perm.append('-')
        perm[1] = 'r'
        perm[2] = 'w'
        if vals['type'] == "vos:ContainerNode":
            perm[0] = 'd'
        if vals['type'] == "vos:LinkNode":
            perm[0] = 'l'
        #if self.props.get('ispublic', "false") == "true":
        if vals['ispublic'] == "true":
            perm[-3] = 'r'
            perm[-2] = '-'
        #write_group = self.props.get('groupwrite', '') # MJG
        #if write_group != '':
        if vals['groupwrite'] != '':
            perm[5] = 'w'
        #read_group = self.props.get('groupread', '')
        #if read_group != '':
        if vals['groupread'] != '':
            perm[4] = 'r'
        vals['permissions'] = ''.join(perm)
    # Return the dictionary of values
    return vals

def writeAscii(filename, txt):
    ''' Write data to ASCII file
    '''
    fd = open(filename,'wb')
    fd.write(txt)
    fd.flush()
    fd.close()

def readAscii(filename):
    '''  Read in an ASCII file and return the data
    '''
    if type(filename) is str:
        fd = open(filename, 'r')
    else:
        fd = filename
    data = fd.read()
    fd.close()
    return data

def convertTableToFormat(t,format):
    '''  Convert Astropy table to a different format using StringIO and write method.
    '''
    out = StringIO()
    t.write(out, format = format)
    return out.getvalue()

#class Node:
#    '''
#       A class to hold node information.
#    '''
#    def __init__(self, xnode):
#        self.uri = None
#        self.type = None
#        self.length = None
#        self.MD5 = None
#        self.target = ''
#        self.date = ''
#        self.groupread = None
#        self.groupwrite = None
#        self.is_public = None
#        self.islocked = None
#        self.caps = ''
#        self.name = ''
#        self.verbosename = ''
#        self.size = ''
#        self.permissions = ''
#       
#    def isdir(self):
#        '''  Determine if this node is a directory.
#        '''
#        return (True if self.type == 'vos:ContainerNode' else False)
#        
#    def islink(self):
#        ''' Determine if this node is a link.
#        '''
#        return (True if self.type == 'vos:LinkNode' else False)
#        
#    def getPerms(self):
#        ''' Get the permissions string.
#        '''
#        perm = []
#        for i in range(10):
#            perm.append('-')
#        perm[1] = 'r'
#        perm[2] = 'w'
#        if self.type == "vos:ContainerNode":
#            perm[0] = 'd'
#        if self.type == "vos:LinkNode":
#            perm[0] = 'l'
#        #if self.props.get('ispublic', "false") == "true":
#        if self.ispublic == "true":
#            perm[-3] = 'r'
#            perm[-2] = '-'
#        #write_group = self.props.get('groupwrite', '') # MJG
#        #if write_group != '':
#        if self.groupwrite != '':
#            perm[5] = 'w'
#        #read_group = self.props.get('groupread', '')
#        #if read_group != '':
#        if self.groupread != '':
#            perm[4] = 'r'
#        return string.join(perm, '')
       
        
class DLInteract:
    '''
       Main class for Data Lab interactions
    '''
    
    def __init__(self):
        self.home = '%s/.datalab' % os.path.expanduser('~')

        # Check that $HOME/.datalab exists
        if not os.path.exists(self.home):
            os.makedirs(self.home)

        # See if datalab conf file exists
        self.config = ConfigParser.RawConfigParser(allow_no_value=True)
        if not os.path.exists('%s/dl.conf' % self.home):
            self.config.add_section('datalab')
            self.config.set('datalab', 'created', time.strftime(
                '%Y-%m-%d %H:%M:%S', time.gmtime()))
            self.config.add_section('login')
            self.config.set('login', 'status', 'loggedout')
            self.config.set('login', 'user', '')
            self.config.add_section('vospace')
            self.config.set('vospace', 'mount', '')
            self._write()
        else:
            self.config.read('%s/dl.conf' % self.home)

        # Set script variables
        CAPS_DIR = os.getenv('VOSPACE_CAPSDIR', '../caps')


    def save(self, section, param, value):
        ''' Save the configuration file.
        '''
        if not self.config.has_section(section):
            self.config.add_section(section)
        self.config.set(section, param, value)
        self._write()


    def get(self, section, param):
        ''' Get a value from the configuration file.
        '''
        return self.config.get(section, param)


    def _write(self):
        ''' Write out the configuration file to disk.
        '''
        with open('%s/dl.conf' % self.home, 'w') as configfile:
            self.config.write(configfile)


#### Main Data Lab Interface Class and method #####

class Dlinterface:
    '''
       Data Lab python interface super-class with methods for each command.
    '''
    def __init__(self, verbose=True):
        dlinteract = DLInteract()
        self.dl = dlinteract
        self.loginstatus = "loggedout"
        self.loginuser = ""
        #self.logintoken = ""
        self.verbose = verbose
        self.fmtmapping = None
        self.qhistory = None
        if verbose is True:
            print ("Welcome to the Data Lab python interface.  Type dl.help() for help.")

    '''
       Print method, just print the help
    '''
    def __str__(self):
        self.help()
        return " "

    
#### HELP ########

    def help(self, command=None):
        '''
        Print out useful help information on the Data Lab python interface and it's commands.
        '''

        # Print out general help information
        if command is None:
            print ("The Data Lab python interface.")
            print (" ")
            print ("The available commands are:")
            print (" ")
            print ("dl.help()      - Helpful information")
            print ("Use dl.help(<command>) for specific help on a command.")
            print (" ")
            print ("-- Login and authentication --")
            print ("dl.login()          - Login to the Data Lab")
            print ("dl.logout()         - Logout of the Data Lab")
            print ("dl.status()         - Report on the user status")
            print ("dl.whoami()         - Print the current active user")
            print ("dl.servicestatus()  - Report on the status of the DL services")
            print (" ")
            print ("-- File system operations --")
            print ("dl.ls()        - List a location in Data Lab VOSpace")
            print ("dl.get()       - Get a file from Data Lab VOSpace")
            print ("dl.put()       - Put a file into Data Lab VOSpace")
            print ("dl.cp()        - Copy a file in Data Lab VOSpace")
            print ("dl.mv()        - Move a file in Data Lab VOSpace")
            print ("dl.rm()        - Delete a file in Data Lab VOSpace")
            print ("dl.mkdir()     - Create a directory in Data Lab VOSpace")
            print ("dl.rmdir()     - Delete a directory in Data Lab VOSpace")
            print ("dl.ln()        - Link a file in Data Lab VOSpace")
            print ("dl.load()      - Load data from a local or VOSpace file")
            print ("dl.save()      - Save data to a local or VOSpace file")
            print ("dl.copyurl()   - Copy a file from a URL to Data Lab VOSpace")
            print (" ")
            print ("-- Query and database operations --")
            print ("dl.query()          - Query a remote data service in the Data Lab")
            print ("dl.queryhistory()   - List history of queries made")
            print ("dl.queryresults()   - Get the async query results")
            print ("dl.querystatus()    - Get an async query job status")
            print ("dl.queryprofiles()  - List the available query profiles")
            print ("dl.schema()         - Get information on database schemas")
            print ("dl.droptable()      - Drop a user MyDB table")
            print ("dl.exporttable()    - Copy a user MyDB table to a VOSpace CSV file")
            print ("dl.listdb()         - List the user MyDB tables")
            print ("dl.siaquery()       - Query a SIA service in the Data Lab")

         # Help on a specific command
        else:
            cmd = getattr(self, command, None)
            if cmd is not None:
                #print cmd.__doc__
                help(cmd)
            else:
                print ("%s is not a supported command." % command)

    def servicestatus(self):
        '''
        This checks on the status of the DL services.
        '''

        # Check the Auth Manager
        if areLoginsWorking() is True:
            print ("Authentication - ALIVE")
        else:
            print ("Authentication -  DEAD")

        # Check that SYNC queries are working
        if areSyncQueriesWorking() is True:
            print ("SYNC queries -   ALIVE")
        else:
            print ("SYNC queries -    DEAD")

        # Check that ASYNC queries and TAP are working
        if isTapWorking() is True:
            print ("ASYNC queries -  ALIVE")
        else:
            print ("ASYNC queries -   DEAD")

        # Check the Storage Manager
        if isListWorking() is True:
            print ("Storage -        ALIVE")
        else:
            print ("Storage -         DEAD")

        
################################################
#  Account Login Tasks
################################################

    
    def login(self, user=None):
        '''
        Login to Data Lab using username.

        Parameters
        ----------
        user : str
            The Data lab username.  If this is not given, then the user will be
            prompted for the information.

        Example
        -------

        .. code-block:: python

        Login and give the username,

            dl.login('myusername')
            Enter password: *******
            Welcome to the Data Lab, myusername

        or,

            dl.login()
            Enter user: myusername
            Enter password: ******
            Welcome to the Data Lab, myusername

        '''
        # Check if we are already logged in.  The 'user' field of the 
        # configuration contains the currently active user and token,
        # however previous logins will have preserved tokens from other
        # accounts we may be able to use.
        DOLOGIN = True                             # login by default
        # Already logged in
        if self.loginstatus == "loggedin":
            _user = self.dl.get("login", "user")
            # Same username
            if user == _user:
                # See whether current token is still valid for this user.
                _token = self.dl.get("login", "authtoken")
                if not authClient.isValidToken (_token):
                    print ("Current token for User '%s' no longer valid.  Please login again." % user)
                    DOLOGIN = True
                else:
                    DOLOGIN = False
                    print ("User '%s' is already logged in to the Data Lab" % user)
            elif user is None:
                DOLOGIN = False
                print ("User '%s' is already logged in to the Data Lab" % user)
            # Different username
            else:

                # We're logging in as a different user.
                print ("You are currently logged in as user '%s'. Switching to %s." % (_user, user))
                DOLOGIN = True
        # Not logged in
        else:
            DOLOGIN = True

        # Do the login via the authClient
        if DOLOGIN is True:
            if user == None or user == '':
                user = raw_input('Enter user: ')
            if user == 'anonymous':
                if self.loginstatus == 'loggedin':   # logout previous user first
                    self.logout(verbose=False)
                token = authClient.login('anonymous','')
                self.loginuser = user
            else:
                token = authClient.login(user,getpass.getpass(prompt='Enter password: '))
       
                if not authClient.isValidToken(token):
                    print ("Invalid user name and/or password provided. Please try again.")
                    return
                else:
                    self.loginuser = user
                    print ("Welcome to the Data Lab, %s" % user)
                    #print "Authentication successful."
                    self.dl.save("login", "status", "loggedin")
                    self.dl.save("login", "user", user)
                    self.dl.save("login", "authtoken", token)
                    self.dl.save(user, "authtoken", token)
                    self.loginstatus = "loggedin"
                    #self.user = user
                    #self.token = token
        return

    def logout(self, unmount=None, verbose=True):
        '''
        Logout out of the Data Lab.

        Example
        -------

        Logout of Data Lab.

        .. code-block:: python

            dl.logout()
            'myusername' is now logged out of the Data Lab

        '''

        if self.loginstatus == 'loggedout':
            print ("No user is currently logged into the Data Lab")
            return
        else:
            token = getUserToken(self)
            user, uid, gid, hash = token.strip().split('.', 3)

            res = authClient.logout (token)
            if res != "OK":
                print ("Error: %s" % res)
                return
            self.dl.save("login", "status", "loggedout")
            self.dl.save("login", "user", "")
            self.dl.save("login", "authtoken", "")
            if verbose is True:
                print ("'%s' is now logged out of the Data Lab" % user)
            self.loginstatus = "loggedout"
            #self.user = ""
            #self.token = ""


    def status(self):
        ''' 
        Print the status of the Data Lab connection.

        Example
        -------

        The "myusername" is logged in.

        .. code-block:: python
     
            dl.status()
            User myusername is logged into the Data Lab

        No user is currently logged in.

        .. code-block:: python
     
            dl.status()
            No user is currently logged into the Data Lab

        '''

        if self.loginstatus == "loggedout":
            print ("No user is currently logged into the Data Lab")
        else:
            print ("User %s is logged into the Data Lab" % \
                    self.dl.get("login", "user"))

    def whoami(self):
        '''
        Print the current active user.

        Example
        -------

        .. code-block:: python
     
            dl.whoami()
            myusername

        '''
        print (getUserName(self))


################################################
#  Query Manager Tasks
################################################


    def query(self, query=None, qtype='sql', fmt='csv', out=None, async=False, profile='default', verbose=True):
        '''
        Send a query to a remote query service.

        Parameters
        ----------
        query : str
            The query string that will be passed to the queryClient and then
            to the DB query manager.  This can either be in the SQL or
            ADQL format (specified by the "type" parameter).  For example,

            .. code-block:: python

                'select ra,dec from gaia_dr1.gaia_source limit 3'

        qtype : str
            The query format, SQL or ADQL.  SQL is used by default.

        fmt : str
            Format of the result to be returned by the query. Permitted values are.
              For file output and direct output to python:
              * 'csv'     the returned result is a comma-separated string that looks like a csv file (newlines at the end of every row)
              * 'ascii'   same as csv but tab-delimited
              * 'votable' result is a string XML-formatted as a VO table
              Only for direct output to python:
              * 'array'   Numpy array
              * 'structarray'  Numpy structured / record array
              * 'pandas'  a Pandas data frame
              * 'table'   in Astropy Table format
              Only for file output:
              * 'fits'    FITS binary table.  Only if the results are saved to a file with out=.
              * 'hdf5'    HDF5 file.  Only if the results are saved to a file  with out=.

        out : str or None
            The output name if the results are to be saved to mydb (mydb://tablename), to VOSpace (vos://filename),
            or the local file system (file:// and other names with no prefix).  The files are in csv format.

        async : bool
            If ``True``, the query is asynchronous, i.e. a job is
            submitted to the DB, and a jobID is returned. The jobID
            must be then used to check the query's status and to retrieve
            the result (when status is ``COMPLETE``). Default is
            ``False``, i.e. synchroneous query.

        Returns
        -------
        result : str
            If ``async=False`` and ``out`` is not used, then the return value is the result of the query
            in the requested format (see ``fmt``).  If ``out`` is given then the query result is saved to
            a file or mydb.  If ``async=True`` the jobID is returned with which later the asynchronous
            query's status can be checked (:func:`dl.querystatus()`), and the result retrieved (see
            :func:`dl.queryresults()`.

        Example
        -------
        A simple query returned as a pandas data frame.

        .. code-block:: python

            data = dl.query('SELECT * from smash_dr1.source LIMIT 100',fmt='pandas')
            Returning Pandas dataframe

            type(data)
            pandas.core.frame.DataFrame

            print data['ra'][0:3]
            0    103.068355
            1    103.071774
            2    103.071598

        Perform a query and save the results to a table called "table1.txt" in mydb.

        .. code::

            res = dl.query('SELECT * from smash_dr1.source LIMIT 100',out='mydb://table1.txt')

             dl.listmydb()

        Perform the same query and save it to a local file.

        .. code::

            res = dl.query('SELECT * from smash_dr1.source LIMIT 100',out='table1.txt')

            ls
            table1.txt

        '''
        # Not enough information input
        if (query is None):
            print ("Syntax - dl.query(query, qtype='sql|adql', fmt='csv|ascii|array|structarray|pandas|table|votable|fits|hdf5',")
            print ("                  out='', async=False, profile='default')")
            return

        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken (self)
        
        # Check if the source file actually exist
        if out is not None and not out.startswith('mydb://'):
            res = storeClient.ls(token,out,'csv')
            if res != '':
                print ("'%s' already exists." % out)
                return

        # Can only use FITS or HDF for file output
        if (out is None or out == '') and fmt in ['fits','hdf5']:
            print ("Can only use format '%s' for file output." % fmt)
            return

        # Cannot use pandas, array, structarray for file output
        if (out is not None and out != '') and fmt in ['pandas','array','structarray','table']:
            print ("Cannot use format '%s' for file output." % fmt)
            return
        
        # Use QID to rerun a previous query
        if (type(query) is int) or (type(query) is str and query.isdigit() is True):
            queryid = int(query)
            keys = sorted(self.qhistory.keys())
            if ((queryid in keys) is False):           # no QID 
                print ("QID = %s not found" % str(queryid))
                return
            print ("Rerunning QID = %d" % queryid)
            v = self.qhistory[queryid]
            # qid, type, async, query, time, jobid, username, format, status/nrows
            query = v[3]
            print ("Query = '%s'" % query)
            
        # Check type
        if qtype not in ['sql','adql']:
            print ("Only 'sql' and 'adql' queries are currently supported.")
            return
            
        _query = query         # local working copy
        
        # Check if the query is in a file
        if os.path.exists (_query):
                with open (_query, "r", 0) as fd:
                    _query = fd.read (os.path.getsize(_query)+1)
                fd.close()
                
        # What type of query are we doing
        sql = None
        adql = None
        if qtype == 'sql':
            sql = _query
        else:
            adql = _query

        # Add the mapping information if not already loaded
        if (out is None or out == '') and (self.fmtmapping is None):
            addFormatMapping(self)
        mapping = self.fmtmapping
        
        # The queryClient "fmt" will depend on the requested output format
        if (out is None or out == ''):
            try:
                qcfmt = mapping[fmt][0]
            except:
                print ("Format '%s' not supported." % fmt)
                return
        else:
            qcfmt = 'csv'
            
        # Execute the query.
        if profile != "default":
            if profile != "" and profile is not None:
                queryClient.set_profile (profile=profile)

        try:
            res = queryClient.query (token, adql=adql, sql=sql, 
                                     fmt=qcfmt, out=out, async=async)
        except Exception as e:
            if not async and str(e) is not None:
                err = str(e)
                if err.find("Time-out") >= 0:
                    print ("Error: Sync query timeout, try an async query")
                else:
                    print (str(e))
            else:
                print (str(e))
        else:
            # Add this query to the query history
            jobid = None
            status = ''
            if (out is None or out == '') and (not async):   # regular sync query
                status = len(res.split('\n'))-2              # number of rows returned
            if (out is not None) and (out != '') and (not async):
                status = res                                 # sync query to file, vos, or mydb
            if async:
                jobid = res
                status = 'SUBMITTED'
            if self.qhistory is None:
                qid = 1
                self.qhistory = {qid : (qid, qtype, async, _query, time.time(), jobid, getUserName(self), fmt, status)}
            else:
                qid = int(max(self.qhistory.keys())) + 1
                self.qhistory[qid] = (qid, qtype, async, _query, time.time(), jobid, getUserName(self), fmt, status) 
            
            # Return the results
            
            # Asynchronous
            if async:
                print ("Asynchronous query JobID = %s " % res)                # Return the JobID
                return res
            # Synchronous
            elif out == '' or out is None:
                # Convert to the desired format
                return reformatQueryOutput(self,res,fmt,verbose=verbose)
                    
                
    def queryhistory(self, async=None):
        '''
        Report the history of queries made so far.

        Parameters
        ----------
        async : bool
            A boolean (True/False) of whether to only show the ASYNC queries.
            By default all quries are shown.

        Results
        -------
        The information on part queries is output to the screen with the following
        columns: query ID, submission time, query type (sql/adql), sync or async query, jobid (for async queries),
                 output format, status of query (or number of returned rows if sync query), query string

        Examples
        --------
        Perform some queries and then list the history.

        .. code-block:: python

            data1 = dl.query('select ra,dec from smash_dr1.source limit 100',fmt='csv')
            Returning CSV formatted table as a string

            data2 = dl.query('select ra,dec from smash_dr1.source limit 500',fmt='pandas')
            Returning Pandas dataframe

            data3 = dl.query('select ra,dec from smash_dr1.source limit 1000',fmt='structarray')
            Returning Numpy structured / record array

            dl.queryhistory()
            1  2017-05-16 13:27:34  sql  SYNC  pandas  100  --  'select ra,dec,gmag from smash_dr1.object limit 100'
            2  2017-05-16 13:27:40  sql  SYNC  csv  500  --  'select ra,dec,gmag from smash_dr1.object limit 500'
            3  2017-05-16 13:27:46  sql  SYNC  structarray  1000  --  'select ra,dec,gmag from smash_dr1.object limit 1000'

        '''
        if self.qhistory is None:
            print ("No queries made so far")
            return
        else:
            keys = sorted(self.qhistory.keys())
            # Only async request, make sure we have some
            if async is True:
                asyncv = []
                for k in keys:
                    v = self.qhistory[k]
                    asyncv.append(v[2])
                if sum(asyncv) == 0:
                    print ("No ASYNC queries made so far")
                    return
                
            # Loop through the query history
            print ("-------------------------------------------------------------------------------------------------------------------")
            print ("QID          DATE        Type  A/SYNC  Format       Status          JobID                   Query")
            print ("-------------------------------------------------------------------------------------------------------------------")
            for k in keys:
                # qid, type, async, query, time, jobid, username, format, status/nrows
                v = list(self.qhistory[k])    # convert to list
                # Get the query status for ASYNC queries
                if v[2] is True:
                    jobid = v[5]
                    token = getUserToken(self)
                    stat = queryClient.status(token, jobId=jobid)
                    v[8] = stat
                if (async is True and v[2] == True) or (async is not True):
                    print ("%-3d  %-19s  %-4s  %-5s  %-11s  %-10s  %-18s  '%-s'" %
                           (v[0], time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(v[4])),
                            v[1], 'ASYNC' if v[2] else 'SYNC', v[7], 
                            str(v[8]), v[5] if v[2] else "--", ' '.join(v[3].split())))
            print ("-------------------------------------------------------------------------------------------------------------------")
                    # Maybe leave off the jobid if we are using QID instead??!!

                
    def querystatus(self, jobid=None):
        '''
        Get the async query job status.

        Parameters
        ----------
        jobid : str
            This can be either (1) the Query ID (QID) returned by the dl.queryhistory() command, or
            (2) the unique job identifier for the asynchronous query which was returned by :func:`ql.query`
            when the query job was submitted.


        Returns
        -------
        status : str
            The status of the query, which can be one of the following:
            ``QUEUED``     the query job is in queue and waiting to be executed.
            ``EXECUTING``  the query job is currently running.
            ``COMPLETED``  the query is done and the results are ready to be retrieved with :func:`dl.queryresults()`.
            ``ERROR``      there was a problem with the query.

        Example
        -------

        Submit an asynchronous query and then check the status.

        .. code-block:: python
     
            jobid = dl.query('SELECT ra,dec from smash_dr1.source LIMIT 100',async=True)
            Asynchronous query JobID = uqrcs8a5n8s6d0je 

            dl.querystatus(jobid)
            COMPLETED

        '''
        # Not enough information input
        if (jobid is None):
            print ("Syntax - dl.querystatus(jobId/QID)")
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Was a QID or JobId input?
        _jobid = jobid      # assume a jobid was input
        # QID was input
        if (type(jobid) is int) or (type(jobid) is str and jobid.isdigit() is True):
            keys = sorted(self.qhistory.keys())
            if ((int(jobid) in keys) is False):           # no QID 
                print ("QID = %s not found" % str(jobid))
                return
            v = self.qhistory[int(jobid)]
            # qid, type, async, query, time, jobid, username, format, status/nrows
            if v[2] is False:         # not an async query
                print ("QID = %s is not an ASYNC query" % str(jobid))
                return
            _jobid = v[5]
        # Get the status
        print (queryClient.status (token, jobId=_jobid))

    def queryresults(self, jobid=None):
        '''
        Get the async query results.

        Parameters
        ----------
        jobid : str
             This can be either (1) the Query ID (QID) returned by the dl.queryhistory() command, or
             (2) the unique job identifier for the asynchronous query which was returned by :func:`ql.query`
             when the query job was submitted.

        Returns
        -------
        result : str
            The result of the query in the requested format (see ``fmt`` in :func:`dl.query`.

        Example
        -------

        Submit an asynchronous query and then check the status.

        .. code-block:: python
     
            jobid = dl.query('SELECT ra,dec from smash_dr1.source LIMIT 3',async=True)
            Asynchronous query JobID = uqrcs8a5n8s6d0je 

            dl.querystatus(jobid)
            COMPLETED

            results = dl.queryresults(jobid)
            print results
            ra,dec
            103.068354922718,-37.973538878907299
            103.071774116284,-37.973599429479599
            103.071597827998,-37.972329108796401

        '''
        # Not enough information input
        if (jobid is None):
            print ("Syntax - dl.queryresults(jobId/QID)")
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Was a QID or JobId input?
        _jobid = jobid      # assume a jobid was input
        # QID was input
        if (type(jobid) is int) or (type(jobid) is str and jobid.isdigit() is True):
            keys = sorted(self.qhistory.keys())
            if ((int(jobid) in keys) is False):           # no QID 
                print ("QID = %s not found" % str(jobid))
                return
            v = self.qhistory[int(jobid)]
            # qid, type, async, query, time, jobid, username, format, status/nrows
            if v[2] is False:         # not an async query
                print ("QID = %s is not an ASYNC query" % str(jobid))
                return
            _jobid = v[5]
            fmt = v[7]
        # JobID input, get the output format
        else:
           keys = sorted(self.qhistory.keys())
           for k in keys:
               v = self.qhistory[k]
               # qid, type, async, query, time, jobid, username, format, status/nrows
               if v[5] == jobid:
                   fmt = v[7]
                   break

        # Check first if the job has been completed
        stat = queryClient.status (token, jobId=_jobid)
        if stat != 'COMPLETED':
            print ("The job has not yet completed")
            return
        # Get the results
        res = (queryClient.results (token, jobId=_jobid))
        # Convert to the desired format
        return reformatQueryOutput(self,res,fmt,verbose=True)
        
        
    def listdb(self, table=''):
        '''
        List the user's MyDB tables.

        Parameters
        ----------
        table : str
             The name of a specific table in mydb.  If this is blank then all tables will be listed.

        Returns
        -------
        list : str
            The list of properties of ``table`` or all tables in mydb.

        Example
        -------

        List the MyDB tables.

        .. code-block:: python
     
            print dl.listmydb()
            table
            table2

        '''
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        try:
            res = queryClient.list (token, table=table)
        except Exception as e:
            print ("Error listing MyDB tables.")
            print (str(e))
        else:
            if res == 'relation "" not known':
                print ("No tables in MyDB")
                res = ''
            else:
                res = ' '.join(res.splitlines())    # convert to space separated list
            return res
            
    def droptable(self, table=None):
        '''
        Drop a user's MyDB table.

        Parameters
        ----------
        table : str
             The name of a specific table in mydb to drop.

        Returns
        -------
        list : str
            The list of properties of ``table`` or all tables in mydb.

        Example
        -------

        Drop the MyDB table called ``table``.

        .. code-block:: python
     
            print dl.listdb()
            table
            table2

            dl.droptable('table')
            table
            table2

            print dl.listdb()
            table2

        '''
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        try:
            queryClient.drop (token, table=table)
        except Exception as e:
            print ("Error dropping table '%s'." % table)
            print (str(e))
        else:
            print ("Table '%s' was dropped." % table)

    def exporttable(self, table=None, name=None, fmt=None):
        '''
        Copy a user's MyDB table to a file in VOSpace.

        Parameters
        ----------
        table : str
             The name of a specific table in mydb to drop.

        name : str
             The file name to save the table to.

        fmt : str
             The output file format.  The available formats are 'csv', 'fits' and 'hdf5'.
             If this is not specified then the file extension is used to identify the
             format type.

        Example
        -------

        Export the MyDB table called ``table`` to file ``test.csv``.

        .. code-block:: python
     
            dl.exporttable('table','test.csv')

        '''
        # Not enough information input
        if (table is None or name is None):
            print ("Syntax - dl.exporttable(table,name,fmt)")
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        if not name.startswith('vos://'): name = ("vos://" + name)
        # Check if the table exists
        res = queryClient.list (token, table='')
        if table not in res.splitlines():
            print ("Table '%s' not found." % table)
            return
        # Check if the file exists already
        res = storeClient.ls(token,name,'csv')
        if res != '':
            print ("'%s' already exists." % name)
            return
        # Figure out the format type
        if fmt is None:
            fbase, fext = os.path.splitext(name)
            fmtmap = { '.fits':'fits', '.hdf5':'hdf5', '.csv':'csv' }
            try:
                fmt = fmtmap[fext]
            except:
                print ("Format '%s' not supported. Using 'csv' instead." % fmt)
                fmt = 'csv'
        # Make sure the fmt is supported
        if fmt not in ['fits','hdf5','csv']:
            print ("Format '%s' not supported." % fmt)
            return
        # Make the MyDB query and output to VOSpace
        try:
            res = queryClient.query (token, sql='select * from mydb://'+table,out=name,fmt=fmt)
        except Exception as e:
            print ("Error exorting table '%s'." % table)
            print (str(e))
        else:
            print ("Table '%s' was copied to '%s'." % (table, name))

        
    def queryprofiles(self, profile=None):
        '''
        List the available Query Manager profiles to use with a :func:`dl.query`.

        Parameters
        ----------
        profile : str
             The name of a specific Query Manager profile to check.  If this is blank
             then all of the available profile names will be listed.

        Returns
        -------
        results : str
            The list of properties of profile ``profile``, or a list of all available profiles.

        Example
        -------

        List of available profiles.

        .. code-block:: python
     
            dl.queryprofiles()
            default,IRSA,HEASARC,Vizier,GAVO,SIMBAD,zeus1,SDSS-DR9,STScI-RegTAP,GALEX-DR6,dldb1

        Get profile information on profile ``dldb1``.

            dl.queryprofiles('dldb1')
            {u'accessURL': u'http://dldb1.sdm.noao.edu:8080/ivoa-dal/tap', u'dbport': 5432, u'password':
            u'datalab', u'description': u'Development NOAO Data Lab TAP Service / Database on dldb1',
            u'database': u'tapdb', u'host': u'dldb1.sdm.noao.edu', u'vosRoot': u'vos://datalab.noao!vospace',
            u'vosEndpoint': u'http://dldb1.sdm.noao.edu:8080/vospace-2.0/vospace', u'user': u'dlquery',
            u'vosRootDir': u'/data/vospace/users', u'type': u'datalab'}

        '''
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        print (queryClient.list_profiles (token, profile=profile))


    def schema(self, val='', fmt='text', profile='default'):
        '''
        Print information about data service schema.

        Parameters
        ----------
        val : str
            Value to list ([[<schema>][.<table>][.<col>]]).

        fmt: str
            Output format (csv|text|json).

        profile : str
            Service profile.

        Returns
        -------
        results : str
            The schema information is printed to the screen.

        Example
        -------

        Print out all the DL tables.

        .. code-block:: python
     
            datalab schema 

            Schema Name   Description
           -----------   -----------
              gaia_dr1   GAIA Data Release 1
                  ivoa   IVOA ObsCore tables
              des_sva1   DES SVA1 Data Products
            tap_schema   TAP Schema Tables
                  usno   USNO Astrometry Catalogs
             sdss_dr13   
               neo_dr1   NEO Survey Data Release 1
                ls_dr3   The DECam Legacy Survey Data Release 3
             smash_dr1   SMASH Data Release 1


        List all tables in a schema/catalog.

        .. code-block:: python
     
            datalab schema val=smash_dr1

            Schema: smash_dr1

            Table Name   Description
            ----------   -----------
                  chip   Info on each chip in the frame
              exposure   Info on each exposure
                 field   Info on each target field (position, Num exposures, etc)
                object   Average photometry of each unique object
                source   All of the individual source measurements
                 stars   View of object table to select for stars
              galaxies   View of object table to select for galaxies
                xmatch   Crossmatch of object against GAIA DR1 and WISE

        '''

        print (queryClient.schema (value=val, format=fmt, profile=profile))
        

################################################
#  Storage Manager Tasks
################################################
        
        
    def ls(self, name='vos://', format='csv', verbose=False):
        '''
        List files in VOSpace.

        Parameters
        ----------
        name : str
             The name of a specific file to list.  If name is blank then all files will be listed.

        format : str
             The format to use.

        verbose: bool
             Give more verbose output, or just a list of files.  The default is verbose=True.

        Returns
        -------
        results : str
            The list of files in VOSpace.

        Example
        -------

        List the files.

        .. code-block:: python
     
            dl.ls()
            test2  test1 

        Verbose listing of the files in the ``public/`` directory.

        .. code-block:: python
     
            dl.ls('public',verbose=True)
            lrw-rw----  demo15      0B  17 May 2017 14:04:25  thisisalsoalink -> /public/smash2  
            lrw-rw----  demo15      0B  17 May 2017 13:58:04  thisisalink -> /smash1  
            -rw-rw-r--  demo15    3.4K  17 May 2017 09:40:13  smash2  
            -rw-rw-r--  demo15    3.4K  17 May 2017 07:34:54  smash1  
            drw-rw----  demo15      0B  17 May 2017 14:05:02  data/  tableingester,downloader,runner

        '''
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception ("Invalid user name and/or password provided. Please try again.")
        # Trim trailing / or /*, messes up directory listing
        if name != 'vos://':
            name = (name if not name.endswith('/') else name[:-1])
            name = (name if not name.endswith('/*') else name[:-2])
        # Run the LS command
        res = storeClient.ls (token, name=name, format='raw')
        root = ET.fromstring('<data>'+res+'</data>')
        pathbase = 'vos://datalab.noao!vospace/'+getUserName(self)+'/'
        # Check if this is a directory listing
        if (len(root) == 1) and (root[0].attrib['{http://www.w3.org/2001/XMLSchema-instance}type'] == 'vos:ContainerNode'):
            pathbase = root[0].attrib['uri']+'/'
            for k in root[0]:
                if (k.tag.endswith('nodes') is True):
                    root = k     # make the "nodes" the new root
                    break
        lenpathbase = len(pathbase)
        if verbose is False:     # start output string list
            flist = []
        # Loop over nodes
        user = getUserName(self)
        for node in root:
            # Gather up all the necessary information for this node
            vals = getNodeInfo(self, node, lenpathbase, verbose=verbose)
            if verbose is True:
                # Now print out the information          
                print ("%10s  %s  %6s  %s  %s  %s" % (vals['permissions'], user, vals['size'],
                                                  vals['time'], vals['verbosename'], vals['caps']))
            else:
                # Add trailing "/" for directories
                name = (vals['name']+'/' if vals['type']=='vos:ContainerNode' else vals['name'])
                flist.append("%s " % name)
        if verbose is False:
            print (' '.join(flist))

    def get(self, source=None, destination=None, verbose=True):
        '''
        Get one or more files from Data Lab.

        Parameters
        ----------
        source : str
             The name of the source file on VOSpace, e.g. ``file2.txt``.

        destination : str
             The name of the local destination file, e.g. ``file1.txt``.

        Example
        -------

        Get a query output table called ``table1_output.txt`` from VOSpace.

        .. code-block:: python
     
            dl.get('table1_output.txt','table1_output.txt')
            (1/1) [====================] [   9.1K] table1_output.txt

        '''
        # Not enough information input
        if (source is None) or (destination is None):
            print ("Syntax - dl.get(source, destination)")
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception ("Invalid user name and/or password provided. Please try again.")
        # Run the GET command
        storeClient.get (token, source, destination,
                            verbose=verbose)

    def put(self, source=None, destination=None, verbose=True):
        '''
        Put files into Data Lab VOSpace.

        Parameters
        ----------
        source : str
             The name of a local file to upload to VOSpace, e.g. ``file1.txt``.

        destination : str
             The name of the destination file with, e.g. ``file2.txt``.  The destination
             file can have the vos:// prefix but it is not required.

        Example
        -------

        Put a catalog called ``cat.fits`` into VOSpace.

        .. code-block:: python
     
            dl.put('cat.fits','cat.fits')
            (1 / 1) cat.fits -> vos://cat.fits

        '''
        # Not enough information input
        if (source is None) or (destination is None):
            print ("Syntax - dl.put(source, destination)")
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception ("Invalid user name and/or password provided. Please try again.")
        # Run the PUT command
        res = storeClient.put (token, source, destination,
                               verbose=verbose)
        if res == '[<Response [200]>]':   # Return None if nothing to give
            res = None
        return res
        
    def mv(self, source=None, destination=None, verbose=True):
        '''
        Move a file in Data Lab VOSpace.

        Parameters
        ----------
        source : str
             The name the file in VOSpace to move/rename, e.g. ``file1.txt``.

        destination : str
             The new name of the file in VOSpace (e.g. ``newfile1.txt``) or the
             directory to move it to.

        Example
        -------

        Rename the file ``file.txt`` to ``newfile.txt``.

        .. code-block:: python
     
            dl.ls()
            file.txt

            dl.mv('file.txt','newfile.txt')

            dl.ls()
            newfile.txt

        Move the file ``output.fits`` to the ``results/`` directory.

        .. code-block:: python
     
            dl.ls()
            output.txt, results

            dl.mv('output.fits','results/output.fits')

            dl.ls()
            results/output.txt

        '''
        # Not enough information input
        if (source is None) or (destination is None):
            print ("Syntax - dl.mv(source, destination)")
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception ("Invalid user name and/or password provided. Please try again.")
        # Check if the source file actually exist
        res = storeClient.ls(token,source,'csv')
        if res == '':
            print ("'%s' not found" % source)
            return
        # Run the MV command
        storeClient.mv (token, fr=source, to=destination,
                        verbose=verbose)

    def cp(self, source=None, destination=None, verbose=True):
        '''
        Copy a file in Data Lab VOSpace.

        Parameters
        ----------
        source : str
             The name of the file in VOSpace to copy, e.g. ``file1.txt``.

        destination : str
             The new name of the file in VOSpace, e.g. ``newfile1.txt``.

        Example
        -------

        Copy the file ``file.txt`` to ``newfile.txt``.

        .. code-block:: python
     
            dl.ls()
            file1.txt

            dl.cp('file1.txt','newfile.txt')

            dl.ls()
            file1.txt, newfile.txt

        '''
        # Not enough information input
        if (source is None) or (destination is None):
            print ("Syntax - dl.cp(source, destination)")
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception ("Invalid user name and/or password provided. Please try again.")
        # Check if the source file actually exist
        res = storeClient.ls(token,source,'csv')
        if res == '':
            print ("'%s' not found" % source)
            return
        # Run the CP command
        storeClient.cp (token, fr=source, to=destination,
                        verbose=verbose)

    def rm(self, name=None, verbose=True):
        '''
        Delete files in Data Lab VOSpace.

        Parameters
        ----------
        name : str
             The name of the file in VOSpace to delete, e.g. ``file1.txt``.

        Example
        -------

        Delete the file ``file1.txt``.

        .. code-block:: python
     
            dl.ls()
            file1.txt, file2.txt

            dl.rm('file1.txt')

            dl.ls()
            file2.txt

        '''
        # Not enough information input
        if (name is None):
            print ("Syntax - dl.rm(name)")
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception ("Invalid user name and/or password provided. Please try again.")
        # Run the RM command
        storeClient.rm (token, name=name, verbose=verbose)

        
    def ln(self, target=None, link=None):
        '''
        Link a file in Data Lab VOSpace.

        Parameters
        ----------
        target : str
             The name of the file in VOSpace to link to, e.g. ``file1.txt``.

        link : str
             The name of the link, e.g. ``file1link``.

        Example
        -------

        Create a link called ``iamlink`` to the file ``file1.txt``.

        .. code-block:: python

            dl.ls()
            file1.txt
     
            dl.ln('file1.txt','iamlink')

            dl.ls()
            file1.txt, iamlink

        '''
        # Not enough information input
        if (link is None) or (target is None):
            print ("Syntax - dl.ln(target, link)")
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception ("Invalid user name and/or password provided. Please try again.")
        # They require vos:// prefix to work
        lnk = (link if link.startswith("vos://") else ("vos://" + link))
        trg = (target if target.startswith("vos://") else ("vos://" + target))
        # Run the LN command
        storeClient.ln (token, fr=lnk, target=trg)

    def mkdir(self, name=None):
        ''' 
        Create a directory in Data Lab VOSpace.

        Parameters
        ----------
        name : str
             The name of the directory in VOSpace to create, e.g. ``results``.

        Example
        -------

        Create the directory ``data1/``.

        .. code-block:: python

            dl.mkdir('data1')

        '''
        # Not enough information input
        if (name is None):
            print ("Syntax - dl.mkdir(name)")
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception ("Invalid user name and/or password provided. Please try again.")
        # Run the MKDIR command
        #  currently this must have vos:// prefix
        if name[0:6] != 'vos://':
            name = 'vos://' + name
        storeClient.mkdir (token, name=name)
        

    def rmdir(self, name=None):
        ''' 
        Delete a directory in Data Lab VOSpace.

        Parameters
        ----------
        name : str
             The name of the directory in VOSpace to delete, e.g. ``results``.

        Example
        -------

        Delete the directory ``data1/``.

        .. code-block:: python

            dl.rmdir('data1')

        '''
        # Not enough information input
        if (name is None):
            print ("Syntax - dl.rmdir(name)")
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception ("Invalid user name and/or password provided. Please try again.")
        # Run the RMDIR command
        storeClient.rmdir (token, name=name)


    def copyurl(self, url=None, name=None):
        ''' 
        Copy a file to VOSpace using a URL.

        Parameters
        ----------
        url : str
             The URL location of the file.

        name : str
             The name of the file in VOSpace.  The vos:// prefix is not necessary.

        Example
        -------

        Copy the file http://www.mywebsite.com/file1.fits to output1.fits in VOSpace.

        .. code-block:: python

            dl.copyurl('http://www.mywebsite.com/file1.fits','output1.fits')

        '''
        # Not enough information input
        if (url is None or name is None):
            print ("Syntax - dl.copyurl(url,name)")
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception ("Invalid user name and/or password provided. Please try again.")
        # Run the LOAD command
        name = (name if name.startswith('vos://') else ('vos://'+name))
        storeClient.load(token, name, url)

             
    def load(self, name=None, inpfmt=None, fmt='pandas', ext=None):
        ''' 
        Save the string representation of a data object to a file in VOSpace.

        Parameters
        ----------
        name : str
             The name of the file in load into memory.  The vos:// prefix is necessary
             otherwise it is assumed a local file that should be read.  Currently only
             FITS binary tables and CSV file formats are supported.
        
        inpfmt : str
             Tne input format type.  The currently supported types are FITS binary tables,
             HDF5, CSV, and ASCII files ('string').  If this is not specified then the
             file extension of ``name`` is used to attempt to figure out the format.

        fmt : str
             The data type format to output.  Permitted values are:
              * 'csv'     the returned result is a comma-separated string that looks like a
                             csv file (newlines at the end of every row)
              * 'ascii'   same as csv but tab-delimited
              * 'string'  just a straight read of ASCII into a string
              * 'array'   Numpy array
              * 'structarray'  Numpy structured / record array
              * 'pandas'  a Pandas data frame
              * 'table'   in Astropy Table format
              * 'votable' result is a string XML-formatted as a VO table
              The output type for a FITS image is a numpy array.  For other data 'pandas' is
              the default format.

        ext : int
             The FITS extension to load for images.  The default is 1 for a FITS binary
             table and 0 for a FITS image.

        Example
        -------

        Load the file "output.fits" into a pandas data frame.

        .. code-block:: python

            df = dl.load('output.fits',fmt='pandas')

        Load a FITS image "im1.fits".

        .. code-block:: python

            im,head = dl.load('im1.fits')

        '''
        # Not enough information input
        if (name is None):
            print ("Syntax - dl.load(name,inpfmt=inpfmt,fmt=fmt,ext=ext)")
            return
        
        # Only fits, csv and string input format currently supported
        if inpfmt != None and inpfmt != '' and inpfmt not in ['fits','hdf5','csv','string']:
            print ("Format '%s' not currently supported for input file." % inpfmt)
            return
                
        # Use file extension to figure out input format
        if inpfmt is None:
            fbase, fext = os.path.splitext(name)
            inpfmtmap = { '.fits':'fits', '.hdf5':'hdf5', '.csv':'csv', '.txt':'string' }
            try:
                inpfmt = inpfmtmap[fext]
            except:
                print ("Cannot use file extension to determine 'inpfmt'")
                return
        
        # Check token if reading from VOSpace
        if name.startswith("vos://"):
            # Check if we are logged in
            if not checkLogin(self):
                return
            token = getUserToken(self)
            # Check that we have a good token
            if not authClient.isValidToken(token):
                raise Exception ("Invalid user name and/or password provided. Please try again.")

        # Check that the file exists
        if name.startswith("vos://"):
            res = storeClient.ls(token,name,'csv')
            if res == '':
                print ("'%s' not found" % name)
                return
        else:
            if os.path.exists(name) is False:
                print ("'%s' not found" % name)
                return  
            
        # Load the neccessary packages
        # astropy fits
        try:
            dum = fits.__doc__
        except:
            from astropy.io import fits
        # astropy Table
        try:
            dum = Table.__doc__
        except:
            from astropy.table import Table
        # astropy votable
        try:
            dum = from_table.__doc__
        except:
            from astropy.io.votable import from_table

            
        # Reading and conversion mapping
        # for reading, x=filename; for conversion, x=data object
        writemap = { 'fits-csv':         (lambda x: Table.read(x,format='fits'),      partial(convertTableToFormat,format='ascii.csv')),
                     'fits-ascii':       (lambda x: Table.read(x,format='fits'),      partial(convertTableToFormat,format='ascii.tab')),
                     'fits-array':       (lambda x: Table.read(x,format='fits'),      lambda x: x.as_array()),
                     'fits-structarray': (lambda x: Table.read(x,format='fits'),      lambda x: x.as_array()),
                     'fits-pandas':      (lambda x: Table.read(x,format='fits'),      lambda x: x.to_pandas()),
                     'fits-table':       (lambda x: Table.read(x,format='fits'),      lambda x: x),
                     'fits-votable':     (lambda x: Table.read(x,format='fits'),      lambda x: from_table(x).resources[0].tables[0]),
                     'hdf5-csv':         (lambda x: Table.read(x,format='hdf5'),      partial(convertTableToFormat,format='ascii.csv')),
                     'hdf5-ascii':       (lambda x: Table.read(x,format='hdf5'),      partial(convertTableToFormat,format='ascii.tab')),
                     'hdf5-array':       (lambda x: Table.read(x,format='hdf5'),      lambda x: x.as_array()),
                     'hdf5-structarray': (lambda x: Table.read(x,format='hdf5'),      lambda x: x.as_array()),
                     'hdf5-pandas':      (lambda x: Table.read(x,format='hdf5'),      lambda x: x.to_pandas()),
                     'hdf5-table':       (lambda x: Table.read(x,format='hdf5'),      lambda x: x),
                     'hdf5-votable':     (lambda x: Table.read(x,format='hdf5'),      lambda x: from_table(x).resources[0].tables[0]),
                     'csv-csv':          (lambda x: readAscii(x),                     lambda x: x),
                     'csv-ascii':        (lambda x: Table.read(x,format='csv'),       partial(convertTableToFormat,format='ascii.tab')),
                     'csv-array':        (partial(np.loadtxt,skiprows=1,delimiter=','), lambda x: x),
                     'csv-structarray':  (lambda x: Table.read(x,format='csv'),       lambda x: x.as_array()),
                     'csv-pandas':       (lambda x: Table.read(x,format='ascii.csv'), lambda x: x.to_pandas()),
                     'csv-table':        (lambda x: Table.read(x,format='ascii.csv'), lambda x: x),
                     'csv-votable':      (lambda x: Table.read(x,format='ascii.csv'), lambda x: from_table(x).resources[0].tables[0]),
                     'string-string':    (lambda x: readAscii(x),                     lambda x: x) }
        # Should add HDF5, use Table.read(x,format='hdf5',path='data'), h5py must be installed
        
        # Check that we can do the operation
        mapcode = inpfmt+'-'+fmt
        if mapcode not in writemap.keys():
            print ("Output format '%s' not supported for input type '%s'" % (fmt, inpfmt) )
            return

        # Open the file
        if name.startswith('vos://'):
            fh = StringIO( storeClient.get(token,name,'',verbose=False) )
        else:
            fh = open(name,'rb')

        # If this is a FITS file, check if its a binary table or image
        if inpfmt == 'fits':
            fitstable = False
            hd = fits.getheader(fh)
            fh.seek(0)    # reset to beginning of file
            if hd['EXTEND'] is True:
                try:
                    tryext = (1 if ext is None else ext)
                    hd1 = fits.getheader(fh,tryext)
                    if hd1['XTENSION'] == 'BINTABLE': fitstable=True
                except:
                    pass
                fh.seek(0)    # reset to beginning of file
                
        # Load a Fits image file
        if inpfmt == 'fits' and fitstable is False:
            try:
                if ext is None: ext=0
                return fits.getdata(fh,ext,header=True)
            except Exception as e:
                print ("There was an error loading the FITS image '%s'" % name)
                print (str(e))
                return
                
        # Step 1) Read the file
        try:
            rdata = writemap[mapcode][0](fh)
        except Exception as e:
            print ("Error reading file")
            print (str(e))
            return
            
        # Step 2) Convert to output format
        try:
            return writemap[mapcode][1](rdata)
        except Exception as e:
            print ("Error converting data")
            print (str(e))
            return
        
    def save(self, data=None, name=None, fmt=None, clobber=False):
        ''' 
        Save the string representation of a data object to a file in VOSpace.

        Parameters
        ----------
        data : str
             The data object such as a pandas data frame or numpy structured array.

        name : str
             The name of the file in VOSpace to create.  The vos:// prefix is not
             necessary.
        
        fmt : str
             The format to use for the output file.  If this is not specified then the
             file extension of ``name`` is used to attempt to figure out the format.
             The currently supported input and output formats are:

             Data type         Format
             csv               csv
             ascii             ascii
             array             csv/fits
             structarray       csv/fits
             pandas            csv/fits/hdf5
             table             csv/fits/hdf5
             votable           csv/fits/hdf5
        
        clobber : bool
             Whether to overwrite an existing file.  The default is False.

        Example
        -------

        Save the pandas data frame called "df" to a file called "data1.csv" in VOSpace.

        .. code-block:: python

            dl.save(df,'data1.csv')

        '''
        # Not enough information input
        if (data is None or name is None):
            print ("Syntax - dl.save(data,name,fmt=fmt,clobber=clobber)")
            return

        # If fmt is None then try to guess format from the output filename extension
        if fmt is None:
            fbase, fext = os.path.splitext(name)
            fmtmap = { '.fits':'fits', '.hdf5':'hdf5', '.csv':'csv', '.xml':'xml' }
            try:
                fmt = fmtmap[fext]
            except:
                print ("Cannot use file extension to determine 'fmt'")
                return
            
        # Input data object types
        # -'csv', type='str', ',' delimited
        # -'ascii', type='str', '\t' delimited
        # -'array' numpy array, type='numpy.ndarray', data.dtype = dtype('float64'), data.shape=(1000,39), len(data.shape)=2, data.dtype.names=None
        # -'structarray' numpy structured array, type='numpy.ndarray', data.dtype = all columns, data.shape=(1000,), len(data.shape)=1
        # -'pandas' data frame, type='pandas.core.frame.DataFrame'
        # -'table' astropy table, type='astropy.table.table.Table'
        # -'votable', astropy VOtable, type='astropy.io.votable.tree.Table'
        inptypemap = {'str':'csv', 'numpy.ndarray':'numpy','pandas.core.frame.DataFrame':'pandas',
                      'astropy.table.table.Table':'table', 'astropy.io.votable.tree.Table':'votable'}
        datatype = str(type(data)).split("'")[1]   # "<type 'numpy.ndarray'>"
        try:
            inptype = inptypemap[datatype]
        except:
            print ("Data object type %s not supported." % datatype)
        # Discern 'csv' vs. 'ascii'
        if (inptype == 'csv'):
            if len(data[0:5000].split('\t')) > len(data[0:5000].split(',')): inptype='ascii'
        # Discern 'array' vs. 'structarray'
        if (inptype == 'numpy'):
            inptype = 'array'     # default
            if len(data.shape) == 1: inptype='structarray'

        # Import the modules if necessary
        # astropy fits
        try:
            dum = fits.__doc__
        except:
            from astropy.io import fits
        # astropy Table
        try:
            dum = Table.__doc__
        except:
            from astropy.table import Table
        # astropy votable
        try:
            dum = writeo.__doc__
        except:
            from astropy.io.votable import from_table, writeto
            
        # Check token if writing to VOSpace
        if name.startswith("vos://"):
            # Check if we are logged in
            if not checkLogin(self):
                return
            token = getUserToken(self)
            # Check that we have a good token
            if not authClient.isValidToken(token):
                raise Exception ("Invalid user name and/or password provided. Please try again.")

        # Check if the file exists already
        if name.startswith("vos://"):
            res = storeClient.ls(token,name,'csv')
            if res != '':
                if clobber is False:
                    print ("'%s' already exists." % name)
                    return
                else:
                    storeClient.rm(token,name)   # clobber it
        else:
            if os.path.exists(name) is True:
                if clobber is False:
                    print ("'%s' already exists." % name)
                    return
                else:
                    os.remove(name)    # clobber it
            
        # What local file are we writing to
        # 1) local file, use name
        # 2) vos:// file, use temporary filename
        outname = name
        if name.startswith("vos://"):
            tfd = tempfile.NamedTemporaryFile()
            outname = tfd.name
            tfd.close()
            
        # Output formats supported
        # -'csv', (1) csv
        # -'ascii', (1) csv
        # -'array', (1) csv, (2) FITS array
        # -'structarray', (1) csv, (2) FITS binary table
        # -'pandas', (1) csv, (2) FITS binary table, (3) HDF5
        # -'table', (1) csv, (2) FITS binary table, (3) HDF5
        # -'votable', (1) csv (2) FITS binary table, (3) HDF5,  XML not working right now
            
        # These are functions to output the various types of files
        # x=data, y=filename
        writemap = { 'csv-csv':          lambda x,y: writeAscii(y, x),
                     'ascii-csv':        lambda x,y: writeAscii(y, x),    # use tab-delimited
                     'ascii-ascii':      lambda x,y: writeAscii(y, x),
                     'array-csv':        lambda x,y: np.savetxt(y,x,delimiter=','),
                     'array-fits':       lambda x,y: fits.writeto(y,x,overwrite=True),
                     'structarray-csv':  lambda x,y: np.savetxt(y,x,delimiter=',',header=','.join(x.dtype.names)),
                     'structarray-fits': lambda x,y: fits.writeto(y,x,overwrite=True),
                     'pandas-csv':       lambda x,y: x.to_csv(y,sep=',',header=True),
                     'pandas-fits':      lambda x,y: Table.from_pandas(x).write(y,format='fits'),
                     'pandas-hdf5':      lambda x,y: Table.from_pandas(x).write(y,format='hdf5',path='table'),
                     'table-csv':        lambda x,y: x.write(y,format='ascii.csv'),
                     'table-fits':       lambda x,y: x.write(y,format='fits'),
                     'table-hdf5':       lambda x,y: x.write(y,format='hdf5',path='table'),
                     'votable-csv':      lambda x,y: x.to_table().write(y,format='ascii.csv'),
                     'votable-fits':     lambda x,y: x.to_table().write(y,format='fits'),
                     'votable-hdf5':     lambda x,y: x.to_table().write(y,format='hdf5',path='table') }

        # Check that we can deal with the requested input and output formats
        if inptype+'-'+fmt not in writemap.keys():
            print ("Output format '%s' for data type '%s' not currently supported." % (fmt, inptype))
            return
            
        # Write the file
        try:
            writemap[inptype+'-'+fmt](data,outname)
        except Exception as e:
            print ("There was a problem writing the file")
            print (str(e))

        # Put to VOSpace if necessary
        if name.startswith('vos://'):
            storeClient.put (token, outname, name, verbose=False)
            os.remove(outname)   # remove temporary file
 

################################################
#  SIA Tasks
################################################


    def siaquery(self, ra=None, dec=None, dist=None, verbose=False):
        '''
        Perform a SIA query with a set of coordinates or from an uploaded file.

        Parameters
        ----------
        ra : float
             The right ascension (in degrees) of the point to use for the search.

        dec : float
             The declination (in degrees) of the point to use for the search.

        dist : float
             The search distance (radius) in degrees.  The default is 0.0085 deg.

        verbose : bool
             Use verbose output.  The default is False.

        Returns
        -------
        images : votable
            The list of images in Astropy table format.

        Example
        -------

        Perform a simple SIA search.

        .. code-block:: python

            itab = dl.siaquery(0.5,10.0,0.1)
            The image list contains 6 entries

        Download the first image using :func:`copyurl()`.

        .. code-block:: python

             dl.copyurl(itab['access_url'][0],'im1.fits')


        '''

        # Not enough information input
        if ((ra is None) or (dec is None)) :
            print ("Syntax - dl.siaquery(ra, dec, dist, file=None, out=None, verbose=False)")
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        
        token = getUserToken(self)
        parts = token.strip().split(".")
        uid = parts[1]
        
        # Use pyvo.dal.sia for now

        svc = sia.SIAService (SIA_DEF_ACCESS_URL)
        if dist is None:
            dist = SIA_DEF_SIZE

        # Run the search query
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', AstropyWarning)   # Turn off some annoying astropy warnings
            images = svc.search((ra,dec), (dist/np.cos(dec*np.pi/180), dist), verbosity=2)
        nrows = images.votable.nrows
        print ("The image list contains",nrows,"entries")
        res = (images.votable.to_table() if nrows > 0 else None)
        # Print the results if verbose set
        if verbose is True and nrows > 0:
            print (res)
    
        return res
