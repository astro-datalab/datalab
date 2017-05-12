#!/usr/bin/env python
#
# DLDO.PY -- Task routines for the 'datalab' notebook interface
#

#from __future__ import print_function

__authors__ = 'David Nidever <dnidever@noao.edu>, Robert Nikutta <rnikutta@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20170509'  # yyyymmdd


"""
    Task routines for the 'datalab' command-line client.

Import via

.. code-block:: python

    from dl import datalab
"""

import os
from subprocess import Popen, PIPE
from time import gmtime, strftime, sleep
try:
    import ConfigParser                         # Python 2
    from urllib import quote_plus               # Python 2
except ImportError:
    import configParse as ConfigParser          # Python 2
    from urllib.parse import quote_plus         # Python 3
    
# std lib imports
import getpass
from functools import partial
from cStringIO import StringIO
from collections import OrderedDict
import numpy as np
from pandas import read_csv
from astropy.table import Table
from astropy.io.votable import parse_single_table

# Data Lab Client interfaces
from dl import authClient, storeClient, queryClient

# VOSpace imports
#import vos as vos
#from vos.fuse import FUSE
#from vos.__version__ import version
#from vos.vofs import VOFS
DAEMON_TIMEOUT = 60                             # Mount timeout
CAPS_DIR = "../caps"                            # Capability directory

ANON_TOKEN = "anonymous.0.0.anon_access"        # default tokens

# Service URLs
AM_URL = "http://dlsvcs.datalab.noao.edu/auth"      # Auth Manager
SM_URL = "http://dlsvcs.datalab.noao.edu/storage"   # Storage Manager
QM_URL = "http://dlsvcs.datalab.noao.edu/query"     # Query Manager


def getUserName (self):
    '''  Get the currently logged-in user token.  If we haven't logged in
         return the anonymous username.
    '''
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
    if self.loginstatus != 'loggedin':
        print "You are not currently logged in.  Please use dl.login() to do so."
        return False
    else:
        return True

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
            self.config.set('datalab', 'created', strftime(
                '%Y-%m-%d %H:%M:%S', gmtime()))
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
        with open('%s/dl.conf' % self.home, 'wb') as configfile:
            self.config.write(configfile)

class Dlinterface:
    '''
       dlinterface super-class
    '''
    def __init__(self, verbose=True):
        dlinteract = DLInteract()
        self.dl = dlinteract
        self.loginstatus = ""
        self.verbose = verbose
        if verbose is True:
            print "Welcome to the Data Lab python interface.  Type dl.help() for help."
        
#### HELP ########

    def help(self, command=None):
        '''
        Print out useful help information.
        '''

        # Print out general help information
        if command is None:
            print "The Data Lab python interface."
            print " "
            print "Use dl.help(<command>) for specific help on one command."
            print " "
            print "The available commands are:"
            print " "
            print "-- Login and authentication --"
            print "dl.login()     - Login to the Data Lab"
            print "dl.logout()    - Logout of the Data Lab"
            print "dl.status()    - Report on the user status"
            print "dl.whoami()    - Print the current active user"
            print " "
            print "-- File system operations --"
            print "dl.ls()        - List a location in Data Lab"
            print "dl.get()       - Get a file from Data Lab"
            print "dl.put()       - Put a file into Data Lab"
            print "dl.cp()()      - Copy a file in Data Lab"
            print "dl.mv()        - Move a file in Data Lab"
            print "dl.rm()        - Delete a file in Data Lab"
            print "dl.mkdir()     - Create a directory in Data Lab"
            print "dl.rmdir()     - Delete a directory in Data Lab"
            print "dl.ln()        - Link a file in Data Lab"
            print "dl.tag()       - Tag a file in Data Lab"
            print " "
            print "-- Query and database operations --"
            print "dl.query()     - Query a remote data service in the Data Lab"
            print "dl.dropdb()    - Drop a user MyDB table"
            print "dl.listdb()    - List the user MyDB tables"
            print "dl.qresults()  - Get the async query results"
            print "dl.qstatus()   - Get an async query job status"
            print "dl.siaquery()  - Query a SIA service in the Data Lab"
            print " "
            print "-- Capabilities --"
            print "dl.listcapability() - List the capabilities supported by this Virtual Storage"
            print "dl.addcapability()  - Activate a capability on a Virtual Storage container"
            print "dl.exec()           - Launch a remote task in the Data Lab"
            print "dl.launch()         - Launch a plugin"
            print "dl.broadcast()      - Broadcast a SAMP message"
            #print "dl.mount()          - mount the default Virtual Storage"

         # Help on a specific command
        else:
            cmd = getattr(dl, command, None)
            if cmd is not None:
                print cmd.__doc__
            else:
                print ("%s is not a supported command." % command)
        
        
################################################
#  Account Login Tasks
################################################

    
    def login(self, user=None):
        '''
        Login to datalab
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
                    print ("Current token for User '%s' no longer valid.  Please re-login." % user)
                    DOLOGIN = True
                else:
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
            if user == '':
                user = raw_input('Enter user: ')
            if user == 'anonymous':
                token = authClient.login('anonymous','')
            else:
                token = authClient.login(user,getpass.getpass(prompt='Enter password: '))
       
                if not authClient.isValidToken(token):
                    print "Invalid user name and/or password provided. Please try again."
                    return
                else:
                    print ("Welcome to the Data Lab, %s" % user)
                    #print "Authentication successful."
                    self.dl.save("login", "status", "loggedin")
                    self.dl.save("login", "user", user)
                    self.dl.save("login", "authtoken", token)
                    self.dl.save(user, "authtoken", token)
                    self.loginstatus = "loggedin"
                    #self.user = user
                    #self.token = token
                    self.loginstatus = "loggedin"

        ## Default parameters if the VOSpace mount is requested.
        #if mount != "":
        #    print ("Initializing virtual storage mount")
        #    self.mount(vospace='vos:', mount=mount, cache_dir=None, cache_limit= 50 * 2 ** (10 + 10 + 10),
        #               cache_nodes=False, max_flux_threads=10, secure_get=False, allow_other=False,
        #               foreground=False, nothreads=True)
        #    #mount = Mountvofs(self.dl)
        #    #mount.setOption('vospace', 'vos:')
        #    #mount.setOption('mount', self.mount.value)
        #    #mount.setOption('cache_dir', None)
        #    #mount.setOption('cache_limit', 50 * 2 ** (10 + 10 + 10))
        #    #mount.setOption('cache_nodes', False)
        #    #mount.setOption('max_flush_threads', 10)
        #    #mount.setOption('secure_get', False)
        #    #mount.setOption('allow_other', False)
        #    #mount.setOption('foreground', False)
        #    #mount.setOption('nothreads', True)
        #    #mount.run()
        return

    def logout(self, unmount=None):
        '''
        Logout out of the Data Lab
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
#            if self.unmount != "":
#                print ("Unmounting remote space")
#                cmd = "umount %s" % self.unmount
#                pipe = Popen(cmd, shell=True, stdout=PIPE)
#                output = pipe.stdout.read()
#                self.mount = ""
            self.dl.save("login", "status", "loggedout")
            self.dl.save("login", "user", "")
            self.dl.save("login", "authtoken", "")
            print ("'%s' is now logged out of the Data Lab" % user)
            self.loginstatus = "loggedout"
            #self.user = ""
            #self.token = ""


    def status(self):
        ''' 
        Status of the Data Lab connection
        '''
        if self.loginstatus == "loggedout":
            print ("No user is currently logged into the Data Lab")
        else:
            print ("User %s is logged into the Data Lab" % \
                    self.dl.get("login", "user"))
        #if self.mount != "":
        #    if status != "loggedout":
        #        print ("The user's Virtual Storage is mounted at %s" % self.mount)
        #    else:
        #        print ("The last user's Virtual Storage is still mounted at %s" % \
        #            self.mount)
            

    def whoami(self):
        '''
        Print the current active user.
        '''
        print (getUserName(self))


################################################
#  Storage Manager Capability Tasks
################################################


    def addcapability(self):
        ''' 
        Add a capability to a VOSpace container
        '''
        # Check if we are logged in
        if not checkLogin(self):
            return
        
        if self.listcap.value:
            print ("The available capabilities are: ")
            for file in glob.glob(self.capsdir):
                print (file[:file.index("_cap.conf")])
        else:
            mountpoint = self.dl.get('vospace', 'mount')
            if mountpoint is None:
                print ("No mounted Virtual Storage can be found")
            else:
                if not os.path.exists("%s/%s_cap.conf" % \
                    (self.capsdir, self.cap.value)):
                        print ("The capability '%s' is not known" % \
                            self.cap.value)
                else:
                    shutil.copy("%s/%s_cap.conf" % (self.capsdir, 
                        self.cap.value), "%s/%s" % (mountpoint, self.dir.value))

    def listcapability(self):
        ''' 
        Add a capability to a VOSpace container
        '''
        # Check if we are logged in
        if not checkLogin(self):
            return

        print ("The available capabilities are: ")
        for file in glob.glob("%s/*_cap.conf" % self.capsdir):
            print ("  %s" % file[file.rindex("/") + 1:file.index("_cap.conf")])
        
################################################
#  Query Manager Tasks
################################################


    def query(self, query=None, type='sql', fmt='csv', out=None, async=False, profile='default'):
        """
        Send a query to a remote query service.

        Parameters
        ----------
        query : str
            The query string that will be passed to the queryClient and then
            to the DB query manager.  This can either be in the SQL or
            ADQL format (specified by the "type" parameter).  For example,

            .. code-block:: python

                'select ra,dec from gaia_dr1.gaia_source limit 3'
        type : str
            The query format, SQL or ADQL.  SQL is used by default.

        fmt : str
            Format of the result to be returned by the query. Permitted values are:
              * 'csv'     the returned result is a comma-separated string that looks like a csv file (newlines at the end of every row)
              * 'string'  same as csv
              * 'array'   Numpy array
              * 'structarray'  Numpy structured / record array
              * 'pandas'  a Pandas data frame
              * 'table'   in Astropy Table format
              * 'votable' result is a string XML-formatted as a VO table

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
            If ``async=False``, the return value is the result of the
            query as a formatted string (see ``fmt``). Otherwise the
            result string is a job token, with which later the
            asynchroneaous query's status can be checked
            (:func:`dl.query.status()`), and the result retrieved (see
            :func:`dl.query.result()`.
          DEPENDS ON "FMT"


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
        # Not enough information input
        if (query is None):
            print "Syntax - dl.query(query, type='sql', fmt='csv', out='', async=False, profile='default')"
            return

        # Check if we are logged in
        if not checkLogin(self):
            return
        
        # Check type
        if (type != 'sql') and (type != 'adql'):
            print "Only 'sql' and 'adql' queries are currently supported."
            return
            
        token = getUserToken (self)
        _query = query         # local working copy
        
        # Check if the query is in a file
        if os.path.exists (_query):
                with open (_query, "r", 0) as fd:
                    _query = fd.read (os.path.getsize(_query)+1)
                fd.close()
                
        # What type of query are we doing
        sql = None
        adql = None
        if type == 'sql':
            sql = _query
        else:
            adql = _query

        # map outfmt container types to a tuple:
        # (:func:`queryClient.query()` fmt-value, descriptive title,
        # processing function for the result string)
        mapping = OrderedDict([
            ('csv'         , ('csv',     'CSV formatted table as a string', lambda x: x.getvalue())),
            ('string'      , ('csv',     'CSV formatted table as a string', lambda x: x.getvalue())),
            ('array'       , ('csv',     'Numpy array',                     partial(np.loadtxt,unpack=False,skiprows=1,delimiter=','))),
            ('structarray' , ('csv',     'Numpy structured / record array', partial(np.genfromtxt,dtype=float,delimiter=',',names=True))),
            ('pandas'      , ('csv',     'Pandas dataframe',                read_csv)),
            ('table'       , ('csv',     'Astropy Table',                   partial(Table.read,format='csv'))),
            ('votable'     , ('votable', 'Astropy VOtable',                 parse_single_table))
        ])

        # The queryClient "fmt" will depend on the requested output format
        try:
            qcfmt = mapping[fmt][0]
        except:
            print ("Format %s not supported." % fmt)
            return
        
        # Execute the query.
        if profile != "default":
            if profile != "" and profile is not None:
                queryClient.set_profile (profile=profile)

        try:
            res = queryClient.query (token, adql=adql, sql=sql, 
                                     fmt=qcfmt, out=out, async=async)

            # Return the results
            
            # Asynchronous
            if async:
                print ("Asynchronous query JobID = %s " % res)                         # Return the JobID
            # Synchronous
            elif out == '' or out is None:
                # Convert to the desired format
                s = StringIO(res)
                output = mapping[fmt][2](s)
                print "Returning %s" % mapping[fmt][1]
                return output
                    
        except Exception as e:
            if not async and e.message is not None:
                err = e.message
                if err.find("Time-out"):
                    print ("Error: Sync query timeout, try an async query")
            else:
                print (e.message)

    def querystatus(self, jobid=None):
        '''
        Get the async query job status.
        '''
        # Not enough information input
        if (jobid is None):
            print "Syntax - dl.querystatus(jobid)"
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        print (queryClient.status (token, jobId=jobid))

    def queryresults(self, jobid=None):
        '''
        Get the async query results.
        '''
        # Not enough information input
        if (jobid is None):
            print "Syntax - dl.queryresults(jobid)"
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        print (queryClient.results (token, jobId=jobid))

    def listmydb(self, table=None):
        '''
        List the user's MyDB tables.
        '''
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        try:
            res = queryClient.list (token, table=table)
        except Exception as e:
            print ("Error listing MyDB tables.")
        else:
            print (res)

            
    def dropmydb(self, table=None):
        '''
        Drop a user's MyDB table.
        '''
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        try:
            queryClient.drop (token, table=table)
        except Exception as e:
            print ("Error dropping table '%s'." % table)

    def queryprofiles(self, profile=None):
        '''
        List the available Query Manager profiles.
        '''
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        print (queryClient.list_profiles (token, profile=profile))


################################################
#  Task Execution Tasks
################################################

# launchjob

################################################
#  FUSE Mounting Tasks
################################################

#    def mount(self, vospace='', mount='', cache_dir=None, readonly=False,
#              cache_limit=(50 * 2 ** (10 + 10 + 10)), cache_nodes=False, max_flux_threads=10,
#              secure_get=False, allow_other=False, foreground=False, nothreads=True):
#        '''
#        Mount a VOSpace via FUSE
#        '''
#        #readonly = False
#        token = self.token
#        user = self.user
#        root = vospace + "/" + user
#        absmount = os.path.abspath(mount)
#        self.mount = absmount
#        if cache_dir is None:
#            cache_dir = os.path.normpath(os.path.join(
#                os.getenv('HOME', '.'), root.replace(":", "_")))
#        if not os.access(absmount, os.F_OK):
#            os.makedirs(absmount)
#        #opt = parseSelf(self)
#        conn = vos.Connection(vospace_token=token)
#        if platform == "darwin":
#            fuse = FUSE(VOFS(root, cache_dir, opt,
#                             conn=conn, cache_limit=cache_limit,
#                             cache_nodes=cache_nodes,
#                             cache_max_flush_threads=max_flush_threads,
#                             secure_get=secure_get),
#                        absmount,
#                        fsname=root,
#                        volname=root,
#                        nothreads=nothreads,
#                        defer_permissions=True,
#                        daemon_timeout=DAEMON_TIMEOUT,
#                        readonly=readonly,
#                        allow_other=allow_other,
#                        noapplexattr=True,
#                        noappledouble=True,
#                        foreground=foreground)
#        else:
#            fuse = FUSE(VOFS(root, cache_dir, opt,
#                             conn=conn, cache_limit=cache_limit,
#                             cache_nodes=cache_nodes,
#                             cache_max_flush_threads=max_flush_threads,
#                             secure_get=secure_get),
#                        absmount,
#                        fsname=root,
#                        nothreads=nothreads,
#                        readonly=readonly,
#                        allow_other=allow_other,
#                        foreground=foreground)
#        if not fuse:
#            self.mount = ''

        
################################################
#  Storage Manager Tasks
################################################
        
        
    def ls(self, name='vos://', format='csv'):
        '''
        The list command method
        '''
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        # Run the LS command
        return storeClient.ls (token, name=name, format=format)


    def get(self, source=None, destination=None, verbose=True):
        '''
        Get one or more files from Data Lab.
        '''
        # Not enough information input
        if (source is None) or (destination is None):
            print "Syntax - dl.get(source, destination)"
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        # Run the GET command
        storeClient.get (token, fr=source, to=destination,
                            verbose=verbose)

    def put(self, source=None, destination=None, verbose=True):
        '''
        Put files into Data Lab.
        '''
        # Not enough information input
        if (source is None) or (destination is None):
            print "Syntax - dl.put(source, destination)"
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        # Run the PUT command
        storeClient.put (token, source, to=destination,
                            verbose=verbose)
        
    def mv(self, source=None, destination=None, verbose=True):
        '''
        The move command method
        '''
        # Not enough information input
        if (source is None) or (destination is None):
            print "Syntax - dl.mv(source, destination)"
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        # Run the MV command
        storeClient.mv (token, fr=source, to=destination,
                        verbose=verbose)

    def cp(self, source=None, destination=None, verbose=True):
        '''
        Copy a file in Data Lab
        '''
        # Not enough information input
        if (source is None) or (destination is None):
            print "Syntax - dl.cp(source, destination)"
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        # Run the CP command
        storeClient.cp (token, fr=source, to=destination,
                        verbose=verbose)

    def rm(self, name=None, verbose=True):
        '''
        Delete files in Data Lab
        '''
        # Not enough information input
        if (name is None):
            print "Syntax - dl.rm(name)"
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        # Run the RM command
        storeClient.rm (token, name=name, verbose=verbose)

        
    def ln(self, source=None, target=None):
        '''
        Link a file in Data Lab
        '''
        # Not enough information input
        if (source is None) or (target is None):
            print "Syntax - dl.ln(source, target)"
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        # Run the LN command
        storeClient.ln (token, fr=source, target=target)

        
    def tag(self, name=None, tag=None):
        '''
        Tag a file in Data Lab
        '''
        # Not enough information input
        if (name is None) or (tag is None):
            print "Syntax - dl.tag(name, tag)"
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        # Run the TAG command
        storeClient.tag (token, name=name, tag=tag)

    def mkdir(self, name=None):
        ''' 
        Create a directory in Data Lab
        '''
        # Not enough information input
        if (name is None):
            print "Syntax - dl.mkdir(name)"
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        # Run the MKDIR command
        storeClient.mkdir (token, name=name)


    def rmdir(self, name=None):
        ''' 
        Delete a directory in Data Lab
        '''
        # Not enough information input
        if (name is None):
            print "Syntax - dl.rmdir(name)"
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        # Run the RMDIR command
        storeClient.rmdir (token, name=name)

    def resolve(self, name=None):
        ''' 
        Resolve a vos short form identifier     -- FIXME
        '''
        # Not enough information input
        if (name is None):
            print "Syntax - dl.resolve(name)"
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        # Check that we have a good token
        if not authClient.isValidToken(token):
            raise Exception, "Invalid user name and/or password provided. Please try again."
        # Run the command
        r = requests.get(SM_URL + "resolve?name=%s" %
                         name, headers={'X-DL-AuthToken': token})



################################################
#  SAMP Tasks
################################################

    def broadcast(self, type=None, pars=None):
        ''' 
        Broadcast a SAMP message
        '''
        # Not enough information input
        if (type is None) or (pars is None):
            print "Syntax - dl.broadcast(type, pars)"
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        client = self.getSampClient()
        mtype = type
        params = {}
        for param in pars.split(","):
            key, value = param.split("=")
            params[key] = value
        self.broadcast(client, mtype, params)

    def launch(self, dir=None):
        '''
        Launch a plugin in Data Lab
        '''
        # Not enough information input
        if (dir is None):
            print "Syntax - dl.launch(dir)"
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        token = getUserToken(self)
        r = requests.get(SM_URL + "/rmdir?dir=%s" %
                         dir, headers={'X-DL-AuthToken': token})



#    def receiver():
#        '''
#        SAMP listener
#        '''
#        def __init__(self, client):
#            self.client = client
#            self.received = False
#
#        def receive_notifications(self, private_key, sender_id, mtype, 
#                                params, extra):
#            self.params = params
#            self.received = True
#            print ('Notification:', private_key, sender_id, mtype, params, extra)
#
#        def receiver_call(self, private_key, sender_id, msg_id, mtype, 
#                                params, extra):
#            self.params = params
#            self.received = True
#            print ('Call:', private_key, sender_id, msg_id, mtype, params, extra)
#            self.client.reply(
#                msg_id, {'samp.status': 'samp.ok', 'samp.result': {}})
#
#        def point_select(self, private_key, send_id, mtype, params, extra):
#            self.params = params
#            self.received = True

################################################
#  SIA Tasks
################################################


# why not use queryClient SIAQUERY????

    def siaquery(self, input=None, search=None, out=None):
        '''
        SIA query with an uploaded file
        '''

        #def getUID(self):
        #    ''' Get the UID for this user
        #    '''
        #    token = getUserToken(self)
        #    parts = token.strip().split(".")
        #    uid = parts[1]
        #    return uid

        # Not enough information input
        if (input is None) or (search is None):
            print "Syntax - dl.siaquery(input, search)"
            return
        # Check if we are logged in
        if not checkLogin(self):
            return
        
        token = getUserToken(self)
        parts = token.strip().split(".")
        uid = parts[1]
        
        # If local input file, upload it
        _input = input
        shortname = '%s_%s' % (uid, input[input.rfind('/') + 1:])
        if input[:input.find(':')] not in ['vos', 'mydb']:
            #      target = 'vos://nvo.caltech!vospace/siawork/%s' % shortname
            # Need to set this from config
            target = 'vos://datalab.noao.edu!vospace/siawork/%s' % shortname
            r = requests.get(SM_URL + "/put?name=%s" %
                             target, headers={'X-DL-AuthToken': token})
            file = open(input).read()
            resp = requests.put(r.content, data=file, headers={
                                'Content-type': 'application/octet-stream',
                                'X-DL-AuthToken': token})

        # Query the Data Lab query service
        headers = {'Content-Type': 'text/ascii', 
                   'X-DL-AuthToken': token}
        dburl = '%s/sia?in=%s&radius=%s&out=%s' % (
            QM_URL, shortname, search, out)
        r = requests.get(dburl, headers=headers)

        # Output value
        if out != '':
            if out[:out.index(':')] not in ['vos']:
                file = open(out, 'wb')
                file.write(r.content)
                file.close()
        else:
            print (r.content)
