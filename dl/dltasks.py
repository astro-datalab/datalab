#!/usr/bin/env python
#
# DATALAB.PY -- Task routines for the 'datalab' command-line client.
#

from __future__ import print_function

__authors__ = 'Matthew Graham <graham@noao.edu>, Mike Fitzpatrick <fitz@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20170520'  # yyyymmdd


"""
    Task routines for the 'datalab' command-line client.

Import via

.. code-block:: python

    from dl import datalab
"""


import sys
from sys import platform
from astropy.vo.samp import SAMPIntegratedClient
import glob
import os
import logging
import shutil
from subprocess import Popen, PIPE
from time import gmtime, strftime, sleep
import httplib2
from optparse import Values
try:
    import ConfigParser                         # Python 2
    from urllib import quote_plus               # Python 2
except ImportError:
    import configparser as ConfigParser          # Python 2
    from urllib.parse import quote_plus         # Python 3

try:
    from http.client import HTTPConnection # py3
except ImportError:
    from httplib import HTTPConnection # py2

import requests             # need to standarize on one library at some point

# VOSpace imports
#import vos as vos
#from vos.fuse import FUSE
#from vos.__version__ import version
version = "2.2.0"                  # VOS version
#from vos.vofs import VOFS
DAEMON_TIMEOUT = 60                             # Mount timeout
CAPS_DIR = "../caps"                            # Capability directory


ANON_TOKEN = "anonymous.0.0.anon_access"        # default tokens
DEMO_TOKEN = "dldemo.99999.99999.demo_access"
TEST_TOKEN = "dltest.99998.99998.test_access"


# Data Lab Client interfaces
from dl import authClient, storeClient, queryClient
#import authClient, storeClient, queryClient


# Uncomment to print HTTP and response headers
httplib2.debuglevel = 0
HTTPConnection.debuglevel = 0

TEST = False

if TEST:
    AM_URL = "http://dldev.datalab.noao.edu/auth"       # Auth Manager
    SM_URL = "http://dldev.datalab.noao.edu/storage"    # Storage Manager
    QM_URL = "http://dldev.datalab.noao.edu/query"      # Query Manager
else:
    AM_URL = "http://dlsvcs.datalab.noao.edu/auth"      # Auth Manager
    SM_URL = "http://dlsvcs.datalab.noao.edu/storage"   # Storage Manager
    QM_URL = "http://dlsvcs.datalab.noao.edu/query"     # Query Manager



def parseSelf(obj):
    opt = Values()
    for attr in dir(obj):
        if isinstance(getattr(obj, attr), Option):
            opt.ensure_value(attr, getattr(obj, attr).value)
    return opt


def getUserName (self):
    '''  Get the currently logged-in user token.  If we haven't logged in
         return the anonymous token.
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



class Option:
    '''
         Represents an option
    '''
    def __init__(self, name, value, description, display=None, default=None,
                    required=False):
        self.name = name
        self.value = value
        self.display = display
        self.description = description
        self.default = default
        self.required = required


class DataLab:
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
            self.config.set('login', 'authtoken', '')
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


class Task:
    ''' 
        Superclass to represent a Task
    '''
    def __init__(self, datalab, name, description):
        self.dl = datalab
        self.name = name
        self.description = description
        self.logger = None
        self.params = []

        self.addOption("verbose", 
            Option( "verbose", "", "print verbose level log messages",
                    default=False))
        self.addOption("debug", 
            Option("debug", "", "print debug log level messages",
                    default=False))
        self.addOption("warning", 
            Option( "warning", "", "print warning level log messages",
                    default=False))

    def run(self):
        pass


    def addOption(self, name, option):
        ''' Add an option to the Task.
        '''
        self.params.append(name)
        setattr(self, name, option)

        # Set the default value if provided.
        if option.default is not None:
            self.setOption(name, option.default)


    def addLogger(self, logLevel, logFile):
        ''' Add a Logger to the Task.
        '''
        logFormat = ("%(asctime)s %(thread)d vos-" + str(version) +
                     "%(module)s.%(funcName)s.%(lineno)d %(message)s")
        logging.basicConfig(level=logLevel, format=logFormat,
                            filename=os.path.abspath(logFile))

        self.logger = logging.getLogger()
        self.logger.setLevel(logLevel)
        self.logger.addHandler(logging.StreamHandler())


    def setLogger(self, logLevel=None):
        ''' Set the logger to be used.
        '''
        if logLevel is None:
            logLevel = logging.ERROR
            if self.verbose:
                logLevel = logging.INFO
            if self.debug:
                logLevel = logging.DEBUG
            if self.warning:
                logLevel = logging.WARNING
        else:
            logLevel = logLevel
        self.addLogger(logLevel, "%s/.datalab/datalab.err" % os.path.expanduser('~'))

    def setOption(self, name, value):
        ''' Set a Task option.
        '''
        if hasattr(self, name):
            opt = getattr(self, name)
            opt.value = value
        else:
            print ("Task '%s' has no option '%s'" % (self.name, name))


    def getSampConnect(self):
        ''' Get a SAMP listening client connection.
        '''
        # Get a SAMP client
        client = SAMPIntegratedClient()
        client.connect()
        r = Receiver(client)
        return client


    def broadcast(self, client, messageType, params, app=None):
        ''' Broadcast a SAMP message.
        '''
        # Broadcast to SAMP clients
        message = {}
        message["samp.mtype"] = messageType
        message["samp.params"] = params

        try:
            if app is None:
                client.notify_all(message)
            else:
                for c in client.get_registered_clients():
                    metadata = client.get_metadata(c)
                    if metadata["samp.name"] in app:
                        client.notify(c, message)
        finally:
            client.disconnect()


    def listen(self, client):  # notification type, params, run?
        ''' Setup a listener for a specific SAMP message.
        '''
        client.bind_receive_notification("coord.pointAt.sky", r.point_select)
        try:
            while True:
                sleep(0.1)
                if r.received:
                    # Parse message
                    ra = r.params['ra']
                    dec = r.params['dec']
                    self.run()
                    r.received = False
        finally:
            client.disconnect()


    def report(self, resp):
        ''' Handle call response
        '''
        #if resp.status_code != 200:
        #    print (resp.text)
        #elif self.verbose.value or self.debug.value:
        #    print (resp.text)
        pass


################################################
#  Account Login Tasks
################################################

class Login(Task):
    ''' 
        Log into the Data Lab
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'login', 'Login to the Data Lab')
        self.addOption("user", 
                Option("user", "", "Username of account in Data Lab",
                        required=True))
        self.addOption("password", 
                Option("password", "", "Password for account in Data Lab",
                        required=True))
        self.addOption("mount", 
                Option("mount", "", "Mountpoint of remote Virtual Storage"))
        if self.dl is not None:
            self.status = self.dl.get("login", "status")

        self.login_error = None


    def run(self):
        ''' Execute the Login Task.
        '''
        # Check if we are already logged in.  The 'user' field of the 
        # configuration contains the currently active user and token,
        # however previous logins will have preserved tokens from other
        # accounts we may be able to use.
        if self.status == "loggedin":
            _user = self.dl.get("login", "user")
            if self.user.value == _user:
                # See whether current token is still valid for this user.
                _token = self.dl.get("login", "authtoken")
                if not authClient.isValidToken (_token):
                    if self.do_login() != "OK":
                        print (self.login_error)
                        sys.exit (-1)
                else:
                    print ("User '%s' is already logged in to the Data Lab" % \
                            self.user.value)
            else:
                # We're logging in as a different user.
                if self.do_login() != "OK":
                    print (self.login_error)
                    sys.exit (-1)
                else:
                    # Log a user into the Data Lab
                    print ("Welcome back to the Data Lab, %s" % self.user.value)
        else:
            # If we're not logged in, do so using the name/password provided.
            if self.do_login() != "OK":
                print (self.login_error)
                sys.exit (-1)
            else:
                # Log a user into the Data Lab
                print ("Welcome to the Data Lab, %s" % self.user.value)

        # Default parameters if the VOSpace mount is requested.
        if self.mount.value != "":
            print ("Initializing virtual storage mount")
            mount = Mountvofs(self.dl)
            mount.setOption('vospace', 'vos:')
            mount.setOption('mount', self.mount.value)
            mount.setOption('cache_dir', None)
            mount.setOption('cache_limit', 50 * 2 ** (10 + 10 + 10))
            mount.setOption('cache_nodes', False)
            mount.setOption('max_flush_threads', 10)
            mount.setOption('secure_get', False)
            mount.setOption('allow_other', False)
            mount.setOption('foreground', False)
            mount.setOption('nothreads', True)
            mount.run()


    def do_login(self):
        if self.login():
            self.dl.save("login", "status", "loggedin")
            self.dl.save("login", "user", self.user.value)
            self.dl.save("login", "authtoken", self.token)
            self.dl.save(self.user.value, "authtoken", self.token)
            return "OK"
        else:
            return self.login_error


    def login(self):
        ''' Log-in to the Data Lab.
        '''
        try:
            self.token = self.dl.get(self.user.value,'authtoken')
            if authClient.isValidToken (self.token):
                # FIXME --  What we really want here is a login-by-token call
                # to the AuthMgr so the login is recorded on the server.
                self.login_error = None
                return True
        except Exception as e:
            pass

        # Get the security token for the user
        self.token = authClient.login (self.user.value,self.password.value)
        if not authClient.isValidToken (self.token):
            self.dl.save("login", "status", "loggedout")
            self.dl.save("login", "user", '')
            self.dl.save("login", "authtoken", '')
            self.dl.save(self.user.value, "authtoken", self.token)
            self.login_error = self.token
            return False
        else:
            self.login_error = None
            return True


class Logout(Task):
    '''
        Logout out of the Data Lab
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'logout', 'Logout of the Data Lab')
        self.addOption("unmount", Option(
            "unmount", "", "Mount point of remote Virtual Storage"))
        if self.dl is not None:
            self.status = self.dl.get("login", "status")

    def run(self):
        if self.status == 'loggedout':
            print ("No user is currently logged into the Data Lab")
            return
        else:
            token = getUserToken(self)
            user, uid, gid, hash = token.strip().split('.', 3)
            res = authClient.logout (token)
            if res != "OK":
                print ("Error: %s" % res)
                sys.exit (-1)
            if self.unmount.value != "":
                print ("Unmounting remote space")
                cmd = "umount %s" % self.unmount.value
                pipe = Popen(cmd, shell=True, stdout=PIPE)
                output = pipe.stdout.read()
                self.dl.save("vospace", "mount", "")
            self.dl.save("login", "status", "loggedout")
            self.dl.save("login", "user", "")
            self.dl.save("login", "authtoken", "")

            print ("'%s' is now logged out of the Data Lab" % user)


class Status(Task):
    ''' 
        Status of the Data Lab connection
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, "status", "Report on the user status")

    def run(self):
        status = self.dl.get("login", "status")
        if status == "loggedout":
            print ("No user is currently logged into the Data Lab")
        else:
            print ("User %s is logged into the Data Lab" % \
                    self.dl.get("login", "user"))
        if self.dl.get("vospace", "mount") != "":
            if status != "loggedout":
                print ("The user's Virtual Storage is mounted at %s" % \
                    self.dl.get("vospace", "mount"))
            else:
                print ("The last user's Virtual Storage is still mounted at %s" % \
                    self.dl.get("vospace", "mount"))


class WhoAmI(Task):
    '''
        Print the current active user.
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'whoami', 'Print the current active user')

    def run(self):
        print (getUserName(self))



################################################
#  Schema Discovery Tasks
################################################

class Schema(Task):
    '''
        Print information about data servicce schema
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'schema', 'Print data service schema info')
        self.addOption("val", 
            Option("val", "", "Value to list ([[<schema>][.<table>][.<col>]])",
                required=False, default=None))
        self.addOption("format", 
            Option("format", "", "Output format (csv|text|json)",
                required=False, default=None))
        self.addOption("profile", 
            Option("profile", "", "Service profile", required=False,
                default="default"))

    def run(self):
        print (queryClient.schema (value=self.val.value,
                                  format=self.format.value,
                                  profile=self.profile.value))



################################################
#  Storage Manager Capability Tasks
################################################

class AddCapability(Task):
    ''' 
        Add a capability to a VOSpace container
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'addcapability',
                      'Activate a capability on a Virtual Storage container')
        self.addOption("fmt", 
            Option("fmt", "", "List of formats to accept", required=True))
        self.addOption("dir", 
            Option("dir", "", "Container name", required=True))
        self.addOption("cap", 
            Option("cap", "", "Capability name", required=True))
        self.addOption("listcap", 
            Option("listcap", "", "List available capabilities",
                    default=False))
        self.capsdir = CAPS_DIR

    def run(self):
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


class ListCapability(Task):
    ''' 
        Add a capability to a VOSpace container
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'listcapability',
                      'List the capabilities supported by this Virtual Storage')
        self.capsdir = CAPS_DIR

    def run(self):
        print ("The available capabilities are: ")
        for file in glob.glob("%s/*_cap.conf" % self.capsdir):
            print ("  %s" % file[file.rindex("/") + 1:file.index("_cap.conf")])



################################################
#  Query Manager Tasks
################################################

class Query2 (Task):
    '''
        Send a query to a remote query service.  [Note: placeholder name
        until we figure out what to do with the old Query() functionality.]
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'query',
                      'Query a remote data service in the Data Lab')
        self.addOption("adql", 
            Option("adql", "", "ADQL statement", default='', 
                required=False))
        self.addOption("sql", 
            Option("sql", "", "Input SQL string or filename", default='',
                required=False))
        self.addOption("fmt", 
            Option("fmt", "csv", "Output format (ascii|csv|tsv|fits|votable)",
                required=False))
        self.addOption("out", 
            Option("out", "", "Output filename or destination",
                required=False))
        self.addOption("async", 
            Option("async", "", "Asynchronous query?", default=False,
                required=False))
        self.addOption("profile", 
            Option("profile", "", "Service profile to use",
                required=False, default="default"))
        self.addOption("timeout", 
            Option("timeout", "", "Requested query timeout",
                required=False, default="120"))

    def run(self):
        token = getUserToken (self)

        # Get the query string to be used.  This can either be supplied
        # directly or by specifying a filename.
        sql = None
        adql = None
        res = None

        if self.adql.value is None or self.adql.value == '':
            if self.sql.value is None or self.sql.value == '':
                print ("Error: At least one of 'adql' or 'sql' is required.")
                sys.exit (-1)
            elif os.path.exists (self.sql.value):
                with open (self.sql.value, "r", 0) as fd:
                    sql = fd.read (os.path.getsize(self.sql.value)+1)
                fd.close()
            else:
                sql = self.sql.value
        elif os.path.exists (self.adql.value):
            with open (self.adql.value, "r", 0) as fd:
                adql = fd.read (os.path.getsize(self.adql.value)+1)
                fd.close()
        else:
            adql = self.adql.value
            
        # Execute the query.
        if self.profile.value != "default":
            if self.profile.value != "" and self.profile.value is not None:
                queryClient.set_profile (profile=self.profile.value)

        try:
            res = queryClient.query (token, adql=adql, sql=sql, 
                        fmt=self.fmt.value, out=self.out.value, 
                        async=self.async.value, timeout=self.timeout.value)

            if self.async.value:
                print (res)                         # Return the JobID
            elif self.out.value== '' or self.out.value is None:
                print (res)                         # Return the results
        except Exception as e:
            if not self.async.value and str(e) is not None:
                err = str(e)
                if err.find("Time-out") > 0:
                    print ("Error: Sync query timeout, try an async query")
                else:
                    print (str(e))
            else:
                print (str(e))


class QueryStatus(Task):
    '''
        Get the async query job status.
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'qstatus', 'Get an async query job status')
        self.addOption("jobId", Option("jobId", "",
                        "Query Job ID", required=True))

    def run(self):
        token = getUserToken(self)
        print (queryClient.status (token, jobId=self.jobId.value))


class QueryResults(Task):
    '''
        Get the async query results.
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'qresults', 'Get the async query results')
        self.addOption("jobId", Option("jobId", "",
                        "Query Job ID", required=True))

    def run(self):
        token = getUserToken(self)
        print (queryClient.results (token, jobId=self.jobId.value))


class QueryStatus(Task):
    '''
        Get the async query job status.
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'qstatus', 'Get an async query job status')
        self.addOption("jobId", Option("jobId", "",
                        "Query Job ID", required=True))

    def run(self):
        token = getUserToken(self)
        print (queryClient.status (token, jobId=self.jobId.value))


class ListMyDB(Task):
    '''
        List the user's MyDB tables.
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'listdb', 'List the user MyDB tables')
        self.addOption("table", Option("table", "",
                        "Table name", required=False))

    def run(self):
        token = getUserToken(self)
        try:
            res = queryClient.list (token, table=self.table.value)
        except Exception as e:
            print ("Error listing MyDB tables.")
        else:
            print (res)


class DropMyDB(Task):
    '''
        Drop a user's MyDB table.
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'dropdb', 'Drop a user MyDB table')
        self.addOption("table", 
            Option("table", "", "Table name", required=False))

    def run(self):
        token = getUserToken(self)
        try:
            queryClient.drop (token, table=self.table.value)
        except Exception as e:
            print ("Error dropping table '%s'." % self.table.value)


class QueryProfiles(Task):
    '''
        List the available Query Manager profiles.
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'list_query_profiles', 
            'List the available Query Manager profiles')
        self.addOption("profile", 
            Option("profile", "", "Profile to list", required=False,
                default=None))
        self.addOption("format", 
            Option("format", "", "Output format (csv|text)",
                required=False, default='text'))

    def run(self):
        token = getUserToken(self)
        print (str(queryClient.list_profiles (token, 
                profile=self.profile.value, format=self.format.value)))



class Query(Task):
    '''
        Send a query to a remote query service (OLD VERSION - NOT USED))
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'query',
                      'Query a remote data service in the Data Lab')
        self.addOption("adql", 
            Option("adql", "", "ADQL statement", required=False))
        self.addOption("sql", 
            Option("sql", "", "Input SQL string or filename", required=False))
        self.addOption("uri", 
            Option("uri", "", "Remote dataset URI", required=False,
                default="dldb"))
        self.addOption("fmt", 
            Option("fmt", "", "Output format (ascii|csv|tsv|fits|votable)",
                required=False))
        self.addOption("out", 
            Option("out", "", "Output filename or destination",
                required=False))
        self.addOption("in", 
            Option("in", "", "Input filename", required=False))
        self.addOption("async", 
            Option("async", "", "Asynchronous query?", required=False,
                    default="false"))
        self.addOption("addArgs", 
            Option("addArgs", "", 
                "Additional arguments to pass to the query service", 
                required=False))

    def run(self):
        token = getUserToken(self)
        h = httplib2.Http()

        colon = self.uri.value.find(":")
        scheme = ('' if colon < 0 else self.uri.value[:colon])

        if scheme == '':
            self.dbquery(h, QM_URL, token)
        elif scheme == 'mydb':
            self.dbquery(h, url, token)
        elif scheme == 'http':
            self.httpquery(h, self.uri.value, token)
        elif scheme == 'ivo':
            self.ivoquery(h, self.uri.value, token, self.out.value)
        else:
            print ("'uri' parameter does not begin with a recognized scheme")

    def dbquery(self, h, url, token):
        # Query the Data Lab query service
        headers = {'Content-Type': 'text/ascii',
                   'X-DL-AuthToken': token}  # application/x-sql
        if 'mydb' in self.adql.value:  # Demo hack
            out = self.out.value.replace("vos://", "/tmp/vospace/")
            shutil.copyfile("demo/ltg.csv", out)
        elif self.sql.value != '':
            sql = open(self.sql.value).read()
            dburl = '%s/query?ofmt=%s&out=%s' % \
                        (url, self.ofmt.value, self.out.value)
            resp, content = h.request(dburl, 'POST', body=sql, headers=headers)
        else:
            query = quote_plus(self.adql.value)
            dburl = '%s/query?adql=%s&ofmt=%s&out=%s' % (
                url, query, self.ofmt.value, self.out.value)
            resp, content = h.request(dburl, 'GET', headers=headers)
        output = self.out.value
        if output != '':
            if output[:output.index(':')] not in ['vos', 'mydb']:
                file = open(output, 'wb')
                file.write(content)
                file.close()
        else:
            print (content)

    def httpquery(self, h, url, token):
        # Send a query to remote URL
        # Hack for CoDR demo
        count = 0
        for line in open(self.input.value):
            if '#' not in line:
                parts = line.split(",")
                httpurl = url + \
                    "?POS=%s,%s&SIZE=0.0000555" % (parts[0], parts[2].strip())
                print (url)
                resp, content = h.request(httpurl, 'GET')
                if "vos://" in self.out.value:
                    out = self.out.value.replace("vos://", "/tmp/vospace/")
                    file = open(out + "_%s" % count, 'wb')
                    file.write(content)
                    file.close()
            count += 1

    def ivoquery(self, h, uri, token, out):
        # Send a query to remote IVOA service
        headers = {'X-DL-AuthToken': token}
        url = "%s/query?uri=%s&out=%s" % (QM_URL, uri, out)
        resp, content = h.request(url, 'GET', headers=headers)
        if out == '':
            print (content)


################################################
#  Task Execution Tasks
################################################

class LaunchJob(Task):
    '''
        Execute a remote processing job in the Data Lab
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'exec',
                      'launch a remote task in the Data Lab')
        self.addOption("cmd", 
            Option("cmd", "", "name of remote task to run", required=True))
        self.addOption("args", 
            Option("args", "", 
                "list of key-value arguments to submit to remote task",
                 required=False))

    def run(self):
        job = self.cmd.value
        if job in self.validJob(job):
            params = {}
            for param in self.params.value.split(','):
                key, value = param.split('=')
                params[key] = value
            job = self.getJob(job)
            job.run(params)
        else:
            print ("The remote task '%s' is not supported" % self.cmd.value)

    def validJob(self, job):
        return False

    def getJob(self, job):
        return None


################################################
#  FUSE Mounting Tasks
################################################

class Mountvofs(Task):
    ''' Mount a VOSpace via FUSE '''

    def __init__(self, datalab):
        Task.__init__(self, datalab, 'mount', 'mount the default Virtual Storage')
        self.addOption("vospace", 
            Option("vospace", "", "Space to mount", required=True,
                default="vos:"))
        self.addOption("mount", 
            Option("mount", "", "Mount point for Virtual Storage",
                required=True, default="/tmp/vospace"))
        self.addOption("foreground", 
            Option("foreground", "", 
                "Mount the filesystem as a foreground operation",
                default=False))
        self.addOption("cache_limit", 
            Option("cache_limit", "", 
                "Limit on local diskspace to use for file caching (in MB)", 
                default=50 * 2 ** (10 + 10 + 10)))
        self.addOption("cache_dir", 
            Option("cache_dir", "", "Local directory to use for file caching",
                default=None))
        self.addOption("readonly", 
            Option("readonly", "", "Mount as read-only", default=False))
        self.addOption("cache_nodes", 
            Option("cache_nodes", "", "Cache dataNode Properties"))
        self.addOption("allow_other", 
            Option("allow_other", "", 
                "Allow all users access to this mountpoint"))
        self.addOption("max_flush_threads", 
            Option("max_flush_threads", "",
                "Limit on number of flush (upload) threads", default=10))
        self.addOption("secure_get", 
            Option("secure_get", "", "use HTTPS instead of HTTP"))
        self.addOption("nothreads", 
            Option("nothreads", "",
                "Only run in a single thread, causes some blocking.",
                default=None))

    def run(self):
        #    readonly = False
        token = getUserToken(self)
        user = getUserName(self)
        root = self.vospace.value + "/" + user
        mount = os.path.abspath(self.mount.value)
        self.dl.save('vospace', 'mount', mount)
        if self.cache_dir.value is None:
            self.cache_dir.value = os.path.normpath(os.path.join(
                os.getenv('HOME', '.'), root.replace(":", "_")))
        if not os.access(mount, os.F_OK):
            os.makedirs(mount)
        opt = parseSelf(self)
        conn = vos.Connection(vospace_token=token)
        if platform == "darwin":
            fuse = FUSE(VOFS(root, self.cache_dir.value, opt,
                             conn=conn, cache_limit=self.cache_limit.value,
                             cache_nodes=self.cache_nodes.value,
                             cache_max_flush_threads=self.max_flush_threads.value,
                             secure_get=self.secure_get.value),
                        mount,
                        fsname=root,
                        volname=root,
                        nothreads=opt.nothreads,
                        defer_permissions=True,
                        daemon_timeout=DAEMON_TIMEOUT,
                        readonly=self.readonly.value,
                        allow_other=self.allow_other.value,
                        noapplexattr=True,
                        noappledouble=True,
                        foreground=self.foreground.value)
        else:
            fuse = FUSE(VOFS(root, self.cache_dir.value, opt,
                             conn=conn, cache_limit=self.cache_limit.value,
                             cache_nodes=self.cache_nodes.value,
                             cache_max_flush_threads=self.max_flush_threads.value,
                             secure_get=self.secure_get.value),
                        mount,
                        fsname=root,
                        nothreads=opt.nothreads,
                        readonly=self.readonly.value,
                        allow_other=self.allow_other.value,
                        foreground=self.foreground.value)
        if not fuse:
            self.dl.save('vospace', 'mount', '')


################################################
#  Storage Manager Tasks
################################################

class List(Task):
    '''
        List files in Data Lab
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'list', 'list a location in Data Lab')
        self.addOption("name", 
            Option("name", "", "Location in Data Lab to list",
                required=False, default="vos://", display="name"))
        self.addOption("format", 
            Option("format", "", "Format for listing (ascii|csv|raw)",
                required=False, default="csv"))

    def run(self):
        token = getUserToken(self)
        return storeClient.ls (token, name=self.name.value,
            format=self.format.value)


class Get(Task):
    '''
        Get one or more files from Data Lab.
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'get', 'get a file from Data Lab')
        self.addOption("fr", 
            Option("fr", "", "Remote Data Lab file name", required=True,
                default="vos://"))
        self.addOption("to", 
            Option("to", "", "Local disk file name", required=False,
                default="./"))
        self.addOption("verbose", 
            Option("verbose", "", "Verbose output?", required=False,
                default=True))

    def run(self):
        token = getUserToken(self)
        storeClient.get (token, fr=self.fr.value, to=self.to.value,
                            verbose=self.verbose.value)


class Put(Task):
    '''
        Put files into Data Lab.
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'put', 'Put a file into Data Lab')
        self.addOption("fr", 
            Option("fr", "", "Local disk file name", required=True,
                default=""))
        self.addOption("to", 
            Option("to", "", "Remote Data Lab file name", required=True,
                default=""))
        self.addOption("verbose", 
            Option("verbose", "", "Verbose output?", required=False,
                default=True))

    def run(self):
        token = getUserToken(self)
        storeClient.put (token, fr=self.fr.value, to=self.to.value,
                            verbose=self.verbose.value)


class Move(Task):
    '''
        Move files in Data Lab
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'move', 'move a file in Data Lab')
        self.addOption("fr", 
            Option("fr", "", "Source location in Data Lab",
                required=True, default="", display="from"))
        self.addOption("to", 
            Option("to", "", "Destination location in Data Lab",
                required=True, default=""))
        self.addOption("verbose", 
            Option("verbose", "", "Verbose output?", required=False,
                default=True))

    def run(self):
        token = getUserToken(self)
        storeClient.mv (token, fr=self.fr.value, to=self.to.value,
                        verbose=self.verbose.value)


class Copy(Task):
    '''
        Copy a file in Data Lab
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'copy', 'copy a file in Data Lab')
        self.addOption("fr", 
            Option("fr", "", "Source location in Data Lab",
                required=True, default="", display="from"))
        self.addOption("to", 
            Option("to", "", "Destination location in Data Lab", 
                required=True, default=""))
        self.addOption("verbose", 
            Option("verbose", "", "Verbose output?", required=False,
                default=True))

    def run(self):
        token = getUserToken(self)
        storeClient.cp (token, fr=self.fr.value, to=self.to.value,
                        verbose=self.verbose.value)


class Delete(Task):
    '''
        Delete files in Data Lab
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'delete', 'delete a file in Data Lab')
        self.addOption("name", 
            Option("name", "", "File in Data Lab to delete", required=True,
                default=""))
        self.addOption("verbose", 
            Option("verbose", "", "Verbose output?", required=False,
                default=True))

    def run(self):
        token = getUserToken(self)
        storeClient.rm (token, name=self.name.value, verbose=self.verbose.value)


class Link(Task):
    '''
        Link a file in Data Lab
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'link', 'link a file in Data Lab')
        self.addOption("fr", 
            Option("fr", "", "Location in Data Lab to link from",
                required=True, default="vos://"))
        self.addOption("to", 
            Option("to", "", "Location in Data Lab to link to",
                required=True, default="vos://"))

    def run(self):
        token = getUserToken(self)
        storeClient.ln (token, fr=self.fr.value, target=self.to.value)


class Tag(Task):
    '''
        Tag a file in Data Lab
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'tag', 'tag a file in Data Lab')
        self.addOption("name", 
            Option("name", "", "File in Data Lab to tag", required=True,
                default="vos://"))
        self.addOption("tag", 
            Option("tag", "", "Tag to add to file", required=True, 
                default=""))

    def run(self):
        token = getUserToken(self)
        storeClient.tag (token, name=self.name.value, tag=self.tag.value)


class MkDir(Task):
    ''' 
        Create a directory in Data Lab
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'mkdir', 'create a directory in Data Lab')
        self.addOption("name", 
            Option("name", "", "Directory in Data Lab to create",
                required=True, default=""))

    def run(self):
        token = getUserToken(self)
        storeClient.mkdir (token, name=self.name.value)


class RmDir(Task):
    ''' 
        Delete a directory in Data Lab
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'rmdir', 'delete a directory in Data Lab')
        self.addOption("name", 
            Option("name", "", "Directory in Data Lab to delete",
                required=True, default=""))

    def run(self):
        token = getUserToken(self)
        storeClient.rmdir (token, name=self.name.value)


class Resolve(Task):
    ''' 
        Resolve a vos short form identifier     -- FIXME
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'resolve',
                      'Resolve a short form Virtual Storage identifier')
        self.addOption("name", 
            Option("name", "", "Short form identifier", required=True,
                default="vos://"))

    def run(self):
        token = getUserToken(self)
        r = requests.get(SM_URL + "resolve?name=%s" %
                         self.name.value, headers={'X-DL-AuthToken': token})


class StorageProfiles(Task):
    '''
        List the available Storage Manager profiles.
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'list_storage_profiles', 
            'List the available Storage Manager profiles')
        self.addOption("profile", 
            Option("profile", "", "Profile to list", required=False,
                default=None))
        self.addOption("format", 
            Option("format", "", "Output format (csv|text)",
                required=False, default='text'))

    def run(self):
        token = getUserToken(self)
        print (str(storeClient.list_profiles (token, 
                profile=self.profile.value, format=self.format.value)))


################################################
#  SAMP Tasks
################################################

class Broadcast(Task):
    ''' 
        Broadcast a SAMP message
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'broadcast', 'broadcast a SAMP message')
        self.addOption("type", 
            Option("mtype", "", "SAMP message type", required=True))
        self.addOption("pars", 
            Option("pars", "", "Message parameters", required=True))

    def run(self):
        client = self.getSampClient()
        mtype = self.mtype.value
        params = {}
        for param in self.pars.split(","):
            key, value = param.split("=")
            params[key] = value
        self.broadcast(client, mtype, params)


class Launch(Task):
    '''
        Launch a plugin in Data Lab
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'launch', 'launch a plugin')
        self.addOption("dir", 
            Option("dir", "", "directory in Data Lab to delete",
                required=True, default="vos://"))

    def run(self):
        token = getUserToken(self)
        r = requests.get(SM_URL + "/rmdir?dir=%s" %
                         self.dir.value, headers={'X-DL-AuthToken': token})


class Receiver():
    '''
        SAMP listener
    '''
    def __init__(self, client):
        self.client = client
        self.received = False

    def receive_notifications(self, private_key, sender_id, mtype, 
                                params, extra):
        self.params = params
        self.received = True
        print ('Notification:', private_key, sender_id, mtype, params, extra)

    def receiver_call(self, private_key, sender_id, msg_id, mtype, 
                                params, extra):
        self.params = params
        self.received = True
        print ('Call:', private_key, sender_id, msg_id, mtype, params, extra)
        self.client.reply(
            msg_id, {'samp.status': 'samp.ok', 'samp.result': {}})

    def point_select(self, private_key, send_id, mtype, params, extra):
        self.params = params
        self.received = True



################################################
#  SIA Tasks
################################################

class SiaQuery(Task):
    '''
        SIA query with an uploaded file
    '''
    def __init__(self, datalab):
        Task.__init__(self, datalab, 'siaquery',
                      'query a SIA service in the Data Lab')
        self.addOption("out", 
            Option("out", "", "Output filename", required=False))
        self.addOption("input", 
            Option("input", "", "Input filename", required=False))
        self.addOption("search", 
            Option("search", "", "Search radius", required=False, default=0.5))

    def getUID(self):
        ''' Get the UID for this user
        '''
        token = getUserToken(self)
        parts = token.strip().split(".")
        uid = parts[1]
        return uid

    def run(self):
        token = getUserToken(self)

        # If local input file, upload it
        input = self.input.value
        shortname = '%s_%s' % (self.getUID(), input[input.rfind('/') + 1:])
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
            QM_URL, shortname, self.search.value, self.out.value)
        r = requests.get(dburl, headers=headers)

        # Output value
        output = self.out.value
        if output != '':
            if output[:output.index(':')] not in ['vos']:
                file = open(output, 'wb')
                file.write(r.content)
                file.close()
        else:
            print (r.content)
