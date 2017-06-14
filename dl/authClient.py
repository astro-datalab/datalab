#!/usr/bin/env python
#
# AUTHCLIENT -- Client methods for the Data Lab Authentication Service
#

from __future__ import print_function

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20170530'  # yyyymmdd


""" 
    Client methods for the Data Lab Authentication Service.

Import via

.. code-block:: python

    from dl import authClient
"""

try:
    from urllib import urlencode		        # Python 2
    from urllib2 import urlopen, Request                # Python 2
except ImportError:
    from urllib.parse import urlencode		        # Python 3
    from urllib.request import urlopen, Request         # Python 3
import requests
import os


# Pre-defined authentication tokens. These are fixed strings that provide
# limited access to Data Lab services, this access is controlled on the
# server-side so we don't need strict security here.

ANON_TOKEN = "anonymous.0.0.anon_access"
DEMO_TOKEN = "dldemo.99999.99999.demo_access"
TEST_TOKEN = "dltest.99998.99998.test_access"


# The URL of the AuthManager service to contact.  This may be changed by
# passing a new URL into the set_service() method before beginning.

DEF_SERVICE_URL = "https://dlsvcs.datalab.noao.edu/auth"

# The requested authentication "profile".  A profile refers to the specific
# machines and services used by the AuthManager on the server.

DEF_SERVICE_PROFILE = "default"

# Set the default user accounts for the authentication service.  We don't
# include privileged users so that account can remain secure.

DEF_USERS = {'anonymous': ANON_TOKEN,
             'dldemo': DEMO_TOKEN,
             'dltest': TEST_TOKEN}

# API debug flag.
DEBUG = False


# ######################################################################
#
#  Authentication Client Interface
#
#  This API provides convenience methods that allow an application to
#  import the Client class without having to explicitly instantiate a
#  class object.  The parameter descriptions and example usage is given
#  in the comments for the class methods.
#
# ######################################################################


# User methods -- All methods except login() return either a 'True' string
# or an error of the form 'ERR <message>'.  On success, the login() method
# will return the user if token.

def login(user, password=None, debug=False, verbose=False):
    if user in list(DEF_USERS.keys()):
        return DEF_USERS[user]
    else:
        try:
            response = client.login(user, password, debug)
        except Exception as e:
            response = str(e)
    return response


def isAlive(svc_url=DEF_SERVICE_URL):
    try:
        response = client.isAlive(svc_url)
    except Exception as e:
        repsponse = str(e)
    return response


def isValidToken(token):
    try:
        user, uid, gid, hash = token.strip().split('.', 3)
    except Exception as e:
        return False

    if user in list(DEF_USERS.keys()) and token in list(DEF_USERS.values()):
        return True
    else:
        try:
            response = client.isValidToken(token)
        except Exception as e:
            print (str(e))
            return False

    return response


def isValidUser(user):
    if user in list(DEF_USERS.keys()):
        return True
    else:
        try:
            response = client.isValidUser(user)
        except Exception as e:
            response = str(e)
    return response


def isValidPassword(user, password):
    if (user == password) and (user in list(DEF_USERS.keys())):
        return True
    else:
        try:
            response = client.isValidPassword(user, password)
        except Exception as e:
            response = str(e)
    return response


def hasAccess(user, resource):
    try:
        response = client.hasAccess(user, resource)
    except Exception as e:
        response = str(e)
    return response


def isUserLoggedIn(user):
    try:
        response = client.isUserLoggedIn(user)
    except Exception as e:
        response = str(e)
    return response


def isTokenLoggedIn(token):
    try:
        response = client.isTokenLoggedIn(token)
    except Exception as e:
        response = str(e)
    return response


def logout(token):
    try:
        response = client.logout(token)
    except Exception as e:
        response = str(e)
    return response


def passwordReset(token, username, password):
    try:
        response = client.passwordReset(token, username, password)
    except Exception as e:
        response = str(e)
    return response


# Standard Service Methods
def set_service(svc_url):
    return client.set_service(svc_url)


def get_service():
    return client.get_service()


def set_profile(profile):
    return client.set_profile(profile)


def get_profile():
    return client.get_profile()


def list_profiles(token):
    return client.list_profiles(token)


# ###################################
#  Authentication error class
# ###################################

class dlAuthError (Exception):
    """ A throwable error class.
    """

    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message
        

#####################################
#  Authentication client procedures
#####################################

class authClient (object):
    """  
         AUTHCLIENT -- Client-side methods to access the Data Lab 
                       Authentication Service.
    """

    def __init__(self):
        """ Initialize the authorization client. """

        self.svc_url = DEF_SERVICE_URL	        # service URL
        self.svc_profile = DEF_SERVICE_PROFILE  # service prfile
        self.username = ""			# default client logn user
        self.auth_token = None			# default client logn token

        # Get the $HOME/.datalab directory.
        self.home = '%s/.datalab' % os.path.expanduser('~')

        self.debug = DEBUG			# interface debug flag

    def set_service(self, svc_url):
        """ Set the URL of the Authentication Service to be used.

        Parameters
        ----------
        svc_url : str
            Authentication service base URL to call.

        Returns
        -------
        Nothing

        Example
        -------
        .. code-block:: python

            from dl import authMgr
            authMgr.client.set_service ("http://localhost:7001/")
        """

        self.svc_url = svc_url

    def get_service(self):
        """ Return the currently-used Authentication Service URL.

        Parameters
        ----------
        None

        Returns
        -------
        service_url : str
            The currently-used Authentication Service URL.

        Example
        -------
        .. code-block:: python

            from dl import authMgr
            service_url = authMgr.client.get_service ()
        """

        return self.svc_url

    def set_profile(self, profile):
        """ Set the requested service profile.

        Parameters
        ----------
        profile : str
            Requested service profile string.

        Returns
        -------
        Nothing

        Example
        -------
        .. code-block:: python

            from dl import authMgr
            token = authMgr.client.set_profile ("dev")
        """

        self.svc_profile = profile

    def get_profile(self):
        """ Get the requested service profile.

        Parameters
        ----------
        None

        Returns
        -------
        profile : str
            The currently requested service profile.

        Example
        -------
        .. code-block:: python

            from dl import authMgr
            profile = authMgr.client.get_profile ()
        """

        return self.svc_profile

    def list_profiles(self, token):
        """ List the service profiles which can be accessed by the user.

        Parameters
        ----------
        token : str
            Valid auth service token.

        Returns
        -------
        profiles : JSON string

        Example
        -------
        .. code-block:: python

            from dl import authMgr
            profiles = authMgr.client.list_profiles (token)
        """

        pass

    def isAlive(self, svc_url):
        """ Check whether the AuthManager service at the given URL is
            alive and responding.  This is a simple call to the root 
            service URL or ping() method.
        """
        try:
            request = Request(svc_url)
            response = urlopen(request,timeout=2)
            output = response.read()
            status_code = response.code
        except Exception:
            return False
        else:
            return (True if (output is not None and status_code == 200) else False)


    ###################################################
    #  SESSION MANAGEMENT
    ###################################################

    def login(self, username, password, debug=False, verbose=False):
        """ Authenticate the user with the Authentication Service.
            We first check for a valid login token in the user's
            $HOME/.datalab/ directory and simply return that rather
            than make a service call to get a new token.  If a token
            exists but is invalid, we remove it and get a new
            token.  In either case, we save the token for later use.

        Parameters
        ----------
        username : str
            User login name.

        password : str
            User password.  If not given, a valid ID token will be
            searched for in the $HOME/.datalab directory.

        debug : bool
            Method debug flag.
        verbose : bool
            Initialize session to print verbose output messages.

        Returns
        -------
        token : str
            One-time security token for valid user (identified via
            'username' and 'password').

        Example
        -------
        .. code-block:: python

            from dl import authMgr
            token = authMgr.login ('dldemo', 'dldemo')   # get security token
        """

        # Check the $HOME/.datalab directory for a valid token.  If that dir
        # doesn't already exist, create it so we can store the new token.
        if not os.path.exists(self.home):
            os.makedirs(self.home)

        # See if a datalab token file exists for the requested user.
        tok_file = ('%s/id_token.%s' % (self.home, username))
        if self.debug:
            print ("top of login: tok_file = '" + tok_file + "'")
            print ("top of login: self.auth_token = '%s'" % str(self.auth_token))
            print ("top of login: token = ")
            os.system('cat ' + tok_file)

        if password is None:
            if os.path.exists(tok_file):
                tok_fd = open(tok_file, "r")
                o_tok = tok_fd.read(128)		# read the old token

                # Return a valid token, otherwise remove the file and obtain a
                # new one.
                if o_tok.startswith(username + '.') and self.isValidToken(o_tok):
                    self.username = username
                    self.auth_token = o_tok
                    if self.debug:
                        print ("using old token for '%s'" % username)
                    return o_tok
                else:
                    if self.debug:
                        print ("removing token file '%s'" % tok_file)
                    os.remove(tok_file)

        # Either the user is not logged in or the token is invalid, so
        # make a service call to get a new token.
        url = self.svc_url + "/login?"
        query_args = {"username": username,
                      "password": password,
                      "profile": self.svc_profile,
                      "debug": self.debug}

        response = 'None'
        try:
            r = requests.get(url, params=query_args)
            response = r.text

            if self.debug:
                print ('resp = ' + response)
                print ('code = ' + str(r.status_code))
            if r.status_code != 200:
                raise Exception(r.text)

        except Exception as e:
            if self.debug:
                print ("Raw exception msg = '%s'" + str(e))
            if self.isAlive(self.svc_url) == False:
                raise dlAuthError("AuthManager Service not responding.")
            if self.isValidUser(username):
                if password is None:
                    if not os.path.exists(tok_file):
                        raise dlAuthError("No password or token supplied")
                    else:
                        raise dlAuthError("No password supplied")
                elif not self.isValidPassword(username, password):
                    raise dlAuthError("Invalid password")
                else:
                    raise dlAuthError(str(e))
            else:
                raise dlAuthError("Invalid username")

        else:
            self.auth_token = response
            self.username = username

        # Save the token.
        if os.access(self.home, os.W_OK):
            tok_file = '%s/id_token.%s' % (self.home, username)
            with open(tok_file, 'w') as tok_fd:
                if self.debug:
                    print ("login: writing new token for '%s'" % username)
                    print ("login: self.auth_token = '%s'" % str(self.auth_token))
                    print ("login: token = ")
                    os.system('cat ' + tok_file)

                tok_fd.write(self.auth_token)
                tok_fd.close()
                
        return self.auth_token

    def logout(self, token):
        """ Log the user out of the Data Lab.
        """
        url = self.svc_url + "/logout?"
        args = urlencode({"token": token,
                                 "debug": self.debug})
        url = url + args

        if self.debug:
            print ("logout: token = '%s'" % token)
            print ("logout: auth_token = '%s'" % self.auth_token)
            print ("logout: url = '%s'" % url)

        if not self.isValidToken(token):
            return "Error: Invalid user token"

        try:
            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': token}

            r = requests.get(url, params=args, headers=headers)
            response = r.text
            
            if r.status_code != 200:
                raise Exception(r.text)
            
        except Exception as e:
            raise dlAuthError(str(e))
        else:
            self.auth_token = None
            tok_file = self.home + '/id_token.' + self.username
            if os.path.exists(tok_file):
                os.remove(tok_file)
                
        return response

    def passwordReset(self, token, username, password):
        """  Reset a user password reset.  We require that the user provide
             either a valid 'root' token or the token for the account being
             reset.
        """
        url = self.svc_url + "/passwordReset?"
        args = urlencode({"token": token,
                                 "username": username,
                                 "password": password,
                                 "debug": self.debug})
        url = url + args

        if self.debug:
            print ("passwdReset: token = '%s'" % token)
            print ("passwdReset: auth_token = '%s'" % self.auth_token)
            print ("passwdReset: url = '%s'" % url)

        if not self.isValidToken(token):
            return "Error: Invalid user token"

        if self.auth_token is None:
            return "Error: User is not currently logged in"
        else:
            user, uid, gid, hash = self.auth_token.strip().split('.', 3)
            if user != 'root' and user != username:
                return "Error: Invalid user or non-root token"

        try:
            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': token}

            r = requests.get(url, params=args, headers=headers)
            response = r.text

            if r.status_code != 200:
                raise Exception(r.text)

        except Exception as e:
            raise dlAuthError(str(e))
        else:
            # Update the saved user token.
            if response is not None:
                self.auth_token = response
                tok_file = self.home + '/id_token.' + self.username
                if os.path.exists(tok_file):
                    os.remove(tok_file)
                with open(tok_file, 'w') as tok_fd:
                    if self.debug:
                        print ("pwreset: writing new token for '%s'" + username)
                        print ("pwreset: response = '%s'" + response)
                        print ("pwreset: token = '%s'" + self.auth_token)
                    tok_fd.write(self.auth_token)
                    tok_fd.close()
            else:
                print ('pwReset response is None')

        return response


    def hasAccess(self, user, resource):
        """  See whether the user has access to the named Resource.  Returns
             True if the user owns the Resource, or if the Resource grants
             group permissions to a Group to which the user belongs.
        """
        # Either the user is not logged in or the token is invalid, so
        # make a service call to get a new token.
        url = self.svc_url + "/hasAccess?"
        args = urlencode({"user": user,
                                 "resource": resource,
                                 "profile": self.svc_profile})
        url = url + args

        if self.debug:
            print ("hasAccess: url = '%s'" % url)

        return self.retBoolValue(url)

    def isValidToken(self, token):
        """ See whether the current token is valid.
        """
        url = self.svc_url + "/isValidToken?"
        args = urlencode({"token": token,
                                 "profile": self.svc_profile})
        url = url + args

        if self.debug:
            print ("isValidToken: url = '%s'" % url)

        return self.retBoolValue(url)

    def isValidPassword(self, user, password):
        """ See whether the password is valid for the user.
        """
        url = self.svc_url + "/isValidPassword?"
        args = urlencode({"user": user,
                                 "password": password,
                                 "profile": self.svc_profile})
        url = url + args

        if self.debug:
            print ("isValidPassword: url = '%s'" % url)

        try:
            val = self.retBoolValue(url)
        except Exception:
            val = "False"
	
        return val

    def isValidUser(self, user):
        """ See whether the specified user is valid.
        """
        url = self.svc_url + "/isValidUser?"
        args = urlencode({"user": user,
                                 "profile": self.svc_profile})
        url = url + args

        if self.debug:
            print ("isValidUser: url = '%s'" % url)

        try:
            val = self.retBoolValue(url)
        except Exception:
            val = "False"

        return val

    def isUserLoggedIn(self, user):
        """ See whether the user identified by the token is currently
            logged in.
        """
        url = self.svc_url + "/isUserLoggedIn?"
        args = urlencode({"user": user,
                                 "profile": self.svc_profile})
        url = url + args

        if self.debug:
            print ("isUserLoggedIn: url = '%s'" % url)

        try:
            val = self.retBoolValue(url)
        except Exception:
            val = "False"

        return val

    def isTokenLoggedIn(self, token):
        """ See whether the user identified by the token is currently
            logged in.
        """
        url = self.svc_url + "/isTokenLoggedIn?"
        args = urlencode({"token": token,
                                 "profile": self.svc_profile})
        url = url + args

        if self.debug:
            print ("isTokenLoggedIn: tok = '%s'" % token)
            print ("isTokenLoggedIn: url = '%s'" % url)

        try:
            val = self.retBoolValue(url)
        except Exception:
            val = "False"

        return val

    ###################################################
    #  PRIVATE UTILITY METHODS
    ###################################################

    def debug(self, debug_val):
        self.debug = debug_val

    def retBoolValue(self, url):
        """  Utility method to call a boolean service at the given URL.
        """
        response = ""
        try:
            # Add the auth token to the reauest header.
            if self.auth_token != None:
                headers = {'X-DL-AuthToken': self.auth_token}
                r = requests.get(url, headers=headers)
            else:
                r = requests.get(url)
            response = r.text

            if r.status_code != 200:
                raise Exception(r.text)

        except Exception as e:
            return str(e)
        else:
            return response


# ###################################
#  Authentication Client Handles
# ###################################

def getClient():
    return authClient()

client = getClient()
