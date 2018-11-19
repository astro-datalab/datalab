#!/usr/bin/env python
#
# RESCLIENT -- Client methods for the Data Lab Resource Management Service
#

""" Client methods for the Data Lab Resource Management Service.
"""

from __future__ import print_function

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20180321'  # yyyymmdd


import requests
import os


# The URL of the ResManager service to contact.  This may be changed by
# passing a new URL into the set_svc_url() method before beginning.

DEF_SERVICE_URL = "https://datalab.noao.edu/res"

# The requested service "profile".  A profile refers to the specific
# machines and services used by the ResManager on the server.

DEF_SERVICE_PROFILE = "default"


# API debug flag.
DEBUG = False



# ######################################################################
#
#  Resource Management Client Interface
#
#  This API provides convenience methods that allow an application to
#  import the Client class without having to explicitly instantiate a
#  class object.  The parameter descriptions and example usage is given
#  in the comments for the class methods.
#
# ######################################################################

def createUser (username, password, email, name, institute):
    try:
        resp = client.createUser (username, password, email, name, institute)
    except dlResError as e:
        resp = str (e)

    return resp

def deleteUser (token, username):
    return client.deleteUser (token, username)

def getUser (token, keyword):
    return client.getUser (token, keyword)

def setUser (token, keyword, value):
    return client.setUser (token, keyword, value)

def passwordReset (token, user, password):
    return client.passwordReset (token, user, password)

def sendPasswordLink (token, user):
    return client.sendPasswordLink (token, user)

def listFields ():
    return client.listFields ()


# Group functions
def createGroup (groupName):
    return client.createGroup (groupName)

def getGroup (keyword):
    return client.getGroup (keyword)

def setGroup (keyword, value):
    return client.setGroup (keyword, value)

def deleteGroup (groupName):
    return client.deleeteGroup (groupName)


# Resource functions
def createResource (resource):
    return client.createResource (resource)

def getResource (keyword):
    return client.getResource (keyword)

def setResource (keyword, value):
    return client.setResource (keyword, value)

def deleteResource (resource):
    return client.deleteResource (resource)


# Service methods
def set_svc_url (svc_url):
    return client.set_svc_url (svc_url)

def get_svc_url ():
    return client.get_svc_url ()

def set_profile (profile):
    return client.set_profile (profile)

def get_profile ():
    return client.get_profile ()

def list_profiles (token, profile=None, format='text'):
    return client.list_profiles (token, profile, format)



# ###################################
#  Resource Management error class
# ###################################

class dlResError (Exception):
    """ A throwable error class.
    """
    def __init__(self, message):
        self.message = message

    def __str__ (self):
        return self.message


#####################################
#  Resource Management client procedures
#####################################

class resClient (object):
    """
         RESCLIENT -- Client-side methods to access the Data Lab
                       Resource Management Service.
    """

    def __init__ (self):
        """ Initialize the Resource Manager client. """

        self.svc_url = DEF_SERVICE_URL          # service URL
        self.svc_profile = DEF_SERVICE_PROFILE  # service prfile
        self.auth_token = None

        # Get the $HOME/.datalab directory.
        self.home = '%s/.datalab' % os.path.expanduser('~')

        self.debug = DEBUG                      # interface debug flag


    def set_svc_url (self, svc_url):
        """ Set the URL of the Resource Management Service to be used.

        Parameters
        ----------
        svc_url : str
            Resource Management service base URL to call.

        Returns
        -------
        Nothing

        Example
        -------
        .. code-block:: python

            from dl import resMgr
            resMgr.client.set_svc_url ("http://localhost:7001/")
        """

        self.svc_url = svc_url


    def get_svc_url (self):
        """ Return the currently-used Resource Management Service URL.

        Parameters
        ----------
        None

        Returns
        -------
        service_url : str
            The currently-used Resource Management Service URL.

        Example
        -------
        .. code-block:: python

            from dl import resMgr
            service_url = resMgr.client.get_svc_url ()
        """

        return self.svc_url


    def set_profile (self, profile):
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

            from dl import resMgr
            token = resMgr.client.set_profile ("dev")
        """

        self.svc_profile = profile


    def get_profile (self):
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

            from dl import resMgr
            profile = resMgr.client.get_profile ()
        """

        return self.svc_profile


    def list_profiles (self, token, profile=None, format='text'):

        """ List the service profiles which can be accessed by the user.

        Returns
        -------
        profiles : JSON string

        Example
        -------
        .. code-block:: python

            from dl import resMgr
            profiles = resMgr.client.list_profiles (token, profile, format)
        """

        pass


    def isAlive (self, svc_url):
        """ Check whether the ResManager service at the given URL is
            alive and responding.  This is a simple call to the root
            service URL or ping() method.
        """
        try:
            r = requests.get(svc_url)
            if r.status_code != 200:
                raise Exception (r.text)
        except Exception:
            return False
        else:
            return True



    ###################################################
    #  USER MANAGEMENT
    ###################################################

    def createUser (self, username, password, email, name, institute):
        """ Create a new user in the system.
        """
        url = self.svc_url + "/create?what=user&"

        query_args = { "username" : username,
                       "password" : password,
                       "email" : email,
                       "name" : name,
                       "institute" : institute,
                       "profile" : self.svc_profile,
                       "debug" : self.debug }
        try:
            headers = {'X-DL-AuthToken': self.auth_token}
            r = requests.get(url, params=query_args, headers=headers)
        except Exception as e:
            raise dlResError ("raise Error creating user '" +
                  username + "' : " + str(e.message) + "'")
        else:
            if self.debug:
                print ('r.text = ' + str(r.text))
                print ('code = ' + str(r.status_code))
            response = r.text

        return response

    def getUser (self, keyword):
        """ Read info about a user in the system.
        """
        return self.clientRead ("user", keyword)

    def setUser (self, keyword, value):
        """ Update info about a user in the system.
        """
        return self.clientUpdate ("user", keyword)

    def deleteUser (self, token, username):
        """ Delete a user in the system.
        """
        query_args = { "username" : username,
                       "profile" : self.svc_profile,
                       "debug" : self.debug }

        return self.clientDelete (token, "user", query_args)



    ###################################
    #  Account Admin Methods
    ###################################

    def svcGet (self, token, url):
        """ Get the named user's account status
        """
        try:
            if self.debug:
                print ("url = '" + url + "'")

            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': token}
            r = requests.get(url, headers=headers)
            response = r.text

            if r.status_code == 302:
                return "OK"
            elif r.status_code != 200:
                raise Exception (r.text)

        except Exception as e:
            raise dlResError (str(e))

        return response


    def passwordReset (self, token, user, password):
        """ Change a user's password.
        """
        url = self.svc_url + ("/pwReset?user=%s&password=%s" % (user, password))

        try:
            resp = self.svcGet (token, url)
            print (resp)
        except Exception as e:
            raise Exception (str(e))
        else:
            # Service call was successful.
            print ("passwordReset:  success, removing local token file")
            tok_file = ('%s/id_token.%s' % (self.home, user))
            if os.path.exists (tok_file):
                os.remove(tok_file)


    def sendPasswordLink (self, token, user):
        """ Send a password-reset link to the user.
        """
        url = self.svc_url + ("/pwResetLink?user=%s" % user)

        try:
            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': token}
            r = requests.get(url, headers=headers)

            if r.status_code == 200:
                return "OK"
            else:
                raise Exception (r.text)

        except Exception as e:
            raise Exception (str(e))


    def listFields (self):
        """ List available user fields.
        """
        url = self.svc_url + ("/listFields")

        try:
            return (self.svcGet (self.auth_token, url))
        except Exception as e:
            raise Exception (str(e))

        pass

    def approveUser (self, token, user):
        """ Approve a pending user request
        """
        url = self.svc_url + "/approveUser?approve=True&user=" + user

        try:
            return (self.svcGet (token, url))
        except Exception as e:
            raise Exception (str(e))


    def disapproveUser (self, token, user):
        """ Disapprove a pending user request
        """
        url = self.svc_url + "/approveUser?approve=False&user=" + user

        try:
            return (self.svcGet (token, url))
        except Exception as e:
            raise Exception (str(e))


    def userRecord (self, token, user, value, format):
        """ Get a value from the User record.  The special 'all' value will
            return all fields accessible to the token.  The 'format' will
            return either 'text' for a single value, or 'json' for a complete
            record.
        """
        url = self.svc_url + \
                ("/userRecord?user=%s&value=%s&fmt=%s" % (user,value,format))

        try:
            resp = self.svcGet (token, url)
        except Exception as e:
            raise Exception (str(e))
        else:
            return resp

        return "OK"


    def listPending (self, token, verbose=False):
        """ List all pending user accounts.
        """
        url = self.svc_url + "/pending?verbose=" + str(verbose)

        try:
            resp = self.svcGet (token, url)
        except Exception as e:
            raise Exception (str(e))
        else:
            return resp

        return "OK"


    def setField (self, token, user, field, value):
        """ Set a specific user record field
        """
        url = self.svc_url + "/setField?user=%s&field=%s&value=%s" % \
                       (user, field, value)

        try:
            resp = self.svcGet (token, url)
        except Exception as e:
            raise Exception (str(e))
        else:
            return resp

        return "OK"



    ###################################################
    #  GROUP MANAGEMENT
    ###################################################

    def createGroup (self, name):
        """ Create a new Group in the system.
        """
        url = self.svc_url + "/create?what=group&"

        query_args = { "name" : name,
                       "profile" : self.svc_profile,
                       "debug" : self.debug }
        try:
            if self.debug:
                print ("createGroup: " + name)

            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': self.auth_token}

            r = requests.get(url, params=query_args, headers=headers)
            response = r.text

            if r.status_code != 200:
                raise Exception (r.text)

        except Exception:
            raise dlResError (response)
        else:
            pass

        return response

    def getGroup (self, keyword):
        """ Read info about a Group in the system.
        """
        return self.clientRead ("group", keyword)

    def setGroup (self, keyword, value):
        """ Update info about a Group in the system.
        """
        return self.clientUpdate ("group", keyword, value)

    def deleteGroup (self, token, group):
        """ Delete a Group in the system.
        """
        query_args = { "group" : group,
                       "profile" : self.svc_profile,
                       "debug" : self.debug }

        return self.clientDelete (token, "group", query_args)




    ###################################################
    #  RESOURCE MANAGEMENT
    ###################################################

    def createResource (self, resource):
        """ Create a new Resource in the system.
        """
        url = self.svc_url + "/create?what=resource&"

        query_args = { "resource" : resource,
                       "profile" : self.svc_profile,
                       "debug" : self.debug }
        try:
            if self.debug:
                print ("createResource: " + resource)

            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': self.auth_token}

            r = requests.get(url, params=query_args, headers=headers)
            response = r.text

            if r.status_code != 200:
                raise Exception (r.text)

        except Exception:
            raise dlResError (response)
        else:
            pass

        return response

    def getResource (self, keyword):
        """ Read info about a Resource in the system.
        """
        return self.clientRead ("resource", keyword)

    def setResource (self, keyword, value):
        """ Update info about a Resource in the system.
        """
        return self.clientUpdate ("resource", keyword, value)

    def deleteResource (self, token, resource):
        """ Delete a Group in the system.
        """
        query_args = { "resource" : resource,
                       "profile" : self.svc_profile,
                       "debug" : self.debug }

        return self.clientDelete (token, "resource", query_args)




    ###################################################
    #  PRIVATE UTILITY METHODS
    ###################################################

    def debug (self, debug_val):
        self.debug = debug_val

    def retBoolValue (self, url):
        """  Utility method to call a boolean service at the given URL.
        """
        try:
            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': self.auth_token}

            r = requests.get(url, headers=headers)
            response = r.text

            if r.status_code != 200:
                raise Exception (r.text)

        except Exception:
            raise dlResError ("Invalid user")
        else:
            return response

    def clientRead (self, what, keyword):
        """ Generic method to call a /get service.
        """
        url = self.svc_url + "/get?what=" + what + "&"

        query_args = { "keyword" : keyword,
                       "profile" : self.svc_profile,
                       "debug" : self.debug }
        try:
            if self.debug:
                print ("get" + what + ": url = '" + url + "'")

            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': self.auth_token}

            r = requests.get(url, params=query_args, headers=headers)
            response = r.text

            if r.status_code != 200:
                raise Exception (r.text)

        except Exception:
            raise dlResError (response)
        else:
            pass

        return response

    def clientUpdate (self, what, keyword, value):
        """ Generic method to call a /set service.
        """
        url = self.svc_url + "/set?what=" + what + "&"

        query_args = { "keyword" : keyword,
                       "value" : value,
                       "profile" : self.svc_profile,
                       "debug" : self.debug }
        try:
            if self.debug:
                print ("set" + what + ": url = '" + url + "'")

            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': self.auth_token}

            r = requests.get(url, params=query_args, headers=headers)
            response = r.text

            if r.status_code != 200:
                raise Exception (r.text)

        except Exception:
            raise dlResError (response)
        else:
            pass

        return response

    def clientDelete (self, token, what, query_args):
        """ Generic method to call a /delete service.
        """
        url = self.svc_url + "/delete?what=" + what + "&"

        try:
            if self.debug:
                print ("delete" + what + ": url = '" + url + "'")

            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': token}

            r = requests.get(url, params=query_args, headers=headers)
            response = r.text

            if r.status_code != 200:
                raise Exception (r.text)

        except Exception:
            raise dlResError (response)
        else:
            pass

        return response




# ###################################
#  Resource Management Client Handles
# ###################################

def getClient ():
    return resClient ()

client = getClient()

