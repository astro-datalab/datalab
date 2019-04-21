#!/usr/bin/env python
#
# RESCLIENT -- Client methods for the Data Lab Resource Management Service
#

'''Client methods for the Data Lab Resource Management Service.

    Resource Manager
    ----------------
            createUser  (username, password, email, name, institute)
            deleteUser  (token, username)
               getUser  (token, keyword)
               setUser  (token, keyword, value)
         passwordReset  (token, user, password)
      sendPasswordLink  (token, user)
            listFields  ()

           createGroup  (token, groupName)
              getGroup  (token, keyword)
              setGroup  (token, keyword, value)
           deleteGroup  (token, groupName)

        createResource  (token, resource)
           getResource  (token, keyword)
           setResource  (token, keyword, value)
        deleteResource  (token, resource)

           set_svc_url  (svc_url)
           get_svc_url  ()
           set_profile  (profile)
           get_profile  ()
         list_profiles  (token, profile=None, format='text')


Import via

.. code-block:: python

    from dl import resClient
'''

from __future__ import print_function

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>, Data Lab <datalab@noao.edu>'
__version__ = '20180422'  # yyyymmdd


import requests
import os

if os.path.isfile ('./Util.py'):                # use local dev copy
    from Util import def_token
else:                                           # use distribution copy
    from dl.Util import def_token


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
#  in the comments for the class methods. Module methods have their
#  docstrings patched below.
#
# ######################################################################

def createUser(username, password, email, name, institute):
    try:
        resp = client.createUser(username, password, email, name, institute)
    except dlResError as e:
        resp = str(e)

    return resp

def deleteUser(token, username):
    return client.deleteUser(token, username)

def getUser(token, keyword):
    return client.getUser(token, keyword)

def setUser(token, keyword, value):
    return client.setUser(token, keyword, value)

def passwordReset(token, user, password):
    return client.passwordReset(token, user, password)

def sendPasswordLink(token, user):
    return client.sendPasswordLink(token, user)

def listFields():
    return client.listFields()


# Group functions
def createGroup(token, groupName):
    return client.createGroup(token, groupName)

def getGroup(token, keyword):
    return client.getGroup(token, keyword)

def setGroup(token, keyword, value):
    return client.setGroup(token, keyword, value)

def deleteGroup(token, groupName):
    return client.deleeteGroup(token, groupName)


# Resource functions
def createResource(token, resource):
    return client.createResource(token, resource)

def getResource(token, keyword):
    return client.getResource(token, keyword)

def setResource(token, keyword, value):
    return client.setResource(token, keyword, value)

def deleteResource(token, resource):
    return client.deleteResource(token, resource)


# Service methods
def set_svc_url(svc_url):
    return client.set_svc_url(svc_url.strip('/'))

def get_svc_url():
    return client.get_svc_url()

def set_profile(profile):
    return client.set_profile(profile)

def get_profile():
    return client.get_profile()

def list_profiles(token, profile=None, format='text'):
    return client.list_profiles(token, profile, format)



# ###################################
#  Resource Management error class
# ###################################

class dlResError(Exception):
    '''A throwable error class.
    '''
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


#####################################
#  Resource Management client procedures
#####################################

class resClient(object):
    '''
         RESCLIENT -- Client-side methods to access the Data Lab
                       Resource Management Service.
    '''

    def __init__(self):
        '''Initialize the Resource Manager client. '''
        self.svc_url = DEF_SERVICE_URL          # service URL
        self.svc_profile = DEF_SERVICE_PROFILE  # service prfile
        self.auth_token = None

        # Get the $HOME/.datalab directory.
        self.home = '%s/.datalab' % os.path.expanduser('~')

        self.debug = DEBUG                      # interface debug flag


    def set_svc_url(self, svc_url):
        '''Set the URL of the Resource Management Service to be used.

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

            from dl import resClient
            resClient.client.set_svc_url("http://localhost:7001/")
        '''
        self.svc_url = svc_url.strip('/')


    def get_svc_url(self):
        '''Return the currently-used Resource Management Service URL.

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

            from dl import resClient
            service_url = resClient.client.get_svc_url()
        '''
        return self.svc_url


    def set_profile(self, profile):
        '''Set the requested service profile.

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

            from dl import resClient
            token = resClient.client.set_profile("dev")
        '''
        self.svc_profile = profile


    def get_profile(self):
        '''Get the requested service profile.

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

            from dl import resClient
            profile = resClient.client.get_profile()
        '''
        return self.svc_profile

    def list_profiles(self, token, profile=None, format='text'):
        '''List the service profiles which can be accessed by the user.

        Returns
        -------
        profiles : JSON string

        Example
        -------
        .. code-block:: python

            from dl import resClient
            profiles = resClient.client.list_profiles(token, profile, format)
        '''
        pass

    def isAlive(self, svc_url):
        '''Check whether the ResManager service at the given URL is
            alive and responding.  This is a simple call to the root
            service URL or ping() method.

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

            from dl import resClient
            resClient.client.set_svc_url("http://localhost:7001/")
        '''
        try:
            r = requests.get(svc_url)
            if r.status_code != 200:
                raise Exception(r.text)
        except Exception:
            return False
        else:
            return True



    ###################################################
    #  USER MANAGEMENT
    ###################################################

    def createUser(self, username, password, email, name, institute):
        '''Create a new user in the system.

        Parameters
        ----------
        username : str
            Account username
        password : str
            Account password
        email : str
            User's contact email address
        name : str
            User's full name
        institute : str
            User's home institution

        Returns
        -------
        Service response
        '''
        url = self.svc_url + "/create?what=user&"

        query_args = {"username" : username,
                      "password" : password,
                      "email" : email,
                      "name" : name,
                      "institute" : institute,
                      "profile" : self.svc_profile,
                      "debug" : self.debug}
        try:
            headers = {'X-DL-AuthToken': self.auth_token}
            r = requests.get(url, params=query_args, headers=headers)
        except Exception as e:
            raise dlResError("raise Error creating user '" +
                  username + "' : " + str(e.message) + "'")
        else:
            if self.debug:
                print('r.text = ' + str(r.text))
                print('code = ' + str(r.status_code))
            response = r.text

        return response

    def getUser(self, keyword):
        '''Read info about a user in the system.

        Parameters
        ----------
        keyword : str
            User record field to be set

        Returns
        -------
        Nothing

        Example
        -------
        .. code-block:: python

            from dl import resClient
            resClient.client.set_svc_url("http://localhost:7001/")
        '''
        return self.clientRead("user", keyword)

    def setUser(self, keyword, value):
        '''Update info about a user in the system.

        Parameters
        ----------
        keyword : str
            User record field to be set
        value : str
            Value of field to set

        Returns
        -------
        Service response
        '''
        return self.clientUpdate("user", keyword)

    def deleteUser(self, token, username):
        '''Delete a user in the system.

        Parameters
        ----------
        token : str
            User identity token.
        username : str
            Name of user to be deleted.

        Returns
        -------
        Service response
        '''
        query_args = {"username" : username,
                      "profile" : self.svc_profile,
                      "debug" : self.debug}

        return self.clientDelete(token, "user", query_args)



    ###################################
    #  Account Admin Methods
    ###################################

    def svcGet(self, token, url):
        '''Utility method to call a Resource Manager service.

        Parameters
        ----------
        token : str
            User identity token
        url : str
            URL to call with HTTP/GET

        Returns
        -------
        Service response
        '''
        try:
            if self.debug:
                print("url = '" + url + "'")

            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': token}
            r = requests.get(url, headers=headers)
            response = r.text

            if r.status_code == 302:
                return "OK"
            elif r.status_code != 200:
                raise Exception(r.text)

        except Exception as e:
            raise dlResError(str(e))

        return response


    def passwordReset(self, token, user, password):
        '''Change a user's password.

        Parameters
        ----------
        token : str
            User identity token
        user : str
            User account name.  Token must match the user or root token.
        password : str
            New password

        Returns
        -------
        Nothing

        Example
        -------
        .. code-block:: python

            from dl import resClient
            resClient.client.set_svc_url("http://localhost:7001/")
        '''
        url = self.svc_url + ("/pwReset?user=%s&password=%s" % (user, password))

        try:
            resp = self.svcGet(token, url)
            print(resp)
        except Exception as e:
            raise Exception(str(e))
        else:
            # Service call was successful.
            print("passwordReset:  success, removing local token file")
            tok_file = ('%s/id_token.%s' % (self.home, user))
            if os.path.exists(tok_file):
                os.remove(tok_file)


    def sendPasswordLink(self, token, user):
        '''Send a password-reset link to the user.

        Parameters
        ----------
        token : str
            User identity token
        user : str
            User account name.

        Returns
        -------
        Service response
        '''
        url = self.svc_url + ("/pwResetLink?user=%s" % user)

        try:
            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': token}
            r = requests.get(url, headers=headers)

            if r.status_code == 200:
                return "OK"
            else:
                raise Exception(r.text)

        except Exception as e:
            raise Exception(str(e))


    def listFields(self):
        '''List available user fields.

        Parameters
        ----------
        None

        Returns
        -------
        Service response
        '''
        url = self.svc_url + ("/listFields")

        try:
            return self.svcGet(self.auth_token, url)
        except Exception as e:
            raise Exception(str(e))

        pass

    def approveUser(self, token, user):
        '''Approve a pending user request.

        Parameters
        ----------
        token : str
            User identity token
        user : str
            User account to approve.  Token must have authority to manage users.

        Returns
        -------
        Service response
        '''
        url = self.svc_url + "/approveUser?approve=True&user=" + user

        try:
            return self.svcGet(token, url)
        except Exception as e:
            raise Exception(str(e))


    def disapproveUser(self, token, user):
        '''Disapprove a pending user request.

        Parameters
        ----------
        token : str
            User identity token
        user : str
            User account to decline.  Token must have authority to manage users.

        Returns
        -------
        Service response
        '''
        url = self.svc_url + "/approveUser?approve=False&user=" + user

        try:
            return self.svcGet(token, url)
        except Exception as e:
            raise Exception(str(e))


    def userRecord(self, token, user, value, format):
        '''Get a value from the User record.  

        Parameters
        ----------
        token : str
            User identity token
        user : str
            User account name to retrieve.  Token must match the use name
            or be a root token to access other records
        value : str
            Value to retrieve.  The special 'all' value will return all
            fields accessible to the token.  
        format : str
            'text' for a single value, or 'json' for a complete record

        Returns
        -------
        User record
        '''
        url = self.svc_url + \
                ("/userRecord?user=%s&value=%s&fmt=%s" % (user,value,format))

        try:
            resp = self.svcGet(token, url)
        except Exception as e:
            raise Exception(str(e))
        else:
            return resp

        return "OK"


    def listPending(self, token, verbose=False):
        '''List all pending user accounts.

        Parameters
        ----------
        token : str
            User identity token
        verbose : bool
            Return verbose listing?

        Returns
        -------
        List of use accounts pending approval
        '''
        url = self.svc_url + "/pending?verbose=" + str(verbose)

        try:
            resp = self.svcGet(token, url)
        except Exception as e:
            raise Exception(str(e))
        else:
            return resp

        return "OK"


    def setField(self, token, user, field, value):
        '''Set a specific user record field

        Parameters
        ----------
        token : str
            User identity token
        user : str
            User name to modify.  If None then identity is take from token,
            a root token is required to modify other users.
        field : str
            Record field to be set
        value : str
            Field value

        Returns
        -------
        'OK' is field was set, else a service error message.
        '''
        url = self.svc_url + "/setField?user=%s&field=%s&value=%s" % \
                       (user, field, value)

        try:
            resp = self.svcGet(token, url)
        except Exception as e:
            raise Exception(str(e))
        else:
            return resp

        return "OK"



    ###################################################
    #  GROUP MANAGEMENT
    ###################################################

    def createGroup(self, token, name):
        '''Create a new Group in the system.

        Parameters
        ----------
        name : str
            Group name to create

        Returns
        -------
        resp : str
            Service response
        '''
        url = self.svc_url + "/create?what=group&"

        query_args = {"name" : name,
                      "profile" : self.svc_profile,
                      "debug" : self.debug}
        try:
            if self.debug:
                print("createGroup: " + name)

            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': self.auth_token}

            r = requests.get(url, params=query_args, headers=headers)
            response = r.text

            if r.status_code != 200:
                raise Exception(r.text)

        except Exception:
            raise dlResError(response)
        else:
            pass

        return response

    def getGroup(self, token, keyword):
        '''Read info about a Group in the system.

        Parameters
        ----------
        keyword : str
            Group record field to retrieve

        Returns
        -------
        value : str
            Value of record field
        '''
        return self.clientRead("group", keyword)

    def setGroup(self, token, keyword, value):
        '''Update info about a Group in the system.

        Parameters
        ----------
        keyword : str
            Group record field to be set
        value : str
            Value of field to set

        Returns
        -------
        '''
        return self.clientUpdate("group", keyword, value)

    def deleteGroup(self, token, group):
        '''Delete a Group in the system.

        Parameters
        ----------
        token : str
            User identity token.
        group : str
            Name of Group to be deleted. The token must identify the owner
            of the Group to be deleted.

        Returns
        -------
        '''
        query_args = {"group" : group,
                      "profile" : self.svc_profile,
                      "debug" : self.debug}

        return self.clientDelete(token, "group", query_args)




    ###################################################
    #  RESOURCE MANAGEMENT
    ###################################################

    def createResource(self, token, resource):
        '''Create a new Resource in the system.

        Parameters
        ----------
        resource : str
            Resource URI to create

        Returns
        -------
        resp : str
            Service response
        '''
        url = self.svc_url + "/create?what=resource&"

        query_args = {"resource" : resource,
                      "profile" : self.svc_profile,
                      "debug" : self.debug}
        try:
            if self.debug:
                print("createResource: " + resource)

            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': self.auth_token}

            r = requests.get(url, params=query_args, headers=headers)
            response = r.text

            if r.status_code != 200:
                raise Exception(r.text)

        except Exception:
            raise dlResError(response)
        else:
            pass

        return response

    def getResource(self, token, keyword):
        '''Read info about a Resource in the system.

        Parameters
        ----------
        keyword : str
            Resource record to retrieve

        Returns
        -------
        value : str
            Resource record value
        '''
        return self.clientRead("resource", keyword)

    def setResource(self, token, keyword, value):
        '''Update info about a Resource in the system.

        Parameters
        ----------
        keyword : str
            Resource record field to be set
        value : str
            Value of field to set

        Returns
        -------
        status : str
            'OK' is record was set
        '''
        return self.clientUpdate("resource", keyword, value)

    def deleteResource(self, token, resource):
        '''Delete a Resource in the system.

        Parameters
        ----------
        token : str
            User identity token.  The token must identify the owner of the
            Resource to be deleted.
        resource : str
            Name of Resource to be deleted. The token must identify the owner
            of the Group to be deleted.

        Returns
        -------
        '''
        query_args = {"resource" : resource,
                      "profile" : self.svc_profile,
                      "debug" : self.debug}

        return self.clientDelete(token, "resource", query_args)




    ###################################################
    #  PRIVATE UTILITY METHODS
    ###################################################

    def debug(self, debug_val):
        '''Set the debug flag.
        '''
        self.debug = debug_val

    def retBoolValue(self, url):
        '''Utility method to call a boolean service at the given URL.
        '''
        try:
            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': self.auth_token}

            r = requests.get(url, headers=headers)
            response = r.text

            if r.status_code != 200:
                raise Exception(r.text)

        except Exception:
            raise dlResError("Invalid user")
        else:
            return response

    def clientRead(self, what, keyword):
        '''Generic method to call a /get service.
        '''
        url = self.svc_url + "/get?what=" + what + "&"

        query_args = {"keyword" : keyword,
                      "profile" : self.svc_profile,
                      "debug" : self.debug}
        try:
            if self.debug:
                print("get" + what + ": url = '" + url + "'")

            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': self.auth_token}

            r = requests.get(url, params=query_args, headers=headers)
            response = r.text

            if r.status_code != 200:
                raise Exception(r.text)

        except Exception:
            raise dlResError(response)
        else:
            pass

        return response

    def clientUpdate(self, what, keyword, value):
        '''Generic method to call a /set service.
        '''
        url = self.svc_url + "/set?what=" + what + "&"

        query_args = {"keyword" : keyword,
                      "value" : value,
                      "profile" : self.svc_profile,
                      "debug" : self.debug}
        try:
            if self.debug:
                print("set" + what + ": url = '" + url + "'")

            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': self.auth_token}

            r = requests.get(url, params=query_args, headers=headers)
            response = r.text

            if r.status_code != 200:
                raise Exception(r.text)

        except Exception:
            raise dlResError(response)
        else:
            pass

        return response

    def clientDelete(self, token, what, query_args):
        '''Generic method to call a /delete service.
        '''
        url = self.svc_url + "/delete?what=" + what + "&"

        try:
            if self.debug:
                print("delete" + what + ": url = '" + url + "'")

            # Add the auth token to the reauest header.
            headers = {'X-DL-AuthToken': token}

            r = requests.get(url, params=query_args, headers=headers)
            response = r.text

            if r.status_code != 200:
                raise Exception(r.text)

        except Exception:
            raise dlResError(response)
        else:
            pass

        return response




# ###################################
#  Resource Management Client Handles
# ###################################

def getClient():
    return resClient()

client = getClient()


# ##########################################
#  Patch the docstrings for module functions
# ##########################################

createUser.__doc__ = client.createUser.__doc__
deleteUser.__doc__ = client.deleteUser.__doc__
getUser.__doc__ = client.getUser.__doc__
setUser.__doc__ = client.setUser.__doc__
passwordReset.__doc__ = client.passwordReset.__doc__
sendPasswordLink.__doc__ = client.sendPasswordLink.__doc__
listFields.__doc__ = client.listFields.__doc__

createGroup.__doc__ = client.createGroup.__doc__
getGroup.__doc__ = client.getGroup.__doc__
setGroup.__doc__ = client.setGroup.__doc__
deleteGroup.__doc__ = client.deleteGroup.__doc__

createResource.__doc__ = client.createResource.__doc__
getResource.__doc__ = client.getResource.__doc__
setResource.__doc__ = client.setResource.__doc__
deleteResource.__doc__ = client.deleteResource.__doc__

set_svc_url.__doc__ = client.set_svc_url.__doc__
get_svc_url.__doc__ = client.get_svc_url.__doc__
set_profile.__doc__ = client.set_profile.__doc__
get_profile.__doc__ = client.get_profile.__doc__
list_profiles.__doc__ = client.list_profiles.__doc__
