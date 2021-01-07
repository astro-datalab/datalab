#!/usr/bin/env python
#
# RESCLIENT -- Client methods for the Data Lab Resource Management Service
#

'''Client methods for the Data Lab Resource Management Service.

    Resource Manager
    ----------------
            createUser  (username, password, email, name, institute)
               getUser  (token, username, keyword)
               setUser  (token, username, keyword, value)
            deleteUser  (token, username)
         passwordReset  (token, user, password)
      sendPasswordLink  (token, user)
            listFields  ()

           createGroup  (token, group)
              getGroup  (token, group, keyword)
              setGroup  (token, group, keyword, value)
           deleteGroup  (token, group)

        createResource  (token, resource)
           getResource  (token, resource, keyword)
           setResource  (token, resource, keyword, value)
        deleteResource  (token, resource)

             createJob  (token, jobid, job_type, query=None, task=None)
                getJob  (token, jobid, keyword)
                setJob  (token, jobid, keyword, value)
             deleteJob  (token, jobid)
              findJobs  (token, jobid, format='text', status='all',
                         option='list')

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
__version__ = 'v2.18.7'


import requests
import os
import json


# The URL of the ResManager service to contact.  This may be changed by
# passing a new URL into the set_svc_url() method before beginning.

DEF_SERVICE_URL = "https://datalab.noao.edu/res"

# The requested service "profile".  A profile refers to the specific
# machines and services used by the ResManager on the server.

DEF_SERVICE_PROFILE = "default"


# API debug flag.
DEBUG = False


keys = {'user':None,
        'group':'group',
        'resource':'resource',
        'job':'jobid'}


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

def createUser(username, password, email, name, institute, profile='default'):
    try:
        resp = client.createUser(username, password, email, name, institute,
                                 profile=profile)
    except dlResError as e:
        resp = str(e)

    return resp

def deleteUser(token, username, profile='default'):
    return client.deleteUser(token, username, profile=profile)

def getUser(token, username, keyword, profile='default'):
    return client.getUser(token, username, keyword, profile=profile)

def setUser(token, username, keyword, value, profile='default'):
    return client.setUser(token, username, keyword, value, profile=profile)

def passwordReset(token, user, password, profile='default'):
    return client.passwordReset(token, user, password, profile=profile)

def sendPasswordLink(token, user, profile='default'):
    return client.sendPasswordLink(token, user, profile=profile)

def listFields(profile='default'):
    return client.listFields(profile=profile)


# Group functions
def createGroup(token, group, profile='default'):
    return client.createGroup(token, group, profile=profile)

def getGroup(token, group, keyword, profile='default'):
    return client.getGroup(token, group, keyword, profile=profile)

def setGroup(token, group, keyword, value, profile='default'):
    return client.setGroup(token, group, keyword, value, profile=profile)

def deleteGroup(token, group, profile='default'):
    return client.deleeteGroup(token, group, profile=profile)


# Resource functions
def createResource(token, resource, profile='default'):
    return client.createResource(token, resource, profile=profile)

def getResource(token, resource, keyword, profile='default'):
    return client.getResource(token, resource, keyword, profile=profile)

def setResource(token, resource, keyword, value, profile='default'):
    return client.setResource(token, resource, keyword, value, profile=profile)

def deleteResource(token, resource, profile='default'):
    return client.deleteResource(token, resource, profile=profile)


# Job functions
def createJob(token, jobid, job_type, query=None, task=None, profile='default'):
    return client.createJob(token, jobid, job_type, query=query, task=task,
                            profile=profile)

def getJob(token, jobid, keyword, profile='default'):
    return client.getJob(token, jobid, keyword, profile=profile)

def setJob(token, jobid, keyword, value, profile='default'):
    return client.setJob(token, jobid, keyword, value, profile=profile)

def deleteJob(token, jobid, profile='default'):
    return client.deleteJob(token, jobid, profile=profile)

def findJobs(token, jobid, format='text', status='all', option='list'):
    return client.findJobs(token, jobid, format=format, status=status,
                           option=option)


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

    def createUser(self, username, password, email, name, institute,
                   profile='default'):
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
                      "profile" : (profile if profile != 'default' else self.svc_profile),
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

    def getUser(self, username, keyword, profile='default'):
        '''Read info about a user in the system.

        Parameters
        ----------
        username : str
            User name
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
        return self.clientRead(token, "user", username, keyword,
                               profile=profile)

    def setUser(self, username, keyword, value, profile='default'):
        '''Update info about a user in the system.

        Parameters
        ----------
        username : str
            User name
        keyword : str
            User record field to be set
        value : str
            Value of field to set

        Returns
        -------
        Service response
        '''
        return self.clientUpdate(token, "user", username, keyword,
                                 profile=profile)

    def deleteUser(self, token, username, profile='default'):
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
                      "profile" : (profile if profile != 'default' else self.svc_profile),
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

            # Add the auth token to the request header.
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


    def passwordReset(self, token, user, password, profile='default'):
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
        url = self.svc_url + ("/pwReset?user=%s&password=%s&profile=%s" % (user,password,profile))

        try:
            resp = self.svcGet(token, url)
        except Exception as e:
            raise Exception(str(e))
        else:
            # Service call was successful.
            print("passwordReset:  success, removing local token file")
            tok_file = ('%s/id_token.%s' % (self.home, user))
            if os.path.exists(tok_file):
                os.remove(tok_file)


    def sendPasswordLink(self, token, user, profile='default'):
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
        url = self.svc_url + ("/pwResetLink?user=%s&profile=%s" % (user,profile))

        try:
            # Add the auth token to the request header.
            headers = {'X-DL-AuthToken': token}
            r = requests.get(url, headers=headers)

            if r.status_code == 200:
                return "OK"
            else:
                raise Exception(r.text)

        except Exception as e:
            raise Exception(str(e))


    def listFields(self, profile='default'):
        '''List available user fields.

        Parameters
        ----------
        None

        Returns
        -------
        Service response
        '''
        url = self.svc_url + ("/listFields?profile=%s" % profile)

        try:
            return self.svcGet(self.auth_token, url)
        except Exception as e:
            raise Exception(str(e))

        pass

    def approveUser(self, token, user, profile='default'):
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
        url = self.svc_url + "/approveUser?approve=True&user=%s&profile=%s" % (user,profile)

        try:
            return self.svcGet(token, url)
        except Exception as e:
            raise Exception(str(e))


    def disapproveUser(self, token, user, profile='default'):
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
        url = self.svc_url + "/approveUser?approve=False&user=%s&profile=%s" % (user,profile)

        try:
            return self.svcGet(token, url)
        except Exception as e:
            raise Exception(str(e))


    def userRecord(self, token, user, value, format, profile='default'):
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
                ("/userRecord?user=%s&value=%s&fmt=%s&profile=%s" % (user,value,format,profile))

        try:
            resp = self.svcGet(token, url)
        except Exception as e:
            raise Exception(str(e))
        else:
            return resp

        return "OK"


    def listPending(self, token, verbose=False, profile='default'):
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
        url = self.svc_url + "/pending?verbose=%s&profile=%s" % (str(verbose),profile)

        try:
            resp = self.svcGet(token, url)
        except Exception as e:
            raise Exception(str(e))
        else:
            return resp

        return "OK"


    def setField(self, token, user, field, value, profile='default'):
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
        url = self.svc_url + "/setField?user=%s&field=%s&value=%s&profile=%s" % (user, field, value, profile)

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

    def createGroup(self, token, group, profile='default'):
        '''Create a new Group in the system.

        Parameters
        ----------
        token : str
            User identity token
        group : str
            Group name to create

        Returns
        -------
        resp : str
            Service response
        '''
        url = self.svc_url + "/create?what=group&"

        query_args = {"group" : group,
                      "profile" : (profile if profile != 'default' else self.svc_profile),
                      "debug" : self.debug}
        try:
            if self.debug:
                print("createGroup: " + group)

            # Add the auth token to the request header.
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

    def getGroup(self, token, group, keyword, profile='default'):
        '''Read info about a Group in the system.

        Parameters
        ----------
        token : str
            User identity token
        group : str
            Group name to create
        keyword : str
            Group record field to retrieve

        Returns
        -------
        value : str
            Value of record field
        '''
        return self.clientRead(token, "group", group, keyword, profile=profile)

    def setGroup(self, token, group, keyword, value, profile='default'):
        '''Update info about a Group in the system.

        Parameters
        ----------
        token : str
            User identity token
        group : str
            Group name to create
        keyword : str
            Group record field to be set
        value : str
            Value of field to set

        Returns
        -------
        '''
        return self.clientUpdate(token, "group", group, keyword, value,
                                 profile=profile)

    def deleteGroup(self, token, group, profile='default'):
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
                      "profile" : (profile if profile != 'default' else self.svc_profile),
                      "debug" : self.debug}

        return self.clientDelete(token, "group", query_args, profile=profile)




    ###################################################
    #  RESOURCE MANAGEMENT
    ###################################################

    def createResource(self, token, resource, profile='default'):
        '''Create a new Resource in the system.

        Parameters
        ----------
        token : str
            User identity token
        resource : str
            Resource URI to create

        Returns
        -------
        resp : str
            Service response
        '''
        url = self.svc_url + "/create?what=resource&"

        query_args = {"resource" : resource,
                      "profile" : (profile if profile != 'default' else self.svc_profile),
                      "debug" : self.debug}
        try:
            if self.debug:
                print("createResource: " + resource)

            # Add the auth token to the request header.
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

    def getResource(self, token, resource, keyword, profile='default'):
        '''Read info about a Resource in the system.

        Parameters
        ----------
        token : str
            User identity token
        resource : str
            Resource URI to create
        keyword : str
            Resource record to retrieve

        Returns
        -------
        value : str
            Resource record value
        '''
        return self.clientRead(token, "resource", resource, keyword, profile=profile)

    def setResource(self, token, resource, keyword, value, profile='default'):
        '''Update info about a Resource in the system.

        Parameters
        ----------
        token : str
            User identity token
        resource : str
            Resource URI to create
        keyword : str
            Resource record field to be set
        value : str
            Value of field to set

        Returns
        -------
        status : str
            'OK' if record was set
        '''
        return self.clientUpdate(token, "resource", resource, keyword, value,
                                 profile=profile)

    def deleteResource(self, token, resource, profile='default'):
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
                      "profile" : (profile if profile != 'default' else self.svc_profile),
                      "debug" : self.debug}

        return self.clientDelete(token, "resource", query_args)



    ###################################################
    #  JOB MANAGEMENT
    ###################################################

    def createJob(self, token, jobid, job_type, query=None, task=None,
                  profile='default'):
        '''Create a new Job record in the system.

        Parameters
        ----------
        token : str
            User identity token
        jobid : str
            Job ID to create
        type : str
            Type of job:  currently only 'query' or 'compute'
        query : str
            If 'type' is 'query', the SQL/ADQL query string
        task : str
            If 'type' is 'compute', the name of the task being run

        Returns
        -------
        resp : str
            Service response
        '''
        url = self.svc_url + "/create?what=job&"

        query_args = {"jobid" : jobid,
                      "type" : job_type,
                      "query" : query,
                      "task" : task,
                      "profile" : (profile if profile != 'default' else self.svc_profile),
                      "debug" : self.debug}
        try:
            if self.debug:
                print("createJob: " + jobid)

            # Add the auth token to the request header.
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

    def getJob(self, token, jobid, keyword, profile='default'):
        '''Read info about a Job in the system.

        Parameters
        ----------
        token : str
            User identity token
        jobid : str
            Job ID to create
        keyword : str
            Job record to retrieve

        Returns
        -------
        value : str
            Job record value
        '''
        return self.clientRead(token, "job", jobid, keyword, profile=profile)


    def setJob(self, token, jobid, keyword, value, profile='default'):
        '''Update info about a Job in the system.

        Parameters
        ----------
        token : str
            User identity token
        jobid : str
            Job ID to create
        keyword : str
            Job record field to be set
        value : str
            Value of field to set

        Returns
        -------
        status : str
            'OK' if record was set
        '''
        return self.clientUpdate(token, "job", jobid, keyword, value,
                                 profile=profile)

    def deleteJob(self, token, jobid, profile='default'):
        '''Delete a Job in the system.

        Parameters
        ----------
        token : str
            User identity token.  The token must identify the owner of the
            Job to be deleted.
        jobid : str
            ID of Job to be deleted. The token must identify the owner
            of the Job to be deleted.

        Returns
        -------
        '''
        query_args = {"jobid" : jobid,
                      "profile" : (profile if profile != 'default' else self.svc_profile),
                      "debug" : self.debug}

        return self.clientDelete(token, "job", query_args)


    def findJobs(self, token, jobid, format='text', status='all',
                 option='list'):
        '''Find job records.  If jobid is None or '*', all records for the user
           identified by the token are returned, otherwise the specific job
           record is returned.

        Parameters
        ----------
        token : str
            User identity token.  The token must identify the owner of the
            Job to be deleted.
        jobid : str
            Job ID to match.
        format : str
            Output format: 'text' or 'json'
        status : str
            Job phase status to match.  Default to 'all' but may be one of
            EXECUTING, COMPLETED, ERROR, ABORT or WAITING.
        option : str
            Processing option:  'list' will return a listing of the matching
            records in the format specified by 'format'; 'delete' will delete
            all matching records from the server except for EXECUTING jobs.

        Returns
        -------
            A JSON string of Job records matching the user or jobid. 
        '''
        url = self.svc_url + "/findJobs"

        query_args = {"jobid" : jobid,
                      "format" : format,
                      "status" : status,
                      "option" : option,
                      "profile" : self.svc_profile,
                      "debug" : self.debug}
        try:
            # Add the auth token to the request header.
            headers = {'X-DL-AuthToken': token}

            r = requests.get(url, params=query_args, headers=headers)
            response = str(r.text)

            if r.status_code != 200:
                raise Exception(r.text)

        except Exception:
            raise dlResError(response)

        if format == 'json':
            try:
                jstr_ = response.replace("'",'"')[1:-1]
                jstr = json.loads(jstr_)
            except Exception as e:
                raise dlResError(str(e))
            return jstr
        else:
            return response



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
            # Add the auth token to the request header.
            headers = {'X-DL-AuthToken': token}

            r = requests.get(url, headers=headers)
            response = r.text

            if r.status_code != 200:
                raise Exception(r.text)

        except Exception:
            raise dlResError("Invalid user")
        else:
            return response

    def clientRead(self, token, what, key, keyword, profile='default'):
        '''Generic method to call a /get service.
        '''
        url = self.svc_url + "/get?what=" + what #+ "&"

        _key = keys[what]
        query_args = {_key : key,
                      "keyword" : keyword,
                      "profile" : self.svc_profile,
                      "debug" : self.debug}
        try:
            if self.debug:
                print("get" + what + ": url = '" + url + "'")

            # Add the auth token to the request header.
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

    def clientUpdate(self, token, what, key, keyword, value, profile='default'):
        '''Generic method to call a /set service.
        '''
        url = self.svc_url + "/set?what=" + what #+ "&"

        _key = keys[what]
        query_args = {_key : key,
                      "keyword" : keyword,
                      "value" : value,
                      "profile" : self.svc_profile,
                      "debug" : self.debug}
        try:
            if self.debug:
                print("set" + what + ": url = '" + url + "'")

            # Add the auth token to the request header.
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

    def clientDelete(self, token, what, query_args):
        '''Generic method to call a /delete service.
        '''
        url = self.svc_url + "/delete?what=" + what + "&"

        try:
            if self.debug:
                print("delete" + what + ": url = '" + url + "'")

            # Add the auth token to the request header.
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
