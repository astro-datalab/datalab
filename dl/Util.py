#!/usr/bin/env python
#
# UTIL -- Utility classes and functions for the Data Lab client interfaces.
#

from __future__ import print_function

__authors__ = 'Mike Fitzpatrick <mike.fitzpatrick@noirlab.edu>'
__version__ = '20190422'  # yyyymmdd


"""
    Utilities for managing the use of Data Lab auth tokens.

Import via

.. code-block:: python

    from dl import Util
    from dl.Util import multimethod, def_token
"""

import os
import mimetypes
import random
import string
import re
from functools import partial
from urllib.parse import urlencode          # Python 3

try:
    import ConfigParser                         # Python 2
except ImportError:
    import configparser as ConfigParser         # Python 3


# =========================================================================
#  MULTIMETHOD -- An object class to manage class methods.
#
# Globals
method_registry = {}			# Class-method registry

def add_doc(value):
    '''Decorator to set the 'Call docstring' ipython field.
    '''
    def _doc(func):
        func.__doc__ = value
        return func
    return _doc


class MultiMethod(object):
    '''MultiMethod -- An object class to manage the module functions
       such that functions may be overloaded and the appropriate functions
       is dispatched depending on the calling arguments.
    '''
    def __init__(self, module, name, cm, func):
        self.module = module
        self.name = name
        self.func = func
        self.cm = cm
        self.obj = None
        self.nargs = None
        self.methodmap = {}

    def __call__(self, *args, **kw):
        '''Call the appropriate instance of the function.
        '''
        # DEBUG - Above docstring roduces the 'Call Docstring' in ipython '??'

        # Lookup the function to call in the method map.
        if self.cm:
            reg_name = self.module + '.' + self.name + '.' + str(len(args)-1)
        else:
            reg_name = self.module + '.' + self.name + '.' + str(len(args))
        function = self.methodmap.get(reg_name)
        if function is None:
            raise TypeError("No MultiFunction match found for " + reg_name)

        # Call the function with all original args/keywords and return result.
        if self.cm:
            return function(self.obj, *args, **kw)
        else:
            return function(*args, **kw)

    def __repr__(self):
        return self.func.__repr__()

    def __get__(self, obj, objtype):
        self.obj = obj
        f = partial(self.__call__, obj)
        f.__doc__ = self.func.__doc__
        f.__dict__ = self.func.__dict__
        f.__module__ = self.func.__module__
        f.__defaults__ = self.func.__defaults__
        if self.cm:
            f.__dir__ = dir(self.obj)
        #return partial(self.__call__, obj)
        return f

    __doc__ = property(lambda self:self.func.__doc__)
    __annotations__ = property(lambda self:self.func.__annotations__)
    __name__ = property(lambda self:self.func.__name__)
    __module__ = property(lambda self:self.func.__module__)

    def getdoc(self):
        # DEBUG - Produces the 'Docstring' value in ipython '??'
        return self.func.__doc__

    def register(self, nargs, function, module):
        '''Register the method based on the number of method arguments.
           Duplicates are rejected when two method names with the same
           number of arguments are registered.  For generality, we
           construct a registry id from the method name and no. of args.
        '''
        reg_name = module + '.' + function.__name__ + '.' + str(nargs)
        if reg_name in self.methodmap:
            raise TypeError("duplicate registration")
        self.methodmap[reg_name] = function
        self.func = function
        self.nargs = nargs


def multimethod(module, nargs, cm):
    '''Wrapper function to implement multimethod for functions.  The
       identifying signature in this case is the number of required
       method parameters.  When methods are called, all original arguments
       and keywords are passed.
    '''

    def register(function):
        '''multimethod register()
        '''
        function = getattr(function, "__lastreg__", function)
        name = function.__name__
        mf = registry.get(name)
        if mf is None:
            mf = registry[name] = MultiMethod(module, name, cm, function)
        mf.register(nargs, function, module)
        mf.__lastreg__ = function

        if not cm or nargs > 0:
            return mf
        else:
            mf.__call__ = classmethod(function)
            return mf.__lastreg__

    if module not in method_registry.keys():
        method_registry[module] = {}
    registry = method_registry[module]

    return register



# =========================================================================
# Globals
ANON_TOKEN	= 'anonymous.0.0.anon_access'
TOK_DEBUG	= False

#  READTOKENFILE -- Read the contents of the named token file.  If it
#  doesn't exist, default to the anonymous token.
#
def readTokenFile (tok_file):
    if TOK_DEBUG: print ('readTokenFile: ' + tok_file)
    if not os.path.exists(tok_file):
        if TOK_DEBUG: print ('returning ANON_TOKEN')
        return ANON_TOKEN 			# FIXME -- print a warning?
    else:
        tok_fd = open(tok_file, "r")
        user_tok = tok_fd.read(128).strip('\n') # read the old token
        tok_fd.close()
        if TOK_DEBUG: print ('returning user_tok: ' + user_tok)
        return user_tok				# return named user tok


#  DEF_TOKEN -- Utility method to get the default user token to be passed
#  by a Data Lab client call.
#
def def_token(tok):
    ''' Get a default token.  If no token is provided, check for an
        existing $HOME/.datalab/id_token.<user> file and return that if
        it exists, otherwise default to the ANON_TOKEN.

        If a token string is provided, return it directly.  The value
        may also simply be a username, in which case the same check for
        a token ID file is done.
    '''

    def isUserLoggedIn (user):
        '''Utility to check with AuthMgr whether user is logged-in.
        '''
        DEF_AUTH_SVC = 'https://datalab.noirlab.edu/auth'
        svc_url = svcOverride('AM_SVC_URL', DEF_AUTH_SVC)
        url = svc_url + "/isUserLoggedIn?"
        args = urlencode({"user": user, "profile": self.svc_profile})
        url = url + args
        print("isUserLoggedIn: url = '%s'" % url)

        try:
            r = requests.get(url)
            response = acToString(r.content)
            if r.status_code != 200:
                raise Exception(r.content)
            val = 'true' in str(r.text.lower())
        except Exception:
            val = False
        else:
            return val


    home = '%s/.datalab' % os.path.expanduser('~')
    if tok is None or tok == '':

        # Read the $HOME/.datalab/dl.conf file
        config = ConfigParser.RawConfigParser(allow_no_value=True)
        if os.path.exists('%s/dl.conf' % home):
            config.read('%s/dl.conf' % home)
            _status = config.get('login','status')
            if _status == 'loggedin':
                # Return the currently logged-in user.
                _user = config.get('login','user')
                tok_file = ('%s/id_token.%s' % (home, _user))
                if TOK_DEBUG: print ('returning loggedin user: %s' % tok_file)
                return readTokenFile(tok_file)
            else:
                # Nobody logged in so return 'anonymous'
                if TOK_DEBUG: print ('returning ANON_TOKEN')
                return ANON_TOKEN

        else:
            # No token supplied, not logged-in, check for a login user token.
            tok_file = ('%s/id_token.%s' % (home, os.getlogin()))
            if TOK_DEBUG: print ('tok_file: %s' % tok_file)
            if not os.path.exists(home) or not os.path.exists(tok_file):
                if TOK_DEBUG: print ('returning ANON_TOKEN')
                return ANON_TOKEN
            else:
                return readTokenFile(tok_file)
    else:
        # Check for a plain user name or valid token.  If we're given a
        # token just return it.  If it may be a user name, look for a token
        # id file and return that, otherwise we're just anonymous.
        if is_auth_token(tok):                # is it a token?
            if TOK_DEBUG: print ('returning input tok:  ' + tok)
            return tok
        elif len(tok.split('.')) == 1:			# user name maybe?
            tok_file = ('%s/id_token.%s' % (home, tok))
            return readTokenFile(tok_file)
        else:
            if TOK_DEBUG: print ('returning ANON_TOKEN')
            return ANON_TOKEN


def parse_auth_token(token):
    """Parses string argument token
    Usage:
        parse_auth_token(token)

    Parameters
    ----------
    token : str
        A string auth token
        E.g.
        "testuser.3666.3666.$1$PKCFmMzy$OPpZg/ThBmZe/V8LVPvpi/"

    Returns
    -------
    return: a regex Match object or None
    """
    r"""
    Explanation of the Regular Expression used:
    E.g. token "testuser.3666.3666.$1$PKCFmMzy$OPpZg/ThBmZe/V8LVPvpi/%"
    Regex deconstruction and explanation:
    -------------------------------------
    1.   ([^\/\s]+)       any string with no "/" or spaces
    2.   \.               separated by a .
    3.   (\d+)            followed by any number of digits, user id
    4.   \.               separated by a .
    5.   (\d+)            followed by any number of digits, group id
    6.   \.               separated by a .
    7.a) (?:\$1\$\S{22,}) (Non capturing group) A string that starts with 
                          $1$ (that's how a md5 hash starts)
                          and that is followed by any non space characters
                          of 22 chars or longer
    7.b) |                or
    7.c) (?:\S+_access)   (Non capturing group) A string that ends in _access.
                          This is a special case for special tokens such as:
                          anonymous.0.0.anon_access
                          dldemo.99999.99999.demo_access
    """

    return re.match(r'([^\/\s]+)\.(\d+)\.(\d+)\.((?:\$1\$\S{22,})|(?:\S+_access))', token)


def split_auth_token(token):
    """ Given an auth token split it in its components
    Usage:
        split_auth_token(token)

    Parameters
    ----------
    token : str
        A string auth token
        E.g.
        "testuser.3666.3666.$1$PKCFmMzy$OPpZg/ThBmZe/V8LVPvpi/"

    Returns
    -------
    return: [username, user_id, group_id, hash]
            or None if not a token
         E.g.
         ["testuser", "3666", "3666" , "$1$PKCFmMzy$OPpZg/ThBmZe/V8LVPvpi/"]
    """
    res = parse_auth_token(token)
    return res.groups() if res else None


def auth_token_to_dict(token):
    """
    Given an auth token splits it in its components
    and returns a dictionary.
    Parameters
    ----------
    token : str
        A string auth token
        E.g.
        "testuser.3666.3666.$1$PKCFmMzy$OPpZg/ThBmZe/V8LVPvpi/"

    Returns
    -------
    return:
           { 'username': "username value",
              'uid': "numeric user id",
              'gid': "numeric group id",
              'hash': hash
            }
    E.g.   { 'username': "testuser",
              'uid': "3666",
              'gid': "3666",
              'hash': "$1$PKCFmMzy$OPpZg/ThBmZe/V8LVPvpi/"
            }
            or None if token not valid
    """
    res = split_auth_token(token)
    if res is not None:
        return {k: v for k, v in zip(['username', 'uid', 'gid', 'hash'], res)}
    else:
        return None



# --------------------------------------------------------------------
# IS_AUTH_TOKEN -- returns True if the the string pass is a token
#                  False otherwise
#
def is_auth_token(token):
    """Check if passed in string is an auth token
    Usage:
        is_auth_token(token)

    Parameters
    ----------
    token : str
        A string auth token
        E.g.
        "testuser.3666.3666.$1$PKCFmMzy$OPpZg/ThBmZe/V8LVPvpi/"

    Returns
    -------
    return: boolean
         True if string is a auth token
    """
    return True if parse_auth_token(token) else False



# --------------------------------------------------------------------
# VALIDTABLENAME -- Validate a DB table name contains only allowed chars.
#

def validTableName(tbl):
    '''Return True if named table contains only valid lower-case chars or
       underscores.  A '.' in the string assumes the presence of a schema
       in the name, the schema and table name will be validated separately
       however both must be valid.
    '''

    def hasCaps(nm):
        '''Return True if nm contains capital letters.'''
        return bool(re.search(r'[A-Z]',nm))

    def beginsWithNumber(nm):
        '''Return True if nm begins with a number.'''
        return bool(re.search(r'[0-9]',nm[0]))

    def validCharsOnly(nm):
        '''Return True if all chars in nm are allowed values.'''
        for e in list(nm):
            if not re.search(r'[a-z0-9_]',e):
                return False
        return True

    def validName(nm):
        if not validCharsOnly(nm):
            return False
        else:
            return bool(validCharsOnly(nm) and \
                        not (hasCaps(nm) or beginsWithNumber(nm)))

    if tbl in [None,'','mydb://']:
        return True
    if tbl.startswith('mydb://'):
        tbl = tbl[7:]
    if '.' in tbl:
        if len(tbl.split('.')) != 2:                    # e.g. 'mydb.foo.bar'
            return False
        _schema, _tbl = tbl.split('.')                  # assumes schema name
        if _schema in [None,''] or _tbl in [None,'']:   # e.g. ".foo" or "foo."
            return False
        return (validName(_schema) and validName(_tbl))
    else:
        return validName(tbl)


# --------------------------------------------------------------------
# SVCOVERRIDE -- Check for a service URL override.
#

def svcOverride(what, default):
    '''Check for an override of a (usually, service) URL as deined by either
       and environment variable, or a /tmp file given by the 'what' string.
       If neither is found, returns the default value.
    '''
    if what is None:
        return default

    env_val = os.getenv (what)
    if env_val not in [None, '']:
        return env_val
    else:
        tmp_path = '/tmp/%s' % what
        if os.path.exists(tmp_path):
            with open(tmp_path) as fd:
                return fd.read().strip()
        else:
            return default


# --------------------------------------------------------------------
# ENCODE_MULTIPART -- Encode multipart form data to upload files via POST.
#

_BOUNDARY_CHARS = string.digits + string.ascii_letters

def encode_multipart(fields, files, boundary=None):
    """ Encode dict of form fields and dict of files as multipart/form-data.
        Return tuple of (body_string, headers_dict). Each value in files is
        a dict with required keys 'filename' and 'content', and optional
        'mimetype' (if not specified, tries to guess mime type or uses
        'application/octet-stream').
        
        ..code-block:: python

            >>> body, headers = encode_multipart({'FIELD': 'VALUE'},
            ...                                  {'FILE': {'filename': 'F.TXT', 'content': 'CONTENT'}},
            ...                                  boundary='BOUNDARY')
            >>> print('\\n'.join(repr(l) for l in body.split('\\r\\n')))
            '--BOUNDARY'
            'Content-Disposition: form-data; name="FIELD"'
            ''
            'VALUE'
            '--BOUNDARY'
            'Content-Disposition: form-data; name="FILE"; filename="F.TXT"'
            'Content-Type: text/plain'
            ''
            'CONTENT'
            '--BOUNDARY--'
            ''
            >>> print(sorted(headers.items()))
            [('Content-Length', '193'), ('Content-Type', 'multipart/form-data; boundary=BOUNDARY')]
            >>> len(body)
            193
    """
    def escape_quote(s):
        return s.replace('"', '\\"')

    if boundary is None:
        boundary = ''.join(random.choice(_BOUNDARY_CHARS) for i in range(30))
    lines = []

    for name, value in fields.items():
        lines.extend((
            '--{0}'.format(boundary),
            'Content-Disposition: form-data; name="{0}"'.format(escape_quote(name)),
            '',
            str(value),
        ))

    for name, value in files.items():
        filename = value['filename']
        if 'mimetype' in value:
            mimetype = value['mimetype']
        else:
            mimetype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        lines.extend((
            '--{0}'.format(boundary),
            'Content-Disposition: form-data; name="{0}"; filename="{1}"'.format(
                    escape_quote(name), escape_quote(filename)),
            'Content-Type: {0}'.format(mimetype),
            '',
            value['content'],
        ))

    lines.extend((
        '--{0}--'.format(boundary),
        '',
    ))
    body = '\r\n'.join(lines)

    headers = {
        'Content-Type': 'multipart/form-data; boundary={0}'.format(boundary),
        'Content-Length': str(len(body)),
    }

    return (body, headers)


'''
Example:

import urllib2

import formdata

fields = {'name': 'BOB SMITH'}
files = {'file': {'filename': 'F.DAT', 'content': 'DATA HERE'}}
data, headers = formdata.encode_multipart(fields, files)
request = urllib2.Request('http://httpbin.org/post', data=data, headers=headers)
f = urllib2.urlopen(request)
print f.read()
'''
