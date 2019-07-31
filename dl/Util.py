#!/usr/bin/env python
#
# UTIL -- Utility classes and functions for the Data Lab client interfaces.
#

from __future__ import print_function

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>'
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
from functools import partial

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

        return mf
        #if cm or nargs > 0:
        if nargs > 0:
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
        # Check for a plane user name or valid token.  If we're given a
        # token just return it.  If it may be a user name, look for a token
        # id file and return that, otherwise we're just anonymous.
        if len(tok.split('.')) >= 4:			# looks like a token
            if TOK_DEBUG: print ('returning input tok:  ' + tok)
            return tok
        elif len(tok.split('.')) == 1:			# user name maybe?
            tok_file = ('%s/id_token.%s' % (home, tok))
            return readTokenFile(tok_file)
        else:
            if TOK_DEBUG: print ('returning ANON_TOKEN')
            return ANON_TOKEN



"""Encode multipart form data to upload files via POST."""

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
