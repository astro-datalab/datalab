#!/usr/bin/env python
#
# UTIL -- Utility classes and functions for the Data Lab client interfaces.
#

from __future__ import print_function

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>'
__version__ = '20180321'  # yyyymmdd


""" 
    Utilities for managing the use of Data Lab auth tokens.

Import via

.. code-block:: python

    from dl import Util
    from dl.Util import multifunc, multimethod, def_token
"""

import os

try:
    import ConfigParser                         # Python 2
except ImportError:
    import configparser as ConfigParser         # Python 3


# =========================================================================
#  MULTIMETHOD -- An object class to manage class methods.
#
# Globals
method_registry = {}			# Class-method registry

class MultiMethod(object):
    ''' MultiMethod -- An object class to manage the methods of a class
        such that methods may be overloaded and the appropriate method is
        dispatched depending on the calling arguments.
    '''
    def __init__(self, module, name):
        self.module = module
        self.name = name
        self.methodmap = {}

    def __call__(self, instance, *args, **kw):
        '''  Call the appropriate instance of the class method.  In this
             case, 'self' is a MultiMethod instance, 'instance' is the object
             we want to bind to
        '''
        # Lookup the function to call in the method map.
        reg_name = self.module + '.' + self.name + '.' + str(len(args))
        function = self.methodmap.get(reg_name)
        if function is None:
            raise TypeError("No MultiMethod match found")

        # Call the method instance with all original args/keywords and
        # return the result.
        return function(instance, *args, **kw)

    def register(self, nargs, function, module):
        ''' Register the method based on the number of method arguments.  
            Duplicates are rejected when two method names with the same
            number of arguments are registered.  For generality, we
            construct a registry id from the method name and no. of args.
        '''
        reg_name = module + '.' + function.__name__ + '.' + str(nargs)
        if reg_name in self.methodmap:
            raise TypeError("duplicate registration")
        self.methodmap[reg_name] = function

def multimethod(module, nargs):
    '''  Wrapper function to implement multimethod for class methods.  The
         identifying signature in this case is the number of required
         method parameters.  When methods are called, all original arguments
         and keywords are passed.
    '''
    def register(function):
        function = getattr(function, "__lastreg__", function)
        name = function.__name__
        mm = method_registry.get(name)
        if mm is None:
            mm = method_registry[name] = MultiMethod(module, name)
        mm.register(nargs, function, module)
        mm.__lastreg__ = function

        # return function instead of an object - Python binds this automatically
        def getter(instance, *args, **kwargs):
            return mm(instance, *args, **kwargs)
        return getter
    return register



# =========================================================================
#  MULTIFUNCTION -- An object class to manage module functions.
#
# Globals
func_registry   = {}			# Function registry

class MultiFunction(object):
    ''' MultiFunction -- An object class to manage the module functions
        such that functions may be overloaded and the appropriate functions
        is dispatched depending on the calling arguments.
    '''
    def __init__(self, module, name):
        self.module = module
        self.name = name
        self.funcmap = {}

    def __call__(self, *args, **kw):
        '''  Call the appropriate instance of the function.
        '''
        # Lookup the function to call in the method map.
        reg_name = self.module + '.' + self.name + '.' + str(len(args))
        function = self.funcmap.get(reg_name)
        if function is None:
            raise TypeError("No MultiFunction match found")

        # Call the function with all original args/keywords and return result.
        return function(*args, **kw)

    def register(self, nargs, function, module):
        ''' Register the method based on the number of method arguments.  
            Duplicates are rejected when two method names with the same
            number of arguments are registered.  For generality, we
            construct a registry id from the method name and no. of args.
        '''
        reg_name = module + '.' + function.__name__ + '.' + str(nargs)
        if reg_name in self.funcmap:
            raise TypeError("duplicate registration")
        self.funcmap[reg_name] = function

def multifunc(module, nargs):
    '''  Wrapper function to implement multimethod for functions.  The
         identifying signature in this case is the number of required
         method parameters.  When methods are called, all original arguments
         and keywords are passed.
    '''
    def register(function):
        function = getattr(function, "__lastreg__", function)
        name = function.__name__
        mf = func_registry.get(name)
        if mf is None:
            mf = func_registry[name] = MultiFunction(module, name)
        mf.register(nargs, function, module)
        mf.__lastreg__ = function
        return mf
    return register



# =========================================================================
# Globals
ANON_TOKEN	= 'anonymous.0.0.anon_access'
TOK_DEBUG	= True

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
            # No token supplied, not logged-in, check for a logged-in user token.
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


