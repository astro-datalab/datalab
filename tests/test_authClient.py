#!/usr/bin/env python
# D. Nidever
#
# test_authClient.py
# Python code to (unit) test the DL Client tools

from dl import authClient, storeClient, queryClient
import os
import unittest
try:
    from urllib import urlencode		        # Python 2
    from urllib2 import urlopen, Request                # Python 2
except ImportError:
    from urllib.parse import urlencode		        # Python 3
    from urllib.request import urlopen, Request         # Python 3
import requests

# Service URLs
AM_URL = "https://dlsvcs.datalab.noirlab.edu/auth"      # Auth Manager
SM_URL = "https://dlsvcs.datalab.noirlab.edu/storage"   # Storage Manager
QM_URL = "https://dlsvcs.datalab.noirlab.edu/query"     # Query Manager
# Test token
TEST_TOKEN = "dltest.99998.99998.test_access"


def isValidTokenStructure(token):
    try:
        user, uid, gid, hash = token.strip().split('.', 3)
    except Exception as e:
        return False
    else:
      return True

def isValidTokenCall(token):
  try:
    url = AM_URL + "/isValidToken?"
    args = urlencode({"token": token, "profile": "default"})
    url = url + args
    headers = {'X-DL-AuthToken': token}
    r = requests.get(url, headers=headers)
    response = r.text
  except Exception as e:
    return False
  return response

def login(username,password):
  url = AM_URL + "/login?"
  query_args = {"username": username, "password": password,
                "profile": "default", "debug": False}
  try:
    r = requests.get(url, params=query_args)
    response = r.text
  except:
    response = 'None'
  return response
    
def logout(token):
  url = AM_URL + "/logout?"
  args = urlencode({"token": token, "debug": False})
  url = url + args
  headers = {'X-DL-AuthToken': token}
  r = requests.get(url, params=args, headers=headers)
  response = r.text
  return response
  
def deleteTokenFile(token):
  home = '%s/.datalab' % os.path.expanduser('~')
  username, uid, gid, hash = token.strip().split('.', 3)
  tok_file = home + '/id_token.' + username
  if os.path.exists(tok_file):
    os.remove(tok_file)
            
def suite():
  suite = unittest.TestSuite()
  # authClient: login, logout, isValidUser, isValidToken
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestLoginDatalab))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestLogin))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestLogout))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestValidUser))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestValidToken))
  return suite

class TestLoginDatalab(unittest.TestCase):
      
    def test_loginvalid(self):
        token = authClient.login('datalabtest','DataLabTest1')
        self.assertTrue(isValidTokenStructure(token))
        self.assertTrue(isValidTokenCall(token))
        user, uid, gid, hash = token.strip().split('.', 3)
        self.assertEqual(user,'datalabtest')
        self.assertEqual(len(token),56)
        res = logout(token)
        
class TestLogin(unittest.TestCase):
  
    def test_logindltest(self):
        token = authClient.login('dltest','datalab')
        self.assertEqual(token,TEST_TOKEN)
      
    def test_loginnotvalid1(self):
        token = authClient.login('temptemptemp','blah')
        self.assertEqual(token,'Error: Username "temptemptemp" does not exist.')
      
    def test_loginnotvalid2(self):
        token = authClient.login('datalabtest','blah')
        self.assertEqual(token,'Error: Invalid password')

class TestLogout(unittest.TestCase):
  
    def test_logoutvalid(self):
        token = login('datalabtest','DataLabTest1')
        res = authClient.logout(token)
        self.assertEqual(res,'OK')

    def test_logoutinvalid1(self):
        token = 'datalabtest.1148.1148.abcdefghijklmnopqrstuvwxyz12345678'
        res = authClient.logout(token)
        self.assertNotEqual(res,'')
        self.assertIn('500 Internal Server Error',res)

    def test_logoutinvalid2(self):
        token = 'datalabtest.1148.1148.abcdefghijklmnopqrstuvwxyz12345678910111213'
        res = authClient.logout(token)
        self.assertNotEqual(res,'')
        self.assertIn('500 Internal Server Error',res)

    def test_logoutinvalid3(self):
        token = 'abcdefghijklmnopqrstuvwxyz12345678910111213'
        res = authClient.logout(token)
        self.assertNotEqual(res,'')
        self.assertIn('500 Internal Server Error',res)

class TestValidUser(unittest.TestCase):

    def test_validusertrue(self):
        res = authClient.isValidUser('datalabtest')
        self.assertTrue(res)

    def test_validuserfalse(self):
        res = authClient.isValidUser('temptemptemp')
        self.assertEqual(res,'False')

class TestValidToken(unittest.TestCase):
        
    def test_validtoken(self):
        token = login('datalabtest','DataLabTest1')
        res = authClient.isValidToken(token)
        self.assertTrue(res)
        res = logout(token)
        res2 = deleteTokenFile(token)
        
    def test_invalidtoken1(self):
        token = 'datalabtest.1148.1148.abcdefghijklmnopqrstuvwxyz12345678'
        res = authClient.isValidToken(token)
        self.assertEqual(res,'False')

    def test_invalidtoken2(self):
        token = 'datalabtest.1148.1148.abcdefghijklmnopqrstuvwxyz12345678910111213'
        res = authClient.isValidToken(token)
        self.assertEqual(res,'False')

    def test_invalidtoken3(self):
        token = 'abcdefghijklmnopqrstuvwxyz12345678910111213'
        res = authClient.isValidToken(token)
        self.assertFalse(res)

if __name__ == '__main__':
  suite = suite()
  unittest.TextTestRunner(verbosity = 2).run(suite)

