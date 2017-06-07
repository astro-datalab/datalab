#!/usr/bin/env python
# D. Nidever
#
# test_storeClient.py
# Python code to (unit) test the DL storage client code

from dl import storeClient
import os
import unittest
import requests

# Service URLs
AM_URL = "http://dlsvcs.datalab.noao.edu/auth"      # Auth Manager
SM_URL = "http://dlsvcs.datalab.noao.edu/storage"   # Storage Manager
QM_URL = "http://dlsvcs.datalab.noao.edu/query"     # Query Manager
# Test token
TEST_TOKEN = "dltest.99998.99998.test_access"

# Create test data sample
testdata = 'id,ra,dec\n'\
           '77.1096574,150.552192729936,-32.7846851370221\n'\
           '77.572838,150.55443538686,-32.7850014657006\n'

def fileExists(name):
  url = SM_URL + "/ls?name=vos://%s&format=%s" % (name, 'raw')
  r = requests.get(url, headers={'X-DL-AuthToken': TEST_TOKEN})
  res = r.content.decode('utf-8')
  return (True if (name in res) else False)
  
def get(fr,to=None):
  url = requests.get(SM_URL + "/get?name=%s" % ('vos://'+fr),
                     headers={"X-DL-AuthToken": TEST_TOKEN})
  r = requests.get(url.text, stream=True)
  if to is not None:
    with open(to, 'wb', 0) as fd:
      fd.write(r.raw.data)
    fd.close()
  else:
    return r.raw.data
    
def put(fr,to):
  r = requests.get(SM_URL + "/put?name=%s" % 'vos://'+to, headers = {"X-DL-AuthToken": TEST_TOKEN})
  with open(fr, 'rb') as file:
    requests.put(r.content, data=file,
                 headers={'Content-type': 'application/octet-stream',
                          'X-DL-AuthToken': TEST_TOKEN})

def rm(name):
  name = (name if name.startswith('vos://') else ('vos://'+name))
  path = "/rm?file=%s" % name
  resp = requests.get("%s%s" % (SM_URL, path), headers = {"X-DL-AuthToken": TEST_TOKEN})
  
def suite():
  suite = unittest.TestSuite()
  # storeClient
  #  get, put, load, cp, ln, ls, mkdir, mv, rm, rmdir, saveAs, 
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestCopy))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestMove))
  return suite

class TestGet(unittest.TestCase):

    def setUp(self):

    def tearDown(self):
      
    def test_copy(self):

class TestPut(unittest.TestCase):

    def setUp(self):

    def tearDown(self):
      
    def test_copy(self):

class TestLoad(unittest.TestCase):

    def setUp(self):

    def tearDown(self):
      
    def test_copy(self):
      
class TestCopy(unittest.TestCase):

    def setUp(self):
        self.file = 'test.csv'
        self.outfile = 'test2.csv'
        self.testdata = testdata        
        fh = open(self.file,'wb')
        fh.write(testdata)
        fh.close()
        # Delete input file if it exists already
        if fileExists(self.file):
          rm(self.file)
        # Delete output file if it exists already
        if fileExists(self.outfile):
          rm(self.outfile)
        # Put local file to VOSpace
        put(self.file,self.file)
        # Delete temporary local test file
        os.remove(self.file)
            
    def tearDown(self):
        # Delete input file in VOSpace
        rm(self.file)
        # Delete output file in VOSpace
        rm(self.outfile)
      
    def test_copy(self):
        # Try copying the file
        storeClient.cp(self.token,self.file,self.outfile)
        # Check that the file is there
        self.assertEqual(True,fileExists(self.outfile))
        # Copy back to local with get
        outdata = get(self.outfile)
        # Make sure they are equal
        self.assertEqual(self.testdata,outdata)

class TestMove(unittest.TestCase):

    def setUp(self):
        self.file = 'test.csv'
        self.outfile = 'test2.csv'
        self.testdata = testdata        
        fh = open(self.file,'wb')
        fh.write(testdata)
        fh.close()
        # Delete input file if it exists already
        if fileExists(self.file):
          rm(self.file)
        # Delete output file if it exists already
        if fileExists(self.outfile):
          rm(self.outfile)
        # Put local file to VOSpace
        put(self.file,self.file)
        # Delete temporary local test file
        os.remove(self.file)
            
    def tearDown(self):
        # Delete input file in VOSpace
        rm(self.file)
        # Delete output file in VOSpace
        rm(self.outfile)
      
    def test_move(self):
        # Try copying the file
        storeClient.mv(self.token,self.file,self.outfile)
        # Check that the file is there
        self.assertEqual(True,fileExists(self.outfile))
        # Get the file contents
        outdata = get(self.outfile)
        # Make sure they are equal
        self.assertEqual(self.testdata,outdata)

class TestRemove(unittest.TestCase):

    def setUp(self):

    def tearDown(self):
      
    def test_copy(self):
        
class TestLink(unittest.TestCase):

    def setUp(self):

    def tearDown(self):
      
    def test_copy(self):

class TestList(unittest.TestCase):

    def setUp(self):

    def tearDown(self):
      
    def test_copy(self):

class TestMkdir(unittest.TestCase):

    def setUp(self):

    def tearDown(self):
      
    def test_copy(self):

class TestRmdir(unittest.TestCase):

    def setUp(self):

    def tearDown(self):
      
    def test_copy(self):

class TestSaveas(unittest.TestCase):

    def setUp(self):

    def tearDown(self):
      
    def test_copy(self):
      

# query with WHERE clause with SORT BY and LIMIT
        
if __name__ == '__main__':
  suite = suite()
  unittest.TextTestRunner(verbosity = 2).run(suite)

