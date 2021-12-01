#!/usr/bin/env python
# D. Nidever
#
# test_storeClient.py
# Python code to (unit) test the DL storage client code

from dl import storeClient
import os
import unittest
import requests
import numpy as np
from io import StringIO

# Service URLs
AM_URL = "https://dlsvcs.datalab.noirlab.edu/auth"      # Auth Manager
SM_URL = "https://dlsvcs.datalab.noirlab.edu/storage"   # Storage Manager
QM_URL = "https://dlsvcs.datalab.noirlab.edu/query"     # Query Manager
# Test token
TEST_TOKEN = "dltest.99998.99998.test_access"

# Create test data sample
testdata = 'id,ra,dec\n'\
           '77.1096574,150.552192729936,-32.7846851370221\n'\
           '77.572838,150.55443538686,-32.7850014657006\n'

qryresid = np.array(['77.1096574','77.572838'])
qryresra = np.array([150.552192729936,150.55443538686])
qryresdec = np.array([-32.7846851370221,-32.7850014657006])

def fileExists(name):
  url = SM_URL + "/ls?name=vos://%s&format=%s" % (name, 'raw')
  r = requests.get(url, headers={'X-DL-AuthToken': TEST_TOKEN})
  res = r.content.decode('utf-8')
  return (True if (name in res) else False)

def list(name,fmt='raw'):
  url = SM_URL + "/ls?name=vos://%s&format=%s" % (name, fmt)
  r = requests.get(url, headers={'X-DL-AuthToken': TEST_TOKEN})
  res = r.content.decode('utf-8')
  return res

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

def mkdir(name):
  name = (name if name.startswith('vos://') else ('vos://'+name))
  path = "/mkdir?dir=%s" % name
  resp = requests.get("%s%s" % (SM_URL, path), headers = {"X-DL-AuthToken": TEST_TOKEN})

def rmdir(name):
  name = (name if name.startswith('vos://') else ('vos://'+name))
  path = "/rmdir?dir=%s" % name
  resp = requests.get("%s%s" % (SM_URL, path), headers = {"X-DL-AuthToken": TEST_TOKEN})
  
def suite():
  suite = unittest.TestSuite()
  # storeClient
  #  get, put, load, cp, ln, ls, mkdir, mv, rm, rmdir, saveAs, 
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestPut))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestGet))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestLoad))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestCopy))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestCopyToDir))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestMove))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestMoveToDir))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestRemove))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestLink))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestList))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestMkdir))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestRmdir))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestSaveAs))
  return suite

class TestPut(unittest.TestCase):

    def setUp(self):
        self.file = 'puttest.csv'
        self.testdata = testdata        
        fh = open(self.file,'w')
        fh.write(testdata)
        fh.close()
        # Delete input file if it exists already
        if fileExists(self.file):
          rm(self.file)
        # Put local file to VOSpace
        put(self.file,self.file)
            
    def tearDown(self):
        # Delete temporary local test file
        os.remove(self.file)
        # Delete input file in VOSpace
        rm(self.file)
      
    def test_put(self):
        # Try putting the file
        storeClient.put(TEST_TOKEN,self.file,self.file)
        # Check that the file is there
        self.assertTrue(fileExists(self.file))
        # Read the data with get
        outdata = get(self.file)
        # Make sure they are equal
        self.assertEqual(outdata.decode('utf-8'),self.testdata)

# puttodir??


class TestGet(unittest.TestCase):

    def setUp(self):
        self.file = 'gettest.csv'
        self.testdata = testdata        
        fh = open(self.file,'w')
        fh.write(testdata)
        fh.close()
        # Delete input file if it exists already
        if fileExists(self.file):
          rm(self.file)
        # Put local file to VOSpace
        put(self.file,self.file)
        # Delete local file
        os.remove(self.file)
        
    def tearDown(self):
        # Delete temporary local test file
        os.remove(self.file)
        # Delete input file in VOSpace
        rm(self.file)
      
    def test_get(self):
        # Get the file
        storeClient.get(TEST_TOKEN,self.file,self.file)
        # Check that the file is there
        os.path.exists(self.file)
        self.assertTrue(os.path.exists(self.file))
        # Read the data
        fh = open(self.file,'r')
        outdata = fh.read()
        fh.close()
        # Make sure they are equal
        self.assertEqual(outdata,self.testdata)

class TestLoad(unittest.TestCase):

    def setUp(self):
        # Download the url from the web
        self.file = 'loadtest.txt'
        self.url = 'https://datalab.noirlab.edu/tests/loadtest.txt'
        # Get the webpage contents
        self.testdata = requests.get(self.url).content
        # Delete input file if it exists already
        if fileExists(self.file):
          rm(self.file)
            
    def tearDown(self):
        # Delete input file in VOSpace
        rm(self.file)
      
    def test_load(self):
        # Load the file
        storeClient.load(TEST_TOKEN,'vos://'+self.file,self.url)
        # Check that the file is there
        self.assertTrue(fileExists(self.file))
        # Read the data
        outdata = get(self.file)
        # Make sure they are equal
        self.assertEqual(outdata,self.testdata)
      
class TestCopy(unittest.TestCase):

    def setUp(self):
        self.file = 'cptest.csv'
        self.outfile = 'cptest2.csv'
        self.testdata = testdata        
        fh = open(self.file,'w')
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
        storeClient.cp(TEST_TOKEN,self.file,self.outfile)
        # Check that the file is there
        self.assertTrue(fileExists(self.outfile))
        # Read the data with get
        outdata = get(self.outfile)
        # Make sure they are equal
        self.assertEqual(outdata.decode('utf-8'),self.testdata)

class TestCopyToDir(unittest.TestCase):

    def setUp(self):
        self.file = 'cptodirtest.csv'
        self.dir = 'cptodirtest'
        self.outfile = self.dir+'/'+self.file
        self.testdata = testdata        
        fh = open(self.file,'w')
        fh.write(testdata)
        fh.close()
        # Delete input file if it exists already
        if fileExists(self.file):
          rm(self.file)
        # Delete output file if it exists already
        if fileExists(self.outfile):
          rm(self.outfile)
        # Create directory if it doesn't exist
        if not fileExists(self.dir):
          mkdir(self.dir)
        # Put local file to VOSpace
        put(self.file,self.file)
        # Delete temporary local test file
        os.remove(self.file)
            
    def tearDown(self):
        # Delete input file in VOSpace
        rm(self.file)
        # Delete output file in VOSpace
        rm(self.outfile)
        # Delete directory
        rmdir(self.dir)
        
    def test_copytodir(self):
        # Try copying the file
        storeClient.cp(TEST_TOKEN,self.file,self.outfile)
        # Check that the file is there
        self.assertTrue(fileExists(self.outfile))
        # Read the data with get
        outdata = get(self.outfile)
        # Make sure they are equal
        self.assertEqual(outdata.decode('utf-8'),self.testdata)
        
class TestMove(unittest.TestCase):

    def setUp(self):
        self.file = 'mvtest.csv'
        self.outfile = 'mvtest2.csv'
        self.testdata = testdata        
        fh = open(self.file,'w')
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
        storeClient.mv(TEST_TOKEN,self.file,self.outfile)
        # Check that the file is there
        self.assertTrue(fileExists(self.outfile))
        # Load the file with get
        outdata = get(self.outfile)
        # Make sure they are equal
        self.assertEqual(outdata.decode('utf-8'),self.testdata)
        # Make sure the original file doesn't exist anymore
        self.assertFalse(fileExists(self.file))
        
class TestMoveToDir(unittest.TestCase):

    def setUp(self):
        self.file = 'mvtodirtest.csv'
        self.dir = 'mvtodirtest'
        self.outfile = self.dir+'/'+self.file
        self.testdata = testdata        
        fh = open(self.file,'w')
        fh.write(testdata)
        fh.close()
        # Delete input file if it exists already
        if fileExists(self.file):
          rm(self.file)
        # Delete output file if it exists already
        if fileExists(self.outfile):
          rm(self.outfile)
        # Create directory if it doesn't exist
        if not fileExists(self.dir):
          mkdir(self.dir)
        # Put local file to VOSpace
        put(self.file,self.file)
        # Delete temporary local test file
        os.remove(self.file)
            
    def tearDown(self):
        # Delete input file in VOSpace
        rm(self.file)
        # Delete output file in VOSpace
        rm(self.outfile)
        # Delete directory
        rmdir(self.dir)
        
    def test_movetodir(self):
        # Try moving the file
        storeClient.mv(TEST_TOKEN,self.file,self.outfile)
        # Check that the file is there
        self.assertTrue(fileExists(self.outfile))
        # Read the data with get
        outdata = get(self.outfile)
        # Make sure they are equal
        self.assertEqual(outdata.decode('utf-8'),self.testdata)
        # Make sure the original file doesn't exist anymore
        self.assertFalse(fileExists(self.file))

class TestRemove(unittest.TestCase):

    def setUp(self):
        self.file = 'rmtest.csv'
        self.testdata = testdata        
        fh = open(self.file,'w')
        fh.write(testdata)
        fh.close()
        # Delete input file if it exists already
        if fileExists(self.file):
          rm(self.file)
        # Put local file to VOSpace
        put(self.file,self.file)
        # Delete temporary local test file
        os.remove(self.file)
            
    def tearDown(self):
        # Delete input file in VOSpace
        rm(self.file)
      
    def test_remove(self):
        # Try removing the file
        storeClient.rm(TEST_TOKEN,self.file)
        # Check that the file is gone
        self.assertFalse(fileExists(self.file))

class TestLink(unittest.TestCase):

    def setUp(self):
        self.file = 'lntest.csv'
        self.link = 'lnlink'
        self.testdata = testdata        
        fh = open(self.file,'w')
        fh.write(testdata)
        fh.close()
        # Delete input file if it exists already
        if fileExists(self.file):
          rm(self.file)
        # Delete output file if it exists already
        if fileExists(self.link):
          rm(self.outfile)
        # Put local file to VOSpace
        put(self.file,self.file)
        # Delete temporary local test file
        os.remove(self.file)
            
    def tearDown(self):
        # Delete input file in VOSpace
        rm(self.file)
        # Delete output file in VOSpace
        rm(self.link)
      
    def test_link(self):
        # Create the link
        storeClient.ln(TEST_TOKEN,'vos://'+self.link,'vos://'+self.file)
        # Check that the file is there
        self.assertTrue(fileExists(self.link))
        # Test it by looking at the raw listing and make sure the
        #  properties match what we expect
        res = list(self.link)
        #  For now just check that the file and link are in the raw listing
        #   in the future check that the properties are set correctly
        self.assertIn(self.file,res)
        self.assertIn(self.link,res)
        # Links don't automatically return the data
        ## Read the data with get
        #filedata = get(self.file)
        ## Read the data with get
        #linkdata = get(self.link)
        ## Make sure they are equal
        #self.assertEqual(filedata.decode('utf-8'),self.testdata)
        #self.assertEqual(linkdata.decode('utf-8'),self.testdata)
        #self.assertEqual(filedata.decode('utf-8'),linkdata.decode('utf-8'))

class TestList(unittest.TestCase):

    def setUp(self):
        self.file1 = 'lstest1.csv'
        self.file2 = 'lstest2.csv'
        self.dir1 = 'lstest1'
        self.dir2 = 'lstest2'
        self.testdata = testdata        
        fh = open(self.file1,'w')
        fh.write(testdata)
        fh.close()
        # Delete input file1 if it exists already
        if fileExists(self.file1):
          rm(self.file1)
        # Delete input file2 if it exists already
        if fileExists(self.file2):
          rm(self.file2)
        # Create dir1 if it does NOT exist already
        if not fileExists(self.dir1):
          mkdir(self.dir1)
        # Delete dir2 if it exists already
        if fileExists(self.dir2):
          rmdir(self.dir2)
        # Put local file to VOSpace
        put(self.file1,self.file1)
        # Delete temporary local test file
        os.remove(self.file1)
        
    def tearDown(self):
        # Delete file1 if it exists
        if fileExists(self.file1):
          rm(self.file1)
        # Delete file2 if it exists
        if fileExists(self.file2):
          rm(self.file2)
        # Delete dir1 if it exists
        if fileExists(self.dir1):
          rmdir(self.dir1)
        if fileExists(self.dir2):
          rmdir(self.dir2)
      
    def test_list(self):
        # Make sure that file1 exists in VOSpace
        self.assertEqual(storeClient.ls(TEST_TOKEN,self.file1,'csv'),self.file1)
        # Make sure that file2 does NOT exist in VOSpace
        self.assertEqual(storeClient.ls(TEST_TOKEN,self.file2,'csv'),'')
        # Make sure that dir1 exists in VOSpace
        self.assertEqual(storeClient.ls(TEST_TOKEN,self.dir1,'csv'),self.dir1)
        # Make sure that dir2 does NOT exist in VOSpace
        self.assertEqual(storeClient.ls(TEST_TOKEN,self.dir2,'csv'),'')

class TestMkdir(unittest.TestCase):

    def setUp(self):
        self.dir = 'mkdirtest'
        # Delete directory if it exists already
        if fileExists(self.dir):
          rmdir(self.dir)
            
    def tearDown(self):
        # Delete dir
        rmdir(self.dir)
      
    def test_mkdir(self):
        # Try making the directory
        storeClient.mkdir(TEST_TOKEN,self.dir)
        # Check that the file is gone
        self.assertTrue(fileExists(self.dir))

class TestRmdir(unittest.TestCase):

    def setUp(self):
        self.dir = 'rmdirtest'
        # Create directory if it does NOT already exist
        if fileExists(self.dir):
          mkdir(self.dir)
            
    def tearDown(self):
        # Delete dir
        rmdir(self.dir)
      
    def test_rmdir(self):
        # Try making the directory
        storeClient.rmdir(TEST_TOKEN,self.dir)
        # Check that the file is gone
        self.assertFalse(fileExists(self.dir))

    
class TestSaveAs(unittest.TestCase):

    def setUp(self):
        self.file = 'svtest.csv'
        self.testdata = testdata        
        # Delete input file if it exists already
        if fileExists(self.file):
          rm(self.file)
        
    def tearDown(self):
        # Delete input file in VOSpace
        rm(self.file)
      
    def test_saveas(self):
        # Try saving the data
        res = storeClient.saveAs(TEST_TOKEN,testdata,self.file)
        self.assertEqual(res,'OK')
        # Check that the file is there
        self.assertTrue(fileExists(self.file))
        # Read the data with get
        outdata = get(self.file)
        # Make sure they are equal
        self.assertEqual(outdata.decode('utf-8'),testdata)
        
if __name__ == '__main__':
  suite = suite()
  unittest.TextTestRunner(verbosity = 2).run(suite)

