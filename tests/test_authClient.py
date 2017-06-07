#!/usr/bin/env python
# D. Nidever
#
# testdlinterface.py
# Python code to (unit) test the DL Client tools

from dl import authClient, storeClient, queryClient
import os
import unittest

def suite():
  suite = unittest.TestSuite()
  # authClient
  #  login, logout, isValidUser, isValidToken
  # storeClient
  #  get, put, load, cp, ln, ls, mkdir, mv, rm, rmdir, saveAs, 
  # queryClient
  #  query, status, results, list_profiles, list, schema, drop
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestCopy))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestMove))
  return suite

class TestCopy(unittest.TestCase):

    def setUp(self):
        self.file = 'test.csv'
        self.outfile = 'test2.csv'
        # Create sample test file locally
        data = 'id,ra,dec\n'\
                '77.1096574,150.552192729936,-32.7846851370221\n'\
                '77.572838,150.55443538686,-32.7850014657006\n'
        fh = open(self.file,'wb')
        fh.write(data)
        fh.close()
        self.data = data
        # Login to the dltest account
        token = authClient.login('dltest','datalab')        
        self.token = token
        # Delete input file if it exists already
        if storeClient.ls(self.token,self.file) != '':
            storeClient.rm(self.token,self.file)
        # Delete output file if it exists already
        if storeClient.ls(self.token,self.outfile) != '':
            storeClient.rm(self.token,self.outfile)
        # Put local file to VOSpace
        storeClient.put(self.token,self.file,self.file)
        # Delete temporary local test file
        os.remove(self.file)
        
    def tearDown(self):
        # Delete temporary files in VOSpace
        storeClient.rm(self.token,self.file)
        storeClient.rm(self.token,self.outfile)
      
    def test_copy(self):
        # Try copying the file
        storeClient.cp(self.token,self.file,self.outfile)
        res = storeClient.ls(self.token,self.outfile)
        self.assertEqual(res,self.outfile)
        # Copy back to local and read in
        storeClient.get(self.token,self.file,self.file)
        f = open(self.file,'r')
        fdata = f.read()
        f.close()
        os.remove(self.file)
        self.assertEqual(self.data,fdata)

class TestMove(unittest.TestCase):

    def setUp(self):
        self.file = 'test.csv'
        self.outfile = 'test2.csv'
        # Create sample test file locally
        data = 'id,ra,dec\n'\
                '77.1096574,150.552192729936,-32.7846851370221\n'\
                '77.572838,150.55443538686,-32.7850014657006\n'
        fh = open(self.file,'wb')
        fh.write(data)
        fh.close()
        self.data = data
        # Login to the dltest account
        token = authClient.login('dltest','datalab')        
        self.token = token
        # Delete input file if it exists already
        if storeClient.ls(self.token,self.file) != '':
            storeClient.rm(self.token,self.file)
        # Delete output file if it exists already
        if storeClient.ls(self.token,self.outfile) != '':
            storeClient.rm(self.token,self.outfile)
        # Put local file to VOSpace
        storeClient.put(self.token,self.file,self.file)
        # Delete temporary local test file
        os.remove(self.file)
        
    def tearDown(self):
        # Delete temporary files in VOSpace
        storeClient.rm(self.token,self.file)
        storeClient.rm(self.token,self.outfile)
      
    def test_move(self):
        # Try moving the file
        storeClient.mv(self.token,self.file,self.outfile)
        res = storeClient.ls(self.token,self.outfile)
        self.assertEqual(res,self.outfile)
        # Copy back to local and read in
        storeClient.get(self.token,self.outfile,self.outfile)
        f = open(self.outfile,'r')
        fdata = f.read()
        f.close()
        os.remove(self.outfile)
        self.assertEqual(self.data,fdata)


# query with WHERE clause with SORT BY and LIMIT
        
if __name__ == '__main__':
  suite = suite()
  unittest.TextTestRunner(verbosity = 2).run(suite)

