#!/usr/bin/env python
# D. Nidever
#
# testdlinterface.py
# Python code to (unit) test the DL Interface operations

from dl.dlinterface import Dlinterface
from dl import authClient, storeClient, queryClient
import os
import unittest

from urllib.parse import urlencode, quote_plus      # Python 3
from urllib.request import urlopen, Request         # Python 3
import requests
from astropy.table import Table
import numpy as np
from io import StringIO
import time
import sys

class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout

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

qryresid = np.array(['77.1096574','77.572838'])
qryresra = np.array([150.552192729936,150.55443538686])
qryresdec = np.array([-32.7846851370221,-32.7850014657006])


# Test query
qry = "select id,ra,dec from smash_dr1.object where "\
      "(ra > 180 and ra < 180.1 and dec > -36.3 and dec < -36.2) "\
      "order by ra limit 2"

qryadql = "select TOP 2 id,ra,dec from smash_dr1.object where "\
          "(ra > 180 and ra < 180.1 and dec > -36.3 and dec < -36.2) "\
          "order by ra"

# Test query results
qryrescsv = 'id,ra,dec\n'\
            '109.127614,180.000153966131,-36.2301641016901\n'\
            '109.128390,180.000208026483,-36.2290234336001\n'

qryresascii = '109.127614\t180.000153966131\t'\
              '-36.2301641016901\n109.128390\t'\
              '180.000208026483\t-36.2290234336001\n'

qryid = np.array(['109.127614','109.128390'])
qryra = np.array([180.000153966, 180.000208026])
qrydec = np.array([-36.2301641017, -36.2290234336])

qryresvotablesql = '<?xml version="1.0" encoding="utf-8"?>\n<!-- Produced with astropy.io.votable version 1.3.2\n     http://www.astropy.org/ -->\n<VOTABLE version="1.2" xmlns="http://www.ivoa.net/xml/VOTable/v1.2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.ivoa.net/xml/VOTable/v1.2">\n <RESOURCE type="results">\n  <TABLE>\n   <FIELD ID="id" arraysize="10" datatype="char" name="id"/>\n   <FIELD ID="ra" datatype="double" name="ra"/>\n   <FIELD ID="dec" datatype="double" name="dec"/>\n   <DATA>\n    <TABLEDATA>\n     <TR>\n      <TD>109.127614</TD>\n      <TD>180.00015396613099</TD>\n      <TD>-36.2301641016901</TD>\n     </TR>\n     <TR>\n      <TD>109.128390</TD>\n      <TD>180.00020802648299</TD>\n      <TD>-36.229023433600098</TD>\n     </TR>\n    </TABLEDATA>\n   </DATA>\n  </TABLE>\n </RESOURCE>\n</VOTABLE>\n'

qryresvotableadql = '<?xml version="1.0" encoding="UTF-8"?>\n<VOTABLE xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\nxsi:noNamespaceSchemaLocation="xmlns:http://www.ivoa.net/xml/VOTable-1.2.xsd" version="1.2">\n<RESOURCE type="results">\n<DESCRIPTION>DALServer TAP Query</DESCRIPTION>\n<INFO name="QUERY_STATUS" value="OK"/>\n<INFO name="QUERY" value="select TOP 2 id,ra,dec from smash_dr1.object where (ra &gt; 180 and ra &lt; 180.1 and dec &gt; -36.3 and dec &lt; -36.2) order by ra"/>\n<INFO name="TableRows" value="2"/>\n<TABLE>\n<FIELD ID="id" name="id" datatype="char" ucd="meta.id;meta.main" arraysize="10" unit="None">\n<DESCRIPTION>Unique ID for this object, the field name plus a running number</DESCRIPTION>\n</FIELD>\n<FIELD ID="ra" name="ra" datatype="double" ucd="pos.eq.ra;meta.main" unit="Degrees">\n<DESCRIPTION>Right Ascension (J2000.0) of source, in degrees</DESCRIPTION>\n</FIELD>\n<FIELD ID="dec" name="dec" datatype="double" ucd="pos.eq.dec;meta.main" unit="Degrees">\n<DESCRIPTION>Declination (J2000.0) of source, in degrees</DESCRIPTION>\n</FIELD>\n<DATA>\n<TABLEDATA>\n<TR><TD>109.127614</TD><TD>180.00015396613091</TD><TD>-36.230164101690086</TD></TR>\n<TR><TD>109.128390</TD><TD>180.00020802648334</TD><TD>-36.22902343360014</TD></TR>\n</TABLEDATA>\n</DATA>\n</TABLE>\n</RESOURCE>\n</VOTABLE>\n'

def login(dl):
  token = authClient.login('dltest','datalab')
  dl.loginuser = 'dltest'
  dl.dl.save("login", "status", "loggedin")
  dl.dl.save("login", "user", "dltest")
  dl.dl.save("login", "authtoken", token)
  dl.dl.save("dltest", "authtoken", token)
  dl.loginstatus = "loggedin"

def logout(dl):
  token = dl.dl.get('login','authtoken')
  user, uid, gid, hash = token.strip().split('.', 3)
  res = authClient.logout (token)
  dl.dl.save("login", "status", "loggedout")
  dl.dl.save("login", "user", "")
  dl.dl.save("login", "authtoken", "")
  dl.loginstatus = "loggedout"
  
def suite():
  suite = unittest.TestSuite()
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestHelp))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestLogin))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestLogout))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestStatus))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestWhoami))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestServiceStatus))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestList))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestGet))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestPut))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestCopy))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestMove))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestRemove))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestMkdir))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestRmdir))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestLink))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestLoad))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestSave))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestCopyURL))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQuery))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQueryHistory))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQueryResults))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQueryStatus))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQueryProfiles))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestSchema))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestDropTable))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestExportTable))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestListDB))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestSIAQuery))
  return suite

class TestList(unittest.TestCase):

    def setUp(self):
        self.file1 = 'lstest1.csv'
        self.file2 = 'lstest2.csv'
        self.dir1 = 'lstest1'
        self.dir2 = 'lstest2'
        self.file3 = self.dir1+'/'+self.file1
        self.testdata = testdata        
        fh = open(self.file1,'w')
        fh.write(testdata)
        fh.close()
        # Delete input file1 if it exists already
        if storeClient.ls(TEST_TOKEN,self.file1,'csv') != '':
          storeClient.rm(TEST_TOKEN,self.file1)
        # Delete input file2 if it exists already
        if storeClient.ls(TEST_TOKEN,self.file2,'csv') != '':
          storeClient.rm(TEST_TOKEN,self.file2)
        # Create dir1 if it does NOT exist already
        if storeClient.ls(TEST_TOKEN,self.dir1,'csv') == '':
          storeClient.mkdir(TEST_TOKEN,self.dir1)
        # Delete dir2 if it exists already
        if storeClient.ls(TEST_TOKEN,self.dir2,'csv') != '':
          storeClient.rmdir(TEST_TOKEN,self.dir2)
        # Put local file to VOSpace
        storeClient.put(TEST_TOKEN,self.file1,self.file1)
        # Put local file to VOSpace directory
        storeClient.put(TEST_TOKEN,self.file1,self.file3)
        # Delete temporary local test file
        os.remove(self.file1)
        
    def tearDown(self):
        # Delete file1 if it exists
        if storeClient.ls(TEST_TOKEN,self.file1,'csv') != '':
          storeClient.rm(TEST_TOKEN,self.file1)
        # Delete file2 if it exists
        if storeClient.ls(TEST_TOKEN,self.file2,'csv') != '':
          storeClient.rm(TEST_TOKEN,self.file2)
        # Delete file2 if it exists
        if storeClient.ls(TEST_TOKEN,self.file3,'csv') != '':
          storeClient.rm(TEST_TOKEN,self.file3)
        # Delete dir1 if it exists
        if storeClient.ls(TEST_TOKEN,self.dir1,'csv') != '':
          storeClient.rmdir(TEST_TOKEN,self.dir1)
        # Delete dir2 if it exists
        if storeClient.ls(TEST_TOKEN,self.dir2,'csv') != '':
          storeClient.rmdir(TEST_TOKEN,self.dir2)
      
    def test_list(self):
        pass
        #dl = Dlinterface()
        #login(dl)
        ## Make sure that file1 exists in VOSpace
        #with Capturing() as output:
        #  dl.ls(self.file1)
        #self.assertEqual(output[0].strip(),self.file1)
        ## Make sure that file2 does NOT exist in VOSpace
        #with Capturing() as output:
        #  dl.ls(self.file2)
        #self.assertEqual(output[0].strip(),'')
        ## Make sure that file3 exists in VOSpace
        #with Capturing() as output:
        #  dl.ls(self.file3)
        #self.assertEqual(output[0].strip(),os.path.basename(self.file3))
        ## Make sure that dir1 exists in VOSpace and contains file3
        ##   which has the same base name as file1
        #with Capturing() as output:
        #  dl.ls(self.dir1)
        #self.assertEqual(output[0].strip(),self.file1)
        ## Make sure that dir2 does NOT exist in VOSpace
        #with Capturing() as output:
        #  dl.ls(self.dir2)
        #self.assertEqual(output[0].strip(),'')
        #logout(dl)
        
if __name__ == '__main__':
  suite = suite()
  unittest.TextTestRunner(verbosity = 2).run(suite)

