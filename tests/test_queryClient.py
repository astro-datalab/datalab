#!/usr/bin/env python
# D. Nidever
#
# testdlinterface.py
# Python code to (unit) test the DL Client tools

from dl import authClient, storeClient, queryClient
import os
import unittest
try:
    from urllib import urlencode, quote_plus	        # Python 2
    from urllib2 import urlopen, Request                # Python 2
except ImportError:
    from urllib.parse import urlencode, quote_plus      # Python 3
    from urllib.request import urlopen, Request         # Python 3
import requests
from astropy.table import Table
import numpy as np
from io import StringIO

# Service URLs
AM_URL = "http://dlsvcs.datalab.noao.edu/auth"      # Auth Manager
SM_URL = "http://dlsvcs.datalab.noao.edu/storage"   # Storage Manager
QM_URL = "http://dlsvcs.datalab.noao.edu/query"     # Query Manager
# Test token
TEST_TOKEN = "dltest.99998.99998.test_access"

# Test query
qry = "select id,ra,dec from smash_dr1.object where "\
      "(ra > 180 and ra < 180.1 and dec > -36.3 and dec < -36.2) "\
      "order by ra limit 2"

# Test query results
qryrescsv = 'id,ra,dec\n'\
            '109.127614,180.000153966131,-36.2301641016901\n'\
            '109.128390,180.000208026483,-36.2290234336001\n'

qryresascii = '109.127614\t180.000153966131\t'\
              '-36.2301641016901\n109.128390\t'\
              '180.000208026483\t-36.2290234336001\n'

qryresid = np.array(['109.127614','109.128390'])
qryresra = np.array([180.000153966, 180.000208026])
qryresdec = np.array([-36.2301641017, -36.2290234336])

qryresvotable = '<?xml version="1.0" encoding="utf-8"?>\n<!-- Produced with astropy.io.votable version 1.3.2\n     http://www.astropy.org/ -->\n<VOTABLE version="1.2" xmlns="http://www.ivoa.net/xml/VOTable/v1.2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.ivoa.net/xml/VOTable/v1.2">\n <RESOURCE type="results">\n  <TABLE>\n   <FIELD ID="id" arraysize="10" datatype="char" name="id"/>\n   <FIELD ID="ra" datatype="double" name="ra"/>\n   <FIELD ID="dec" datatype="double" name="dec"/>\n   <DATA>\n    <TABLEDATA>\n     <TR>\n      <TD>109.127614</TD>\n      <TD>180.00015396613099</TD>\n      <TD>-36.2301641016901</TD>\n     </TR>\n     <TR>\n      <TD>109.128390</TD>\n      <TD>180.00020802648299</TD>\n      <TD>-36.229023433600098</TD>\n     </TR>\n    </TABLEDATA>\n   </DATA>\n  </TABLE>\n </RESOURCE>\n</VOTABLE>\n'

def list(table):
    headers = {'Content-Type': 'text/ascii', 'X-DL-AuthToken': TEST_TOKEN}
    dburl = '%s/list?table=%s' % (QM_URL, table)
    r = requests.get(dburl, headers=headers)
    return r.content.decode('utf-8')
  
def drop(table):
    headers = {'Content-Type': 'text/ascii', 'X-DL-AuthToken': TEST_TOKEN}
    dburl = '%s/delete?table=%s' % (QM_URL, table)
    r = requests.get(dburl, headers=headers)
    return r.content.decode('utf-8')

def sqlquery(qry, fmt='csv', out=None, async=False):
  qry = quote_plus(qry)
  dburl = '%s/query?adql=%s&ofmt=%s&out=%s&async=%s' % (QM_URL, qry, fmt, out, async)
  dburl += "&profile=%s" % "default"
  headers = {'Content-Type': 'text/ascii', 'X-DL-AuthToken': TEST_TOKEN}
  r = requests.get(dburl, headers=headers)
  return r.content.decode('utf-8')

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
  # queryClient
  #  query, status, results, list_profiles, list, schema, drop
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQuerySql))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQuerySqlToVospaceCsv))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQuerySqlToVospaceFits))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQuerySqlToMydb))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQueryAdql))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQueryAdqlToVospace))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQueryAdqlToMydb))
  # async
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestStatus))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestResults))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestListProfiles))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestListNotExists))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestListExists))
  #suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestSchema))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestDrop))
  return suite

class TestQuerySql(unittest.TestCase):

    def test_querysqlcsv(self):
        self.qry = qry
        # Run the list command
        res = queryClient.query(TEST_TOKEN,sql=self.qry,out=None,async=False,fmt='csv')
        self.assertEqual(res,qryrescsv)

    def test_querysqlascii(self):
        self.qry = qry
        # Run the list command
        res = queryClient.query(TEST_TOKEN,sql=self.qry,out=None,async=False,fmt='ascii')
        self.assertEqual(res,qryresascii)

    def test_querysqlvotable(self):
        self.qry = qry
        # Run the list command
        res = queryClient.query(TEST_TOKEN,sql=self.qry,out=None,async=False,fmt='votable')
        self.assertEqual(res,qryresvotable)

class TestQuerySqlToVospaceCsv(unittest.TestCase):

    def setUp(self):
        self.outfile = 'qrytest.csv'
        # Delete file if it already exists
        if fileExists(self.outfile):
          rm(self.outfile)

    def tearDown(self):
        # Delete temporary file
        if fileExists(self.outfile):
          rm(self.outfile)
  
    def test_querysqltovospacecsv(self):
        self.qry = qry
        # Run the list command
        res = queryClient.query(TEST_TOKEN,sql=self.qry,out='vos://'+self.outfile,async=False,fmt='csv')
        self.assertEqual(res,'OK')
        self.assertTrue(fileExists(self.outfile))
        # Get the results and compare
        data = get(self.outfile)
        self.assertEqual(data,qryrescsv)

class TestQuerySqlToVospaceFits(unittest.TestCase):

    def setUp(self):
        self.outfile = 'qrytest.fits'
        # Delete file if it already exists
        if fileExists(self.outfile):
          rm(self.outfile)
        # Delete local file if it exists
        if os.path.exists(self.outfile):
          os.remove(self.outfile)
          
    def tearDown(self):
        # Delete temporary file
        if fileExists(self.outfile):
          rm(self.outfile)
        # Delete temporary local file
        if os.path.exists(self.outfile):
          os.remove(self.outfile)
          
    def test_querysqltovospacefits(self):
        self.qry = qry
        # Run the list command
        res = queryClient.query(TEST_TOKEN,sql=self.qry,out='vos://'+self.outfile,async=False,fmt='fits')
        self.assertEqual(res,'OK')
        self.assertTrue(fileExists(self.outfile))
        # Get the results and compare
        res = get(self.outfile,self.outfile)
        outdata = Table.read(self.outfile,format='fits')
        self.assertEqual(outdata['id'].data.tostring(),qryresid.tostring())
        self.assertTrue(np.allclose(outdata['ra'].data, qryresra))
        self.assertTrue(np.allclose(outdata['dec'].data, qryresdec))

class TestQuerySqlToMydb(unittest.TestCase):

    def setUp(self):
        self.table = 'qrytable'
        # Make sure test table does not exist
        if list(self.table) != 'relation "'+self.table+'" not known':
          drop(self.table)
          
    def tearDown(self):
        # Delete table
        if list(self.table) != 'relation "'+self.table+'" not known':
          drop(self.table)
          
    def test_querysqltomydb(self):
        self.qry = qry
        # Run the list command
        res = queryClient.query(TEST_TOKEN,sql=self.qry,out='mydb://'+self.table,async=False)
        self.assertEqual(res,'OK')
        res = list(self.table)
        self.assertNotEqual(res,'relation "'+self.table+'" not known')
        # Get the results and compare
        res = sqlquery(self.qry,fmt='csv',async=False,out=None)
        tab = Table.read(StringIO(res),format='ascii.csv')
        self.assertTrue(np.allclose(tab['id'].data,qryresid.astype('float64')))
        self.assertTrue(np.allclose(tab['ra'].data,qryresra))
        self.assertTrue(np.allclose(tab['dec'].data,qryresdec))
        
#class TestResults(unittest.TestCase):
#
#    def setUp(self):
#        self.table = 'lstable'
#        res = sqlquery('', fmt='csv', out=None, async=True):
#        # Make sure test table does not exist
#        if list(self.table) != 'relation "'+self.table+'" not known':
#          drop(self.table)
#
#    def tearDown(self):
#        pass
#      
#    def test_listnotexist(self):
#        # Run the list command
#        res = queryClient.list(TEST_TOKEN,self.table)
#        self.assertEqual(res,'relation "'+self.table+'" not known')


class TestListNotExists(unittest.TestCase):

    def setUp(self):
        self.table = 'lstable'
        # Make sure test table does not exist
        if list(self.table) != 'relation "'+self.table+'" not known':
          drop(self.table)

    def tearDown(self):
        pass
      
    def test_listnotexist(self):
        # Run the list command
        res = queryClient.list(TEST_TOKEN,self.table)
        self.assertEqual(res,'relation "'+self.table+'" not known')

class TestListExists(unittest.TestCase):

    def setUp(self):
        self.table = 'lstable'
        # Make sure test table does not exist
        if list(self.table) != 'relation "'+self.table+'" not known':
          drop(self.table)
        # Create table with a query
        res = sqlquery('select * from smash_dr1.object limit 10',out='mydb://'+self.table)
        
    def tearDown(self):
        # Delete temporary table from VOSpace
        drop(self.table)
      
    def test_listexists(self):
        # Run the list command
        res = queryClient.list(TEST_TOKEN,self.table)
        self.assertNotEqual(res,'relation "'+self.table+'" not known')

    def test_listcontents(self):
        # Run the list command
        res = queryClient.list(TEST_TOKEN,self.table)
        self.assertIn('ra,double precision',res)
        self.assertIn('umag,real',res)
                         
class TestDrop(unittest.TestCase):

    def setUp(self):
        self.table = 'dptable'
        # Make sure test table does not exist
        if list(self.table) != 'relation "'+self.table+'" not known':
          drop(self.table)
        # Create table with a query
        res = sqlquery('select * from smash_dr1.object limit 10',out='mydb://'+self.table)
        
    def tearDown(self):
        # Delete temporary table from VOSpace
        drop(self.table)
      
    def test_drop(self):
        # Try dropping the table
        res = queryClient.drop(TEST_TOKEN,self.table)
        self.assertEqual(res,'')
        # Check that it is not there anymore
        res = list(self.table)
        self.assertEqual(res,'relation "'+self.table+'" not known')


# query with WHERE clause with SORT BY and LIMIT
        
if __name__ == '__main__':
  suite = suite()
  unittest.TextTestRunner(verbosity = 2).run(suite)

