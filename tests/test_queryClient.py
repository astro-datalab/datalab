#!/usr/bin/env python
# D. Nidever
#
# test_queryClient.py
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
import time

# Service URLs
AM_URL = "https://dlsvcs.datalab.noirlab.edu/auth"      # Auth Manager
SM_URL = "https://dlsvcs.datalab.noirlab.edu/storage"   # Storage Manager
QM_URL = "https://dlsvcs.datalab.noirlab.edu/query"     # Query Manager
# Test token
TEST_TOKEN = "dltest.99998.99998.test_access"

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

qryresid = np.array(['109.127614','109.128390'])
qryresra = np.array([180.000153966, 180.000208026])
qryresdec = np.array([-36.2301641017, -36.2290234336])

qryresvotablesql = '<?xml version="1.0" encoding="utf-8"?>\n<!-- Produced with astropy.io.votable version 1.3.2\n     http://www.astropy.org/ -->\n<VOTABLE version="1.2" xmlns="http://www.ivoa.net/xml/VOTable/v1.2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://www.ivoa.net/xml/VOTable/v1.2">\n <RESOURCE type="results">\n  <TABLE>\n   <FIELD ID="id" arraysize="10" datatype="char" name="id"/>\n   <FIELD ID="ra" datatype="double" name="ra"/>\n   <FIELD ID="dec" datatype="double" name="dec"/>\n   <DATA>\n    <TABLEDATA>\n     <TR>\n      <TD>109.127614</TD>\n      <TD>180.00015396613099</TD>\n      <TD>-36.2301641016901</TD>\n     </TR>\n     <TR>\n      <TD>109.128390</TD>\n      <TD>180.00020802648299</TD>\n      <TD>-36.229023433600098</TD>\n     </TR>\n    </TABLEDATA>\n   </DATA>\n  </TABLE>\n </RESOURCE>\n</VOTABLE>\n'

qryresvotableadql = '<?xml version="1.0" encoding="UTF-8"?>\n<VOTABLE xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\nxsi:noNamespaceSchemaLocation="xmlns:http://www.ivoa.net/xml/VOTable-1.2.xsd" version="1.2">\n<RESOURCE type="results">\n<DESCRIPTION>DALServer TAP Query</DESCRIPTION>\n<INFO name="QUERY_STATUS" value="OK"/>\n<INFO name="QUERY" value="select TOP 2 id,ra,dec from smash_dr1.object where (ra &gt; 180 and ra &lt; 180.1 and dec &gt; -36.3 and dec &lt; -36.2) order by ra"/>\n<INFO name="TableRows" value="2"/>\n<TABLE>\n<FIELD ID="id" name="id" datatype="char" ucd="meta.id;meta.main" arraysize="10" unit="None">\n<DESCRIPTION>Unique ID for this object, the field name plus a running number</DESCRIPTION>\n</FIELD>\n<FIELD ID="ra" name="ra" datatype="double" ucd="pos.eq.ra;meta.main" unit="Degrees">\n<DESCRIPTION>Right Ascension (J2000.0) of source, in degrees</DESCRIPTION>\n</FIELD>\n<FIELD ID="dec" name="dec" datatype="double" ucd="pos.eq.dec;meta.main" unit="Degrees">\n<DESCRIPTION>Declination (J2000.0) of source, in degrees</DESCRIPTION>\n</FIELD>\n<DATA>\n<TABLEDATA>\n<TR><TD>109.127614</TD><TD>180.00015396613091</TD><TD>-36.230164101690086</TD></TR>\n<TR><TD>109.128390</TD><TD>180.00020802648334</TD><TD>-36.22902343360014</TD></TR>\n</TABLEDATA>\n</DATA>\n</TABLE>\n</RESOURCE>\n</VOTABLE>\n'

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

def sqlquery(qry, fmt='csv', out=None, async_=False):
  qry = quote_plus(qry)
  dburl = '%s/query?sql=%s&ofmt=%s&out=%s&async=%s' % (QM_URL, qry, fmt, out, async_)
  dburl += "&profile=%s" % "default"
  headers = {'Content-Type': 'text/ascii', 'X-DL-AuthToken': TEST_TOKEN}
  r = requests.get(dburl, headers=headers)
  return r.content.decode('utf-8')

def adqlquery(qry, fmt='csv', out=None, async_=False):
  qry = quote_plus(qry)
  dburl = '%s/query?adql=%s&ofmt=%s&out=%s&async=%s' % (QM_URL, qry, fmt, out, async_)
  dburl += "&profile=%s" % "default"
  headers = {'Content-Type': 'text/ascii', 'X-DL-AuthToken': TEST_TOKEN}
  r = requests.get(dburl, headers=headers)
  return r.content.decode('utf-8')

def qstatus(jobid):
    headers = {'Content-Type': 'text/ascii', 'X-DL-AuthToken': TEST_TOKEN}
    dburl = '%s/status?jobid=%s' % (QM_URL, jobid)
    r = requests.get(dburl, headers=headers)
    return r.content.decode('utf-8')

def qresults(jobid):
    headers = {'Content-Type': 'text/ascii', 'X-DL-AuthToken': TEST_TOKEN}
    dburl = '%s/results?jobid=%s' % (QM_URL, jobid)
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
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQueryAdql))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQueryAdqlToVospaceCsv))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQueryAdqlToVospaceFits))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQueryAsync))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQueryStatus))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestQueryResults))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestListProfiles))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestListNotExists))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestListExists))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestSchema))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestDrop))
  return suite

class TestQuerySql(unittest.TestCase):

    def test_querysqlcsv(self):
        self.qry = qry
        # Run the list command
        res = queryClient.query(TEST_TOKEN,sql=self.qry,out=None,async_=False,fmt='csv')
        self.assertEqual(res,qryrescsv)

    def test_querysqlascii(self):
        self.qry = qry
        # Run the list command
        res = queryClient.query(TEST_TOKEN,sql=self.qry,out=None,async_=False,fmt='ascii')
        self.assertEqual(res,qryresascii)

    def test_querysqlvotable(self):
        self.qry = qry
        # Run the list command
        res = queryClient.query(TEST_TOKEN,sql=self.qry,out=None,async_=False,fmt='votable')
        self.assertEqual(res,qryresvotablesql)

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
        res = queryClient.query(TEST_TOKEN,sql=self.qry,out='vos://'+self.outfile,async_=False,fmt='csv')
        self.assertEqual(res,'OK')
        self.assertTrue(fileExists(self.outfile))
        # Get the results and compare
        data = get(self.outfile)
        self.assertEqual(data.decode('utf-8'),qryrescsv)

class TestQuerySqlToVospaceFits(unittest.TestCase):

    def setUp(self):
        self.outfile = 'qrytest.fits'
        # Make sure the output file doesn't exist
        rm(self.outfile)
        # Delete local file if it exists
        if os.path.exists(self.outfile):
          os.remove(self.outfile)

    def tearDown(self):
        # Delete temporary file
        rm(self.outfile)
        # Delete temporary local file
        if os.path.exists(self.outfile):
          os.remove(self.outfile)

    def test_querysqltovospacefits(self):
        self.qry = qry
        # Run the list command
        res = queryClient.query(TEST_TOKEN,sql=self.qry,out='vos://'+self.outfile,async_=False,fmt='fits')
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
        # Create the mydb table from a query
        res = queryClient.query(TEST_TOKEN,sql=self.qry,out='mydb://'+self.table,async_=False)
        self.assertEqual(res,'OK')
        res = list(self.table)
        self.assertNotEqual(res,'relation "'+self.table+'" not known')
        # Get the results and compare
        mydbqry = 'select * from mydb://'+self.table
        res = sqlquery(mydbqry,fmt='csv',async_=False,out=None)
        tab = np.loadtxt(StringIO(res),unpack=False,skiprows=1,delimiter=',')
        self.assertTrue(np.array_equiv(tab[:,0],qryresid.astype('float64')))
        self.assertTrue(np.allclose(tab[:,1],qryresra))
        self.assertTrue(np.allclose(tab[:,2],qryresdec))

class TestQueryAdql(unittest.TestCase):

    def test_queryadqlcsv(self):
        self.qry = qryadql
        # Run the query command
        res = queryClient.query(TEST_TOKEN,adql=self.qry,out=None,async_=False,fmt='csv')
        tab = np.loadtxt(StringIO(res),unpack=False,skiprows=1,delimiter=',')
        self.assertTrue(np.array_equiv(tab[:,0],qryresid.astype('float64')))
        self.assertTrue(np.allclose(tab[:,1],qryresra))
        self.assertTrue(np.allclose(tab[:,2],qryresdec))

    def test_queryadqlascii(self):
        self.qry = qryadql
        # Run the query command
        res = queryClient.query(TEST_TOKEN,adql=self.qry,out=None,async_=False,fmt='ascii')
        tab = np.loadtxt(StringIO(res),unpack=False,skiprows=1,delimiter='\t')
        self.assertTrue(np.array_equiv(tab[:,0],qryresid.astype('float64')))
        self.assertTrue(np.allclose(tab[:,1],qryresra))
        self.assertTrue(np.allclose(tab[:,2],qryresdec))

    def test_queryadqlvotable(self):
        self.qry = qryadql
        # Run the query command
        res = queryClient.query(TEST_TOKEN,adql=self.qry,out=None,async_=False,fmt='votable')
        # remove whitespace when comparing the strings
        self.assertEqual("".join(res.split()),"".join(qryresvotableadql.split()))

class TestQueryAdqlToVospaceCsv(unittest.TestCase):

    def setUp(self):
        self.outfile = 'adqrytest.csv'
        # Delete file if it already exists
        if fileExists(self.outfile):
          rm(self.outfile)

    def tearDown(self):
        # Delete temporary file
        if fileExists(self.outfile):
          rm(self.outfile)

    def test_queryadqltovospacecsv(self):
        self.qry = qryadql
        # Run the list command
        res = queryClient.query(TEST_TOKEN,adql=self.qry,out='vos://'+self.outfile,async_=False,fmt='csv')
        self.assertEqual(res,'OK')
        self.assertTrue(fileExists(self.outfile))
        # Get the results and compare
        data = get(self.outfile)
        tab = np.loadtxt(StringIO(data.decode('utf-8')),unpack=False,skiprows=1,delimiter=',')
        self.assertTrue(np.array_equiv(tab[:,0],qryresid.astype('float64')))
        self.assertTrue(np.allclose(tab[:,1],qryresra))
        self.assertTrue(np.allclose(tab[:,2],qryresdec))

class TestQueryAdqlToVospaceFits(unittest.TestCase):

    def setUp(self):
        self.outfile = 'adqrytest.fits'
        # Make sure the output file doesn't exist
        rm(self.outfile)
        # Delete local file if it exists
        if os.path.exists(self.outfile):
          os.remove(self.outfile)

    def tearDown(self):
        # Delete temporary file
        rm(self.outfile)
        # Delete temporary local file
        if os.path.exists(self.outfile):
          os.remove(self.outfile)

    def test_queryadqltovospacefits(self):
        self.qry = qryadql
        # Run the list command
        res = queryClient.query(TEST_TOKEN,adql=self.qry,out='vos://'+self.outfile,async_=False,fmt='fits')
        self.assertEqual(res,'OK')
        self.assertTrue(fileExists(self.outfile))
        # Get the results and compare
        res = get(self.outfile,self.outfile)
        outdata = Table.read(self.outfile,format='fits')
        self.assertEqual(outdata['id'].data.tostring(),qryresid.tostring())
        self.assertTrue(np.allclose(outdata['ra'].data, qryresra))
        self.assertTrue(np.allclose(outdata['dec'].data, qryresdec))

class TestQueryAsync(unittest.TestCase):

    def setUp(self):
        self.qry = qry

    def tearDown(self):
        pass

    def test_queryasync(self):
        # Run the query command
        jobid = queryClient.query(TEST_TOKEN,sql=self.qry,async_=True)
        if qstatus(jobid) != 'COMPLETED':
          time.sleep(2)
        # Get the results
        res = qresults(jobid)
        tab = np.loadtxt(StringIO(res),unpack=False,skiprows=1,delimiter=',')
        self.assertTrue(np.array_equiv(tab[:,0],qryresid.astype('float64')))
        self.assertTrue(np.allclose(tab[:,1],qryresra))
        self.assertTrue(np.allclose(tab[:,2],qryresdec))

class TestQueryStatus(unittest.TestCase):

    def setUp(self):
        self.qry = qry

    def tearDown(self):
        pass

    def test_querystatus(self):
        # Run the query command
        jobid = queryClient.query(TEST_TOKEN,sql=self.qry,async_=True)
        res = qstatus(jobid)
        self.assertIn(res,['QUEUED','EXECUTING','COMPLETED'])
        # Wait a little
        time.sleep(3)
        res = qstatus(jobid)
        self.assertEqual(res,'COMPLETED')

class TestQueryResults(unittest.TestCase):

    def setUp(self):
        self.qry = qry

    def tearDown(self):
        pass

    def test_queryresults(self):
        # Run the query
        jobid = sqlquery(self.qry,async_=True,out=None)
        if qstatus(jobid) != 'COMPLETED':
          time.sleep(2)
        # Get the results
        res = queryClient.results(TEST_TOKEN, jobid)
        tab = np.loadtxt(StringIO(res),unpack=False,skiprows=1,delimiter=',')
        self.assertTrue(np.array_equiv(tab[:,0],qryresid.astype('float64')))
        self.assertTrue(np.allclose(tab[:,1],qryresra))
        self.assertTrue(np.allclose(tab[:,2],qryresdec))

class TestListProfiles(unittest.TestCase):

    def test_listprofilesall(self):
        # Get the profiles
        res = queryClient.list_profiles(TEST_TOKEN)
        self.assertIn('NOAO',res)
        self.assertIn('IRSA',res)
        self.assertIn('Vizier',res)
        self.assertIn('SYMBAD',res)

    def test_listprofilesall(self):
        # Get the default profile
        res = queryClient.list_profiles(TEST_TOKEN,'default')
        self.assertEqual(type(res),dict)
        self.assertIn('accessURL',res.keys())
        self.assertIn('description',res.keys())
        self.assertIn('database',res.keys())
        self.assertIn('vosRoot',res.keys())
        self.assertIn('type',res.keys())

class TestSchema(unittest.TestCase):

    def test_schema(self):
        # Get the schemas
        res = queryClient.schema('','text','default')
        self.assertIn('gaia_dr1',res)
        self.assertIn('ivoa',res)
        self.assertIn('smash_dr1',res)

    def test_schemasmash(self):
        # Get the SMASH schema
        res = queryClient.schema('smash_dr1','text','default')
        self.assertIn('chip',res)
        self.assertIn('exposure',res)
        self.assertIn('object',res)
        self.assertIn('xmatch',res)

    def test_schemaobject(self):
        # Get the SMASH object table description
        res = queryClient.schema('smash_dr1.object','text','default')
        self.assertIn('id',res)
        self.assertIn('ra',res)
        self.assertIn('dec',res)
        self.assertIn('gmag',res)

    def test_schemaobjectra(self):
        # Get the SMASH object.ra description
        res = queryClient.schema('smash_dr1.object.ra','text','default')
        self.assertIn('column_name',res)
        self.assertIn('table_name',res)
        self.assertIn('smash_dr1.object',res)
        self.assertIn('datatype',res)
        self.assertIn('ucd',res)

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

