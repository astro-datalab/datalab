# #!/usr/bin/python -u
# 2016/01/27 - v0.1: Original version
#
# testclient.py
# Python code to (unit) test datalab operations

from dl import dltasks as dl
import unittest

def suite():
  suite = unittest.TestSuite()
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(ListTestCase))
  return suite


class DataLabTestCase(unittest.TestCase):
  """
  Base DataLab class
  """
  @classmethod
  def setUpClass(cls):
    datalab = dl.DataLab()
    cls.dl = datalab
      

class OptionTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.option = dl.Option(self.dl)


class TaskTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.task = dl.Task(self.dl)


class LoginTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.login = dl.Login(self.dl)



class LogoutTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.logout = dl.Logout(self.dl)


class AddCapabilityTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.addCapability = dl.AddCapability(self.dl)


class ListCapabilityTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.listCapability = dl.ListCapability(self.dl)


class QueryTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.query = dl.Query(self.dl)

    # datalab query -adql="select id, ra_j2000, dec_j2000, g, g_i, i_z from lsdr2.stars" -ofmt="csv" -out="vos://lsdr2.csv"
    # datalab query -sql=complex.sql -async=true -ofmt='csv'


class LaunchJobTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.launchjob = dl.LaunchJob(self.dl)


class MountvofsTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.mountvofs = dl.Mountvofs(self.dl)


class PutTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.put = dl.Put(self.dl)
  

class GetTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.get = dl.Get(self.dl)


class MoveTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.move = dl.Move(self.dl)


class CopyTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.copy = dl.Copy(self.dl)


class DeleteTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.delete = dl.Delete(self.dl)


class LinkTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.link = dl.Link(self.dl)


class ListTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.list = dl.List(self.dl)

  def test_raw(self):
    """
    Test getting a listing in XML format
    """
    resp = self.list.run()
    #assert 

  def test_csv(self):
    """
    Test getting a listing in CSV format
    """
    pass

  def test_json(self):
    """
    Test getting a listing in JSON format
    """
    pass

  def test_unsupported_format(self):
    """
    Test getting a listing in an unsupported format
    """
    pass

  def test_invalid_from(self):
    """
    Test getting a listing from an invalid location
    """
    pass

    

class TagTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.tag = dl.Tag(self.dl)


class MkDirTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.mkdir = dl.MkDir(self.dl)


class RmDirTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.rmdir = dl.RmDir(self.dl)


class ResolveTestCase(DataLabTestCase):

  def setUp(self):
    """
    Initialize the test case
    """
    self.resolve = dl.Resolve(self.dl)
        

if __name__ == '__main__':
  suite = suite()
  unittest.TextTestRunner(verbosity = 2).run(suite)

