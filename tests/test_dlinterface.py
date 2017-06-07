#!/usr/bin/env python
# D. Nidever
#
# testdlinterface.py
# Python code to (unit) test the DL Interface operations

from dl.dlinterface import Dlinterface
import unittest

def suite():
  suite = unittest.TestSuite()
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestStringMethods))
  suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestHelp))
  return suite

class TestStringMethods(unittest.TestCase):

    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')

    def test_isupper(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)

class TestHelp(unittest.TestCase):

    def setUp(self):
        dl = Dlinterface()
        self.help = dl.help()
        self.dodo = dl.dodo()
        
    def test_toplevel(self):
        pass

    def test_dodo(self):
        pass

class TestLogin(unittest.TestCase):

    def setUp(self):
        dl = Dlinterface()
        self.login = dl.login()

    def test_login(self):
        self.login('dltest')
      
if __name__ == '__main__':
  suite = suite()
  unittest.TextTestRunner(verbosity = 2).run(suite)

