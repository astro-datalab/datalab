import unittest
from dl.helpers import utils
from astropy.coordinates.sky_coordinate import SkyCoord
import numpy

class TestHelpersUtilsResolve(unittest.TestCase):

    def setUp(self):
        self.res = utils.resolve('M31')

    def test_instance(self):
        """Should return SkyCoord object"""
        self.assertIsInstance(self.res,SkyCoord)

    def test_ra(self):
        """Should return 10.684793"""
        self.assertEqual(self.res.ra.value,10.684793)

    def test_dec(self):
        """Should return 41.269065"""
        self.assertEqual(self.res.dec.value,41.269065)


class TestHelpersUtilsConvert(unittest.TestCase):

    def setUp(self):
        self.csvtable =\
"""ra,dec,mag_auto_i
326.94607100000002,-39.692920999999998,23.168003
326.95835299999999,-39.701720000000002,23.581308
326.95537100000001,-39.697189999999999,21.966629"""

    def test_string(self):
        """Result of convert() to string should be identical to the input string"""
        res = utils.convert(self.csvtable,'string')
        self.assertEqual(res,self.csvtable)

    def test_is_array(self):
        """Result of convert() to array should be a numpy array"""
        res = utils.convert(self.csvtable,'array')
        self.assertIsInstance(res,numpy.ndarray)
        
    def test_array_value(self):
        """Result of convert() to array should have res[0,2] == 23.168003"""
        res = utils.convert(self.csvtable,'array')
        self.assertEqual(res[0,2],float(self.csvtable[57:66]))

    def test_is_structarray(self):
        """Result of convert() to structarray should be a numpy array"""
        res = utils.convert(self.csvtable,'structarray')
        self.assertIsInstance(res,numpy.ndarray)

    def test_structarray_value(self):
        """Result of convert() to structarray should have res['mag_auto_i'][0] == 23.168003"""
        res = utils.convert(self.csvtable,'structarray')
        self.assertEqual(res['mag_auto_i'][0],float(self.csvtable[57:66]))
        
if __name__ == '__main()__':
    unittest.main()
