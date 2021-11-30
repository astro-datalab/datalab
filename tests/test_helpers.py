__author__ = 'Robert Nikutta <robert.nikutta@noirlab.edu>, NOAO Data Lab <datalab@noirlab.edu>'
__version__ = '20180131' #yyyymmdd

import unittest
from dl.helpers import utils
from astropy.coordinates.sky_coordinate import SkyCoord
import astropy
import numpy
import pandas


skipMock = False
try:
    from unittest.mock import call, patch, mock_open, MagicMock, DEFAULT
except ImportError:
    # Python 2
    skipMock = True


class TestHelpersUtilsVOSpace(unittest.TestCase):

    def setUp(self):
        pass

    @unittest.skipIf(skipMock, "Skipping test that requires unittest.mock.")
    def test_path(self):
        """Test case where name is a string.
        """
        with patch('dl.helpers.utils.get_readable_fileobj') as get:
            with utils.vospace_readable_fileobj('/foo/bar', 'fake.token') as fileobj:
                    foo = fileobj.read()
        get.assert_called_with('/foo/bar')
        with patch('dl.helpers.utils.get_readable_fileobj') as get:
            with vospace_readable_fileobj('/foo/bar', 'fake.token', 
                                          encoding='binary') as fileobj:
                foo = fileobj.read()
        get.assert_called_with('/foo/bar', encoding='binary')


    @unittest.skipIf(skipMock, "Skipping test that requires unittest.mock.")
    def test_path(self):
        """Test case where name is a VOSpace path.
        """
        with patch('dl.helpers.utils.get_readable_fileobj') as get:
            with patch('dl.helpers.utils.storeClient') as store:
                store.get.return_value = b'Hello'
                with vospace_readable_fileobj('/foo/bar', 'fake.token') as fileobj:
                    foo = fileobj.read()
        store.get.assert_called_with('fake.token', fr='vos://foo/bar', to='')


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

        # minimal csv table example
        self.csvtable =\
"""ra,dec,mag_auto_i
326.94607,-39.69292,23.16800
326.95835,-39.70172,23.58130
326.95537,-39.69718,21.96662"""

        # minimal votable example
        self.votable =\
"""<?xml version="1.0" encoding="UTF-8"?>
<VOTABLE xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xsi:noNamespaceSchemaLocation="xmlns:http://www.ivoa.net/xml/VOTable-1.2.xsd" version="1.2">
<RESOURCE type="results">
<TABLE>
<FIELD ID="ra" name="ra" datatype="double" unit="degrees"></FIELD>
<FIELD ID="dec" name="dec" datatype="double" unit="degrees"></FIELD>
<FIELD ID="mag_auto_i" name="mag_auto_i" datatype="float" unit="mag"></FIELD>
<DATA>
<TABLEDATA>
<TR><TD>326.94607</TD><TD>-39.69292</TD><TD>23.16800</TD></TR>
<TR><TD>326.95835</TD><TD>-39.70172</TD><TD>23.58130</TD></TR>
<TR><TD>326.95537</TD><TD>-39.69718</TD><TD>21.96662</TD></TR>
</TABLEDATA>
</DATA>
</TABLE>
</RESOURCE>
</VOTABLE>"""

        # testing int(floats) for accuracy reasons
        self.intval = 23

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
        self.assertEqual(int(res[0,2]),self.intval)

    def test_is_structarray(self):
        """Result of convert() to structarray should be a numpy array"""
        res = utils.convert(self.csvtable,'structarray')
        self.assertIsInstance(res,numpy.ndarray)

    def test_structarray_value(self):
        """Result of convert() to structarray should have res['mag_auto_i'][0] == 23.168003"""
        res = utils.convert(self.csvtable,'structarray')
        self.assertEqual(int(res['mag_auto_i'][0]),self.intval)

    def test_is_pandas_dataframe(self):
        """Result of convert() to pandas should be a numpy array"""
        res = utils.convert(self.csvtable,'pandas')
        self.assertIsInstance(res,pandas.DataFrame)

    def test_pandas_dataframe_value(self):
        """Result of convert() to pandas should have res['mag_auto_i'][0] == 23.168003"""
        res = utils.convert(self.csvtable,'pandas')
        self.assertEqual(int(res['mag_auto_i'][0]),self.intval)

    def test_is_astropy_table(self):
        """Result of convert() to table should be an astropy table.Table"""
        res = utils.convert(self.csvtable,'table')
        self.assertIsInstance(res,astropy.table.table.Table)

    def test_astropy_table_value(self):
        """Result of convert() to table should have res['mag_auto_i'][0] == 23.168003"""
        res = utils.convert(self.csvtable,'table')
        self.assertEqual(int(res['mag_auto_i'][0]),self.intval)

    def test_is_votable(self):
        """Result of convert() to votable should be a votable"""
        res = utils.convert(self.votable,'votable')
        self.assertIsInstance(res,astropy.io.votable.tree.Table)

    def test_votable_value(self):
        """Result of convert() to votable should have res['mag_auto_i'][0] == 23.168003"""
        res = utils.convert(self.votable,'votable')
        self.assertEqual(int(res.array['mag_auto_i'][0]),self.intval)

if __name__ == '__main()__':
    unittest.main()
