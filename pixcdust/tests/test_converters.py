import unittest

import os
from glob import glob
import geopandas as gpd


from pixcdust.tests.mock import mock_xarray
from pixcdust.converters.gpkg import PixCNc2GpkgConverter
from pixcdust.converters.zarr import PixCNc2ZarrConverter


class TestConverters(unittest.TestCase):
    """Class for testing Converters
    """

    def setUp(self):
        """function to set up the test environment
        """
        self.list_vars = ["height", "sig0", "classification", "geoid", "cross_track"]
        self.data = mock_xarray()

    def test_convert_ds_to_gpkg(self):
        """function for testing the conversion to geopackage
        """
        pixc = PixCNc2GpkgConverter(
            "/tmp",
            "foo",
            variables=self.list_vars,
        )
        # forcing data with mock
        pixc.data = self.data

        self.assertIsInstance(pixc, PixCNc2GpkgConverter)
        # TODO: add relevant tests

    def test_convert_ds_to_zarr(self):
        """function for testing the conversion from netcdf to zarr with zcollection
        """

        pixc = PixCNc2ZarrConverter(
            "/tmp",
            "foo",
            variables=self.list_vars,
        )
        # forcing data with mock
        pixc.data = self.data

        self.assertIsInstance(pixc, PixCNc2ZarrConverter)
        # TODO: add relevant tests


if __name__ == "__main__":
    unittest.main()
