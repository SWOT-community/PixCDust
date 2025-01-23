import unittest

from pixcdust.tests.mock import mock_xarray
from pixcdust.converters.gpkg import Nc2GpkgConverter
from pixcdust.converters.zarr import Nc2ZarrConverter


class TestConverters(unittest.TestCase):
    """Class for testing Converters, to be implemented
    """

    def setUp(self):
        """function to set up the test environment
        """
        self.list_vars = [
            "height", "sig0", "classification", "geoid", "cross_track"
        ]
        self.data = mock_xarray()

    def test_convert_ds_to_gpkg(self):
        """function for testing the conversion to geopackage
        """
        pixc = Nc2GpkgConverter(
            "/tmp",
            variables=self.list_vars,
        )
        # forcing data with mock
        pixc.data = self.data

        self.assertIsInstance(pixc, Nc2GpkgConverter)
        # TODO: add relevant tests

    def test_convert_ds_to_zarr(self):
        """function for testing the conversion from 
        netcdf to zarr with zcollection
        """

        pixc = Nc2ZarrConverter(
            "/tmp",
            variables=self.list_vars,
        )
        # forcing data with mock
        pixc.data = self.data

        self.assertIsInstance(pixc, Nc2ZarrConverter)
        # TODO: add relevant tests


if __name__ == "__main__":
    unittest.main()
