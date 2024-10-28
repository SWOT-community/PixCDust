#
# Copyright (C) 2024 Centre National d'Etudes Spatiales (CNES)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#

import unittest

from pixcdust.tests.mock import mock_xarray
from pixcdust.converters.gpkg import PixCNc2GpkgConverter
from pixcdust.converters.zarr import PixCNc2ZarrConverter


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
        """function for testing the conversion from 
        netcdf to zarr with zcollection
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
