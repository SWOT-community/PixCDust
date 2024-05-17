import unittest

import os
from glob import glob
import geopandas as gpd

from pixcdust.converters.gpkg import PixCNc2GpkgConverter
from pixcdust.converters.zarr import PixCNc2ZarrConverter


class TestConverter(unittest.TestCase):

    def setUp(self):
        self.dir_swot = "/home/hysope2/STUDIES/SWOT_Sudan/DATA/Raw_Data"
        self.files_swot_pxc = os.path.join(self.dir_swot, "SWOT*.nc")
        self.paths = sorted(glob(self.files_swot_pxc))
        self.list_vars = ["height", "sig0", "classification", "geoid", "cross_track"]

    def test_convert_nc_to_gpkg(self):

        restrict = "/home/hysope2/STUDIES/SWOT_Kakhovka/DATA/kakhovka.gpkg"
        aoi = gpd.read_file(restrict)

        pixc = PixCNc2GpkgConverter(
            self.paths,
            "foo",
            variables=self.list_vars,
            area_of_interest=aoi,
        )
        # pixc.database_from_mf_nc()

        self.assertIsInstance(aoi, gpd.GeoDataFrame)
        self.assertIsInstance(pixc, PixCNc2GpkgConverter)

    def test_convert_nc_to_zarr(self):

        pixc = PixCNc2ZarrConverter(
            self.paths,
            "foo",
            variables=self.list_vars,
        )
        pixc.database_from_mf_nc()

        self.assertIsInstance(pixc, PixCNc2ZarrConverter)


if __name__ == "__main__":
    unittest.main()
