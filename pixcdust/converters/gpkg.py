import os
from tqdm import tqdm
from dataclasses import dataclass

import fiona
import geopandas as gpd

from pixcdust.converters.core import PixCConverter
from pixcdust.readers.netcdf import PixCNcSimpleReader
from pixcdust.readers.zarr import PixCZarrReader


class PixCNc2GpkgConverter(PixCConverter):
    """Class for converting Pixel Cloud files to Geopackage database

    """

    def database_from_nc(self, layer_name: str = None):
        """function to create a database from a single or\
            multiple netcdf PIXC files

        Args:
            layer_name (str, optional): _description_. Defaults to None.
        """

        for path in tqdm(self.path_in):
            ncsimple = PixCNcSimpleReader(path, self.variables)
            if layer_name is None:
                time_start, _, cycle_number, pass_number, tile_number = (
                    ncsimple.extract_info_from_nc_attrs(path)
                )

                layer_name = f"{cycle_number}_{pass_number}_\
                    {tile_number}_{time_start}"

            # cheking if output file and layer already exist
            if os.path.exists(self.path_out) and self.mode == "w":
                if layer_name in fiona.listlayers(self.path_out):
                    tqdm.write(
                        f"skipping layer {layer_name} \
                            (already in geopackage {self.path_out})"
                    )
                    continue
            # converting data from xarray to geodataframe
            ncsimple.open_dataset()
            gdf = ncsimple.to_geodataframe(
                area_of_interest=self.area_of_interest
            )

            if gdf.size == 0:
                tqdm.write(
                    f"--File {path} combined with area of interest\
                        returned empty. Skipping it"
                )
                continue

            # writing pixc layer in output file, geopackage
            gdf.to_file(self.path_out, layer=layer_name, driver="GPKG")
            tqdm.write(f"--File{path} processed")


@dataclass
class PixCZarr2GpkgConverter:
    """Class for converting Pixel Cloud zcollection to Geopackage database

    """
    path: str
    data: gpd.GeoDataFrame = None

    def __post_init__(self):
        self.__collection = PixCZarrReader(self.path)
        self.__collection.read()

    def convert(self, path_out: str):
        self.data = self.__collection.to_geodataframe()
        self.data.to_file(path_out, driver="GPKG")

