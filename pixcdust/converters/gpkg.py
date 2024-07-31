import os
from tqdm import tqdm
from dataclasses import dataclass

import fiona
import geopandas as gpd

from pixcdust.converters.core import PixCConverter
from pixcdust.readers.netcdf import PixCNcSimpleReader
from pixcdust.readers.zarr import PixCZarrReader
from pixcdust.readers.gpkg import PixCGpkgReader


class PixCNc2GpkgConverter(PixCConverter):
    """Class for converting Pixel Cloud files to Geopackage database

    """

    def database_from_nc(self):
        """function to create a geopackage database from a single or\
            multiple netcdf PIXC files

        """
        for path in tqdm(self.path_in):
            ncsimple = PixCNcSimpleReader(path, self.variables)

            # computing layer_name
            _, dt_time_start, cycle_number, pass_number, tile_number, swath_side = (
                ncsimple.extract_info_from_nc_attrs(path)
            )
            time_start = dt_time_start.strftime('%Y%m%d')

            layer_name = f"{time_start}_{cycle_number}_\
{pass_number}_{tile_number}{swath_side}"


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
            gdf = ncsimple.xvec.to_geodataframe(
            )

            if gdf.size == 0:
                tqdm.write(
                    f"--File {path} combined with area of interest\
                        returned empty. Skipping it"
                )
                continue

            if self._wse:
                gdf[self._get_name_wse_var()] = \
                    gdf[self._get_vars_wse_computation()[0]] -\
                    gdf[self._get_vars_wse_computation()[1]]
            # writing pixc layer in output file, geopackage
            gdf.to_file(self.path_out, layer=layer_name, driver="GPKG")


@dataclass
class GpkgH3Projecter:
    from pixcdust.converters.core import GeoLayerH3Projecter
    
    path: str
    variable: str
    h3_res: int
    conditions: dict = None
    h3_layer_pattern: str = '_h3'
    path_out: str = None
    database: PixCGpkgReader = None

    def __post_init__(self):
        self.database = PixCGpkgReader(self.path)
        self.database.layers = [
            layer for layer in fiona.listlayers(self.path)
            if not layer.endswith(self.h3_layer_pattern)
            ]

        if self.path_out is None:
            self.path_out = self.path

    def compute_layers(self):
        for layer in tqdm(self.database.layers, desc="Layers"):
            gdf = self.database.read_single_layer(layer)
            h3_gdf = self._compute_layer(gdf)

            layername_out = f"{layer}_{self.variable}_\
{self.h3_res}_{self.h3_layer_pattern}"

            h3_gdf.to_file(self.path_out, layer=layername_out, driver="GPKG")
            # tqdm.write(layername_out)
            # lancer write avec le bon nom

    def _compute_layer(self, gdf):
        geolayer = GeoLayerH3Projecter(
            gdf,
            self.variable,
            self.h3_res,
        )
        if self.conditions:
            geolayer.filter_variable(self.conditions)
        geolayer.compute_h3_layer()

        return geolayer.data


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
