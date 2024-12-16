"""Geopackage converters."""

import os
from pathlib import Path
from typing import Optional, Union
from dataclasses import dataclass

from tqdm import tqdm
import fiona
import geopandas as gpd

from pixcdust.converters.core import PixCConverterWSE, GeoLayerH3Projecter
from pixcdust.readers.netcdf import PixCNcSimpleReader
from pixcdust.readers.zarr import PixCZarrReader
from pixcdust.readers.gpkg import PixCGpkgReader


class PixCNc2GpkgConverter(PixCConverterWSE):
    """Converter from official SWOT Pixel Cloud Netcdf to a Geopackage database.

    Attributes:
        path_in: List of path of files to convert.
        variables: Optionally only read these variables.
        area_of_interest: Optionally only read points in area_of_interest.

    """

    def database_from_nc(self, path_out: str | Path, mode: str = "w", compute_wse: bool = True) \
            -> None:
        path_out = str(path_out)
        if compute_wse:
            self._append_wse_vars()
        for path in tqdm(self.path_in):
            ncsimple = PixCNcSimpleReader(
                path,
                variables= self.variables,
                area_of_interest=self.area_of_interest,
            )

            # computing layer_name
            _, dt_time_start, cycle_number, pass_number, tile_number, swath_side = (
                ncsimple.extract_info_from_nc_attrs(path)
            )
            time_start = dt_time_start.strftime('%Y%m%d')

            layer_name = f"{time_start}_{cycle_number}_\
{pass_number}_{tile_number}{swath_side}"

            # cheking if output file and layer already exist
            if os.path.exists(path_out) and mode == "w":
                if layer_name in fiona.listlayers(path_out):
                    tqdm.write(
                        f"skipping layer {layer_name} \
                            (already in geopackage {path_out})"
                    )
                    continue
            # converting data from xarray to geodataframe
            ncsimple.open_dataset()
            gdf = ncsimple.to_geodataframe(
            )

            if gdf.size == 0:
                tqdm.write(
                    f"--File {path} combined with area of interest\
                        returned empty. Skipping it"
                )
                continue

            if compute_wse:
                self._compute_wse(gdf)
            # writing pixc layer in output file, geopackage
            gdf.to_file(path_out, layer=layer_name, driver="GPKG")


@dataclass
class GpkgH3Projecter:
    """Converter from a Gpkg pixelcloud to a Gpkg H3 projection.

    Attributes:
        path: Gpkg pixelcloud to convert.
        variable: FIXME
        h3_res: esolotion of the h3 projection.
        conditions: Optional limits on points converted.
        h3_layer_pattern: Postfix of output layers.
        path_out: Output path of the convertion.

    """

    path: str
    variable: str
    h3_res: int
    conditions: Optional[dict[str,dict[str, Union[str, float]]]] = None
    h3_layer_pattern: str = '_h3'
    path_out: Optional[str] = None
    # database: PixCGpkgReader

    def __post_init__(self) -> None:
        self.database = PixCGpkgReader(self.path)
        self.database.layers = [
            layer for layer in fiona.listlayers(self.path)
            if not layer.endswith(self.h3_layer_pattern)
            ]

        if self.path_out is None:
            self.path_out = self.path

    def compute_layers(self) -> None:
        """Convert to an H3 projection and write it as Gpkg in `self.path_out`"""
        for layer in tqdm(self.database.layers, desc="Layers"):
            gdf = self.database.read_single_layer(layer)
            h3_gdf = self._compute_layer(gdf)

            layername_out = f"{layer}_{self.variable}_\
{self.h3_res}_{self.h3_layer_pattern}"

            h3_gdf.to_file(self.path_out, layer=layername_out, driver="GPKG")
            # tqdm.write(layername_out)
            # lancer write avec le bon nom

    def _compute_layer(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Convert to an H3 projection a single layer.

        Args:
            gdf: Layer to project.

        Returns:
            Converted layer.
        """
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
    """Converter from Pixel Cloud zcollection to Geopackage database
    Attributes:
        path: Gpkg pixelcloud to convert.
    """
    path: str
    data: gpd.GeoDataFrame = None

    def __post_init__(self) -> None:
        self.__collection = PixCZarrReader(self.path)
        self.__collection.read()

    def convert(self, path_out: str) -> None:
        """Convert and write to path_out."""
        self.data = self.__collection.to_geodataframe()
        self.data.to_file(path_out, driver="GPKG")
