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
"""Converted Pixcdust GeoPackage Reader."""

from pathlib import Path
from typing import Optional, List
from tqdm import tqdm

import fiona
import xarray as xr
import pandas as pd
import geopandas as gpd

from pixcdust.readers.base_reader import BaseReader


class GpkgReader(BaseReader):
    """GeoPackage pixcdust database reader.

    Read a database from a GeoPackage file .
    You can then request a xr.Dataset, pd.DataFrame or gpd.GeoDataFrame
    view of the database.

    Attributes:
        path: Path to read.
        variables: Not supported.
        area_of_interest: Optionally only read points in area_of_interest.
        MULTI_FILE_SUPPORT: False, only support one file.
    """

    def __init__(self,
                 path: str | Path,
                 area_of_interest: Optional[gpd.GeoDataFrame] = None
                 ):
        """Gpkg pixcdust database reader configuration.
        Read the list of layers from path.

        Args:
            path: Path of the file to read.
            area_of_interest: Optionally only read points in area_of_interest.
        """
        super().__init__(path, area_of_interest=area_of_interest)
        self._gdf_data: Optional[gpd.GeoDataFrame] = None
        self.layers: list[str]  = fiona.listlayers(self.path)

    @property
    def data(self) ->  xr.Dataset:
        return self._gdf_data.to_xarray()

    @data.setter
    def data(self, obj: xr.Dataset) -> None:
        raise NotImplementedError("PixCGpkgReader internal data representation is a GeoDataFrame.")


    def to_geodataframe(
        self,
    ) -> gpd.GeoDataFrame:
        return self._gdf_data

    def read_single_layer(self, layer: str) -> gpd.GeoDataFrame:
        """Read and return a single layer of geopackage database.

        Don't load the read data into the class (can't be then converted by the reader).
        Use read for more advanced usage.

        Args:
            layer : name of the geodataframe layer to read. Must be in self.layers

        Returns:
            Geodataframe containing data read from layer
        """
        layer_data = gpd.read_file(
            self.path,
            engine="pyogrio",
            use_arrow=True,
            layer=layer,
        )

        if self.area_of_interest is not None:
            layer_data = gpd.sjoin(
                left_df=layer_data,
                right_df=self.area_of_interest,
                how="inner",
                predicate="within",
            )

        return layer_data

    def read(self, layers: Optional[List[str]] = None) -> None:
        """Load all layers, or subset of layers, from geopackage database.
        You can then access from data or with methods like
        to_xarray, to_dataframe or to_geodataframe.

        Args:
            layers: Optional list of layers to load. Default to all.
        """

        self._gdf_data = None

        if layers is None:
            layers = self.layers

        for layer in tqdm(layers):

            layer_data = self.read_single_layer(
                layer,
            )

            if self._gdf_data is None:
                self._gdf_data = layer_data
            else:
                self._gdf_data = gpd.GeoDataFrame(
                    pd.concat([self._gdf_data, layer_data], ignore_index=True)
                )

            del layer_data
