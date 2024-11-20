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

from pathlib import Path
from typing import Optional, List, Iterable
from tqdm import tqdm

import fiona
import xarray as xr
import pandas as pd
import geopandas as gpd


class PixCGpkgReader:
    """Class to read geopackage database from path
    """

    def __init__(self,
                 path: str | Iterable[str] | Path | Iterable[Path],
                 area_of_interest: Optional[gpd.GeoDataFrame] = None
                 ):
        super().__init__(path, area_of_interest=area_of_interest)
        self._gdf_data: Optional[gpd.GeoDataFrame] = None
        self.layers: list[str]  = fiona.listlayers(self.path)

    @property
    def data(self) ->  xr.Dataset:
        return self._gdf_data.to_xarray()

    def to_geodataframe(
        self,
    ) -> gpd.GeoDataFrame:
        """

        Returns:
            gpd.GeoDataFrame: a geodataframe with information from file
        """
        return self._gdf_data

    def read_single_layer(self, layername: str) -> gpd.GeoDataFrame:
        """reads a single layer of geopackage database

        Args:
            layername (str): name of the database, 
            from list accessible with self.layers

        Returns:
            gpd.GeoDataFrame: geodataframe containing data read from layer
        """
        layer_data = gpd.read_file(
            self.path,
            engine="pyogrio",
            use_arrow=True,
            layer=layername,
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
        """reads all layers, or subset of layers, from geopackage database

        Args:
            layers (Optional[List[str]] | None, optional): \
                list of layers accessible with self.layers. Defaults to None.
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
