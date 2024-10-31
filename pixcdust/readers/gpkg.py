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


from dataclasses import dataclass
from typing import Optional, List
from tqdm import tqdm

import fiona
import pandas as pd
import geopandas as gpd


@dataclass
class PixCGpkgReader:
    """Class to read geopackage database from path
    """
    path: str
    # layers: list[str]
    area_of_interest: gpd.GeoDataFrame = None
    data: gpd.GeoDataFrame = None

    def __post_init__(self) -> None:
        self.layers: list[str]  = fiona.listlayers(self.path)

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

    def read(self, layers: Optional[List[str]] | None = None) -> None:
        """reads all layers, or subset of layers, from geopackage database

        Args:
            layers (Optional[List[str]] | None, optional): \
                list of layers accessible with self.layers. Defaults to None.
        """

        self.data = None

        if layers is None:
            layers = self.layers

        for layer in tqdm(layers):

            layer_data = self.read_single_layer(
                layer,
            )

            if self.data is None:
                self.data = layer_data
            else:
                self.data = gpd.GeoDataFrame(
                    pd.concat([self.data, layer_data], ignore_index=True)
                )

            del layer_data
