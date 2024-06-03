from dataclasses import dataclass
from typing import Optional, List
from tqdm import tqdm

import fiona
import pandas as pd
import geopandas as gpd


@dataclass
class PixCGpkgReader:
    path: str
    layers: list[str] = None
    area_of_interest: gpd.GeoDataFrame = None
    data: gpd.GeoDataFrame = None

    def __post_init__(self):
        self.layers = fiona.listlayers(self.path)

    def read_single_layer(self, layername: str) -> gpd.GeoDataFrame:
        layer_data = gpd.read_file(
            self.path,
            engine='pyogrio',
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

    def read(
        self,
        layers: Optional[List[str]] | None = None
            ):

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

