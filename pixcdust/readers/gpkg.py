from dataclasses import dataclass
from tqdm import tqdm

import fiona
import pandas as pd
import geopandas as gpd


@dataclass
class PixCGpkgReader:
    path: str
    layers: list[str] = None
    area_of_interest: gpd.GeoDataFrame = None

    def __post_init__(self):
        self.layers = fiona.listlayers(self.path)

    def load_layer(self, layername: str) -> gpd.GeoDataFrame:
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

    def load_all_layers(self) -> gpd.GeoDataFrame:
        joined_data = None

        for layer in tqdm(self.layers):

            layer_data = self.load_layer(
                layer,
            )

            if joined_data is None:
                joined_data = layer_data
            else:
                joined_data = gpd.GeoDataFrame(
                    pd.concat([joined_data, layer_data], ignore_index=True)
                )
    
            del layer_data

        return joined_data

