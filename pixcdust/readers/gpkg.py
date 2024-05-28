from dataclasses import dataclass

import fiona
import geopandas as gpd


@dataclass
class PixCGpkgReader:
    path: str
    layers: list[str] = None

    def __post_init__(self):
        self.layers = fiona.listlayers(self.path)

    def get_layer(self, layername):
        return gpd.read_file(
            self.path,
            engine='pyogrio',
            use_arrow=True,
            layer=layername,
        )
