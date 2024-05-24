"""This module reads the zarr archives created by the converters"""

from dataclasses import dataclass

import xarray as xr
import geopandas as gpd
import zcollection

from pixcdust.readers.netcdf import PixCNcSimpleConstants
from pixcdust.converters.geo_utils import geoxarray_to_geodataframe


@dataclass
class PixCZarrReader:
    path: str
    variables: list[str] = None
    data: xr.Dataset = None

    def read(
        self,
        cycle_number: int = None,
        pass_number: int = None,
        tile_number: int = None,
            ):
        collection: zcollection.Dataset = zcollection.open_collection(
            self.path,
            mode='r',
        )
        self.data = collection.load()
        # filters=lambda keys: keys['month'] == 6 and keys['year'] == 2000)

    def __repr__(self):
        return self.data

    def to_xarray(self):
        return self.data.to_xarray()

    def to_dataframe(self):
        return

    def to_geodataframe(
        self,
        **kwargs,
            ) -> gpd.GeoDataFrame:
        """_summary_

        Args:
            crs (str | int, optional): Coordinate Reference System.\
                Defaults to 4326.
            area_of_interest (gpd.GeoDataFrame, optional): a geodataframe\
                containing polygons of interest where data will be restricted.\
                Defaults to None.

        Returns:
            gpd.GeoDataFrame: a geodataframe with information from file
        """

        cst = PixCNcSimpleConstants()

        return geoxarray_to_geodataframe(
            self.to_xarray(),
            long_name=cst.default_long_name,
            lat_name=cst.default_lat_name,
            **kwargs,
            )
