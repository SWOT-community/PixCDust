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


"""This module reads the zarr archives created by the converters"""

from dataclasses import dataclass
from typing import Optional, Tuple

import datetime
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
        date_interval: Optional[
            Tuple[datetime.datetime, datetime.datetime]
            ] | None = None,
            ):

        collection: zcollection.Dataset = zcollection.open_collection(
            self.path,
            mode='r',
        )

        if date_interval:
            date_min = date_interval[0]
            date_max = date_interval[1]
            self.data = collection.load(
                filters=lambda keys: date_min <= datetime.datetime(
                    keys['year'], keys['month'], keys['day'],
                    keys['hour'], keys['minute'], keys['second'],
                ) <= date_max
            )
        else:
            self.data = collection.load()

    def to_xarray(self):
        if self.data is None:
            return xr.Dataset()
        return self.data.to_xarray()

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
        if self.data is None:
            return gpd.GeoDataFrame()

        cst = PixCNcSimpleConstants()

        return geoxarray_to_geodataframe(
            self.to_xarray(),
            long_name=cst.default_long_name,
            lat_name=cst.default_lat_name,
            **kwargs,
            )
