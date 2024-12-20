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
"""Converted Pixcdust zarr database Reader."""

from typing import Optional, Tuple

import datetime
import xarray as xr
import zcollection

from pixcdust.readers.base_reader import BaseReader


class ZarrReader(BaseReader):
    """Zarr pixcdust database reader.

    Read a database from a Zarr database (folder).
    You can then request a xr.Dataset, pd.DataFrame or gpd.GeoDataFrame
    view of the database.

    Attributes:
        path: Path to read.
        variables: Optionally only read these variables.
        area_of_interest: Optionally only read points in area_of_interest.
        MULTI_FILE_SUPPORT: False, only support one file.
    """


    def read(
        self,
        date_interval: Optional[
            Tuple[datetime.datetime, datetime.datetime]
            ] | None = None,
            ) -> None:
        """Load a zarr database.
        You can then access from data or with methods like
        to_xarray, to_dataframe or to_geodataframe.

        Args:
            date_interval: Optional date filter on the database read.
                Only load data dated within the interval.
        """

        collection = zcollection.open_collection(
            self.path,
            mode='r',
        )

        if date_interval:
            date_min = date_interval[0]
            date_max = date_interval[1]
            data_z = collection.load(
                filters=lambda keys: date_min <= datetime.datetime(
                    keys['year'], keys['month'], keys['day'],
                    keys['hour'], keys['minute'], keys['second'],
                ) <= date_max
            )
        else:
            data_z = collection.load()
        if data_z is None:
            self.data = xr.Dataset()
        else:
            self.data = data_z.to_xarray()
