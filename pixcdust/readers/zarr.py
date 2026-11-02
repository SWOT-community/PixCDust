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

import datetime

import numpy as np
import xarray as xr

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
        date_interval: tuple[datetime.datetime, datetime.datetime] | None = None,
    ) -> None:
        """Load a zarr database.
        You can then access from data or with methods like
        to_xarray, to_dataframe or to_geodataframe.

        Args:
            date_interval: Optional date filter on the database read.
                Only load data dated within the interval.
        """

        self.data = xr.open_zarr(
            self.path,
        )

        if date_interval is not None:
            date_min = date_interval[0]
            date_max = date_interval[1]

            if "time" not in self.data:
                self.data.close()
                self.data = xr.Dataset()
                raise ValueError(
                    "Cannot filter the Zarr dataset by date: "
                    "no 'time' coordinate is available."
                )

            # Convert timezone-aware bounds to local naive timestamps,
            # matching the local-time interpretation of the old reader.
            if date_min.tzinfo is not None:
                date_min = date_min.astimezone().replace(tzinfo=None)

            if date_max.tzinfo is not None:
                date_max = date_max.astimezone().replace(tzinfo=None)

            date_min = np.datetime64(date_min.replace(tzinfo=None), "ns")
            date_max = np.datetime64(date_max.replace(tzinfo=None), "ns")

            mask = (self.data["time"] >= date_min) & (self.data["time"] <= date_max)
            mask = mask.compute()

            self.data = self.data.isel(points=mask)
