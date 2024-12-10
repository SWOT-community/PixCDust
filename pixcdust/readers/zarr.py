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

from typing import Optional, Tuple

import datetime
import xarray as xr
import zcollection

from pixcdust.readers.base_reader import BaseReader


class PixCZarrReader(BaseReader):


    def read(
        self,
        date_interval: Optional[
            Tuple[datetime.datetime, datetime.datetime]
            ] | None = None,
            ) -> None:

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
