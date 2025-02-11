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
"""Converters utility"""

import xarray as xr
import geopandas as gpd


def geoxarray_to_geodataframe(
    ds: xr.Dataset,
        *args, **kwargs) -> gpd.GeoDataFrame:
    """Converts an xarray.Dataset with points coordinates  into\
        a geopandas.GeodataFrame with xvec

    Args:
        ds: Dataset with geometry points coordinates
        args: Cf xvec.to_geodataframe
        kwargs: Cf xvec.to_geodataframe


    Returns:
        A geodataframe with information from ds.
    """

    return ds.xvec.to_geodataframe(*args, **kwargs)
