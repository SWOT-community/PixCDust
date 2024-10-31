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


import geopandas as gpd
import h3

from shapely.geometry import Polygon


def cell_to_shapely(cell) -> Polygon:
    coords = h3.h3_to_geo_boundary(cell)
    flipped = tuple(coord[::-1] for coord in coords)
    return Polygon(flipped)


def get_h3_res_name(res: int) -> str:
    return "h3_" + str(res).zfill(2)


def gdf_to_h3_gdf(
    gdf: gpd.GeoDataFrame,
    resolution: int,
    var: str,
        ) -> gpd.GeoDataFrame:

    h3_col = get_h3_res_name(resolution)

    gdf[h3_col] = gdf.apply(
        lambda row: str(h3.geo_to_h3(
            row.geometry.y,
            row.geometry.x,
            resolution
            )),
        axis=1,
        )

    # compute statistics in each H3 cell in a new dataframe
    h3_df = gdf.groupby(h3_col)[var].describe().reset_index()

    # add the geometry of each H3 cell in a new geodataframe
    h3_geoms = h3_df[h3_col].apply(lambda x: cell_to_shapely(x))
    # copy the geometries in the previous statiscal dataframe
    return gpd.GeoDataFrame(data=h3_df, geometry=h3_geoms, crs=4326)