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

import geopandas as gpd
import h3

from shapely.geometry import Polygon

def h3_to_polygon(h3_index):
    boundary = h3.api.basic_int.h3_to_geo_boundary(h3_index, geo_json=True)
    return Polygon(boundary)


def get_h3_res_name(res: int) -> str:
    return "h3_" + str(res).zfill(2)


def gdf_to_h3_gdf(
    gdf: gpd.GeoDataFrame,
    resolution: int,
        ) -> gpd.GeoDataFrame:
    """
   Convert a GeoDataFrame with latitude and longitude columns to a GeoDataFrame
   where rows are aggregated into H3 hexagons based on a given resolution.

   Args:
       gdf (gpd.GeoDataFrame): The input GeoDataFrame containing 'latitude' and 'longitude' columns.
       resolution (int): The H3 resolution level for hexagon indexing.

   Returns:
       gpd.GeoDataFrame: A new GeoDataFrame where the rows are grouped by H3 hexagons,
                         containing the mean of the grouped variables, and a geometry column with polygons
                         representing the H3 hexagons.
   """

    # Get the column name for H3 index based on the resolution
    h3_col = get_h3_res_name(resolution)

    # Apply the H3 function to each row to calculate the H3 index based on latitude, longitude, and resolution
    gdf[h3_col] = gdf.apply(lambda row: h3.api.basic_int.geo_to_h3(row['latitude'], row['longitude'], resolution),
                                axis=1)

    # Drop the latitude, longitude, and geometry columns as they are no longer needed
    h3_df = gdf.drop(columns=['latitude', 'longitude', 'geometry'])

    # Group by the H3 index column, and compute the mean of all other columns in each group
    h3_df = h3_df.groupby(h3_col).mean().reset_index()

    # Convert each H3 index into a polygon geometry (hexagon) representing its boundaries
    geometry = h3_df[h3_col].apply(h3_to_polygon)

    return gpd.GeoDataFrame(data=h3_df, geometry=geometry, crs=4326)
