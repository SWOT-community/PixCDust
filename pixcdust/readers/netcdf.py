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


"""This module reads SWOT Pixel Cloud Netcdfs"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Tuple, Optional

import numpy as np

import xarray as xr
import xvec
import pandas as pd
import geopandas as gpd


@dataclass
class PixCNcSimpleConstants:
    """Class setting defaults values in SWOT pixel cloud files \
        such as name of attributes and variables
    """

    default_dim_name: str = "points"
    default_long_name: str = "longitude"
    default_lat_name: str = "latitude"
    default_cyc_num_name: str = "cycle_number"
    default_pass_num_name: str = "pass_number"
    default_tile_num_name: str = "tile_number"
    default_swath_side_name: str = "swath_side"
    default_time_start_name: str = "time_granule_start"
    default_time_format_filename: str = "%Y%m%dT%H%M%S"
    default_time_format_attrs: str = "%Y-%m-%dT%H:%M:%S.%fZ"
    default_added_time_name = "time"
    default_added_points_name = "points"


@dataclass
class PixCNcSimpleReader:
    """Class for reading SWOT Pixel cloud official format files reader
    for most simple uses cases:
    It only reads in the pixel_cloud group

    Returns:
        _type_: _description_
    """

    path: list[str] | str
    variables: Optional[list[str]] = None
    area_of_interest: Optional[gpd.GeoDataFrame] = None
    trusted_group: str = "pixel_cloud"
    forbidden_variables: list[str] = field(
        default_factory=lambda: [
            "pixc_line_qual",
            "pixc_line_to_tvp",
            "data_window_first_valid",
            "data_window_last_valid",
            "data_window_first_cross_track",
            "data_window_last_cross_track",
            "interferogram",
        ]
    )
    data: xr.Dataset = None
    cst = PixCNcSimpleConstants()

    @staticmethod
    def extract_info_from_nc_attrs(filename: str) -> Tuple[str, datetime, int, int, int, str]:
        """Extracts orbit information from global attributes\
            in SWOT pixel cloud netcdf

        Args:
            filename (str): path of SWOT PIXC Netcdf file

        Returns:
            str: time of granule start
            datetime.datetime: time of granule start
            int: cycle number
            int: pass number
            int: tile number
        """
        cst = PixCNcSimpleConstants()

        with xr.open_dataset(filename, engine="netcdf4") as ds_glob:
            tile_number = np.uint16(ds_glob.attrs[cst.default_tile_num_name])
            swath_side = ds_glob.attrs[cst.default_swath_side_name]
            pass_number = np.uint16(ds_glob.attrs[cst.default_pass_num_name])
            cycle_number = np.uint16(ds_glob.attrs[cst.default_cyc_num_name])
            time_granule_start = ds_glob.attrs[cst.default_time_start_name]
            dt_time_start = datetime.strptime(
                time_granule_start, cst.default_time_format_attrs
            ).replace(microsecond=0)

        return (
            time_granule_start,
            dt_time_start,
            cycle_number,
            pass_number,
            tile_number,
            swath_side,
        )

    def open_dataset(self) -> None:
        """reads one pixc file and stores data in self.data"""
        self.data = xr.open_dataset(
            self.path,
            group=self.trusted_group,
            engine="netcdf4",
        )
        if self.variables:
            self.data = self.data[self.variables]

        self.__postprocess_points()

    def open_mfdataset(
        self,
        orbit_info: bool = False,
    ) -> None:
        """ reads one or multiple pixc files and stores\
            a nested xarray in self.data.
        In this case, variables that are not one-dimensional
        along `points` dimension are not allowed and will be dropped:
            - 'pixc_line_qual',
            - 'pixc_line_to_tvp',
            - 'interferogram'
            - etc.


        Args:
            orbit_info (bool, optional): option to extract\
                the orbit information in data. Defaults to False.
        """

        if not orbit_info:
            self.data = xr.open_mfdataset(
                self.path,
                group=self.trusted_group,
                engine="netcdf4",
                drop_variables=self.forbidden_variables,
                combine="nested",
                concat_dim="points",
                preprocess=self.__preprocess_types,
            )
        else:
            self.data = xr.open_mfdataset(
                self.path,
                group=self.trusted_group,
                engine="netcdf4",
                drop_variables=self.forbidden_variables,
                combine="nested",
                concat_dim="points",
                preprocess=self.__preprocess_types_and_add_orbit_info,
            )

        if self.variables:
            # check if variables in forbidden variables before loading
            if len(set(self.variables).intersection(set(self.forbidden_variables))) > 0:
                raise IOError(
                    f"variables from {self.forbidden_variables} \
                        cannot be extracted"
                )

            if orbit_info:
                self.variables.extend(
                    [
                        self.cst.default_tile_num_name,
                        self.cst.default_cyc_num_name,
                        self.cst.default_pass_num_name,
                        self.cst.default_added_time_name,
                    ]
                )
            self.data = self.data[self.variables]

            self.__postprocess_points()

    def __postprocess_points(self) -> None:
        """Adds a points coordinates containing shapely.Points (longitude, latitude)
        Useful for compatibility with xvec package and geographic manipulation

        """
        geom = gpd.points_from_xy(
            self.data[self.cst.default_long_name],
            self.data[self.cst.default_lat_name],
        )

        self.data = self.data.assign_coords(
            {self.cst.default_added_points_name: geom},
        )

        self.data = self.data.xvec.set_geom_indexes(
            self.cst.default_added_points_name,
            crs=4326,
        )

        if self.area_of_interest is not None:
            self.data = self.data.xvec.query(
                self.cst.default_added_points_name,
                self.area_of_interest.geometry,
                # predicate="within",
                # unique=True,
            )

    def __preprocess_types(self, ds: xr.Dataset) -> xr.Dataset:
        """preprocessing function changing types in pixc dataset

        Args:
            ds (xarray.Dataset): pixc dataset read by xarray.open_dataset

        Returns:
            xarray.Dataset: dataset with enhanced types
        """
        ds[self.cst.default_long_name] = ds[self.cst.default_long_name].astype(
            np.float32,
            copy=False,
        )
        ds[self.cst.default_lat_name] = ds[self.cst.default_lat_name].astype(
            np.float32,
            copy=False,
        )

        return ds

    def __preprocess_types_and_add_orbit_info(self, ds: xr.Dataset) -> xr.Dataset:
        """preprocessing function adding orbit information in pixc dataset

        Args:
            ds (xarray.Dataset): pixc dataset read by xarray.open_dataset

        Returns:
            xarray.Dataset: dataset augmented with orbit\
                information for each index
        """
        ds[self.cst.default_long_name] = ds[self.cst.default_long_name].astype(
            np.float32, copy=False
        )

        filename = ds.encoding["source"]

        _, dt_time_start, cycle_number, pass_number, tile_number, swath_side = (
            self.extract_info_from_nc_attrs(filename)
        )

        ds[self.cst.default_tile_num_name] = tile_number
        ds[self.cst.default_swath_side_name] = swath_side
        ds[self.cst.default_pass_num_name] = pass_number
        ds[self.cst.default_cyc_num_name] = cycle_number
        ds[self.cst.default_added_time_name] = dt_time_start

        return ds

    def to_xarray(self) -> xr.Dataset:
        """returning an xarray.Dataset from object
        (this function exists for potential future compatibility)

        Returns:
            xr.Dataset: Dataset with information from file
        """

        return self.data

    def to_dataframe(self) -> pd.DataFrame:
        """returns a pandas.DataFrame from object

        Returns:
            pd.DataFrame: Dataframe with information from file
        """
        return self.data.to_dataframe()

    def to_geodataframe(
        self,
    ) -> gpd.GeoDataFrame:
        """

        Returns:
            gpd.GeoDataFrame: a geodataframe with information from file
        """

        gdf = self.data.xvec.to_geodataframe()

        if self.area_of_interest is not None:
            gdf = gdf.overlay(self.area_of_interest, how="intersection")

        return gdf
