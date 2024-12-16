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


"""Pre-conversion SWOT Pixel Cloud Netcdf reader."""
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Tuple, Optional, Iterable

import numpy as np

import xvec # noqa  # pylint: disable=unused-import
# xvec provide xvec accessor to xarray.

import xarray as xr
import geopandas as gpd

from pixcdust.readers.base_reader import BaseReader


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
class PixcNcFormatCfg:
    """Class configuring how a SWOT pixel cloud files is expected to be structured.
    """
    constants: PixCNcSimpleConstants  =  field(
        default_factory=PixCNcSimpleConstants
    )
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


class PixCNcSimpleReader(BaseReader):
    """Class for reading SWOT Pixel cloud official format files.
     It's for simple uses cases as it only reads the pixel_cloud group.

    Attributes:
        path: Path or list of path to read.
        variables: Optionally only read these variables.
        area_of_interest: Optionally only read points in area_of_interest.
        MULTI_FILE_SUPPORT: True, this class can read multiple netcdf.
    """
    MULTI_FILE_SUPPORT=True
    def __init__(self,
                 path: str | Iterable[str] | Path | Iterable[Path],
                 variables: Optional[list[str]] = None,
                 area_of_interest: Optional[gpd.GeoDataFrame] = None,
                 format_cfg : Optional[PixcNcFormatCfg] = None
                 ):
        """Netcdf pixcdust reader configuration.

        Args:
            path: Path or list of path of the file(s) to read.
            variables: Optionally only read these variables.
            area_of_interest: Optionally only read points in area_of_interest.
            format_cfg: Optional. Config describing the netcdf structure.
                Default to current SWOT Pixel Cloud.
        """
        super().__init__(path, area_of_interest=area_of_interest, variables=variables)
        if not format_cfg:
            format_cfg = PixcNcFormatCfg()
        self.forbidden_variables = format_cfg.forbidden_variables
        self.trusted_group = format_cfg.trusted_group
        self.cst = format_cfg.constants



    @staticmethod
    def extract_info_from_nc_attrs(filename: str) -> Tuple[str, datetime, int, int, int, str]:
        """Extracts orbit information from global attributes\
            in a SWOT pixel cloud netcdf.

        Args:
            filename: path of SWOT PIXC Netcdf file

        Returns:
            (time of granule start as string,
            time of granule start as datetime,
            cycle number,
            pass number,
            tile number)
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

    def read(self, orbit_info: bool = False) -> None:
        """ Load self.path file(s).
        You can then access from data or with methods like
        to_xarray, to_dataframe or to_geodataframe.

        See self.open_mfdataset for more details on how multiple files are merged.

        Args:
            orbit_info: Option to extract the orbit information.
                Only used if multiple files are read.
        """
        if self.multi_file_db:
            return self.open_mfdataset(orbit_info)
        return self.open_dataset()


    def open_dataset(self) -> None:
        """ Load the self.path file (need only one file in self.path).
        You can then access from data or with methods like
        to_xarray, to_dataframe or to_geodataframe.
        """
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
        """ Load self.path file(s) as a nested array.
        You can then access from data or with methods like
        to_xarray, to_dataframe or to_geodataframe.

        Variables that are not one-dimensional
        along `points` dimension are not allowed and will be dropped:
            - 'pixc_line_qual',
            - 'pixc_line_to_tvp',
            - 'interferogram'
            - etc.


        Args:
            orbit_info: option to extract the orbit information.
        """

        if orbit_info:
            preprocess = self.__preprocess_types_and_add_orbit_info
        else:
            preprocess = self.__preprocess_types
        self.data = xr.open_mfdataset(
            self.path,
            group=self.trusted_group,
            engine="netcdf4",
            drop_variables=self.forbidden_variables,
            combine="nested",
            concat_dim="points",
            preprocess=preprocess,
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
        Useful for compatibility with xvec package and geographic manipulation.

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
            if self.cst.default_added_time_name in self.data :
                self.data = self.data.sortby(self.cst.default_added_time_name)


    def __preprocess_types(self, ds: xr.Dataset) -> xr.Dataset:
        """Preprocessing function changing types in pixc dataset.

         It cast the lon and lat to float32.

        Args:
            ds: pixc dataset read by xarray.open_dataset to preprocess

        Returns:
            dataset with cast types
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
        """Preprocessing function adding orbit information in pixc dataset.

         It cast the lon and lat to float32.

        Args:
            ds: pixc dataset read by xarray.open_dataset to preprocess

        Returns:
            dataset augmented with orbit information for each index and with cast types
        """
        ds = self.__preprocess_types(ds)

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
