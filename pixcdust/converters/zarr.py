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
"""Zarr converter."""

import os
import shutil
from collections.abc import Iterable
from pathlib import Path

import dask
import dask.distributed
import dask.utils
import fsspec
import geopandas as gpd
import pandas

from pixcdust.converters.core import Converter
from pixcdust.readers.netcdf import NcSimpleConstants, NcSimpleReader

TIME_VARNAME = "time"
# TODO is there a better solution than fix the chunk size here ?
CHUNK_SIZE = dask.utils.parse_bytes("2MiB")


class Nc2ZarrConverter(Converter):
    """Converter from official SWOT Pixel Cloud Netcdf to Shapefile database

    Attributes:
        path_in: List of path of files to convert.
        variables: Optionally only read these variables.
        area_of_interest: Optionally only read points in area_of_interest.
        conditions: Optionally pass conditions to filter variables.\
                    Example: {\
                    "sig0":{'operator': "ge", 'threshold': 20},\
                    "classification":{'operator': "ge", 'threshold': 3},\
                    }
    """

    def __init__(
        self,
        path_in: str | Iterable[str] | Path | Iterable[Path],
        variables: list[str] | None = None,
        area_of_interest: gpd.GeoDataFrame | None = None,
        conditions: dict[str, dict[str, str | float]] | None = None,
    ):
        """Basic initialisation of a pixcdust converter.

        They convert from official SWOT Pixel Cloud Netcdf to the supported format.

        Attributes:
            path_in: Path or list of path of file(s) to convert.
            variables: Optionally only read these variables.
            area_of_interest: Optionally only read points in area_of_interest.
            conditions: Optionally pass conditions to filter variables.\
                    Example: {\
                    "sig0":{'operator': "ge", 'threshold': 20},\
                    "classification":{'operator': "ge", 'threshold': 3},\
                    }
        """
        super().__init__(
            path_in=path_in,
            variables=variables,
            area_of_interest=area_of_interest,
            conditions=conditions,
        )
        self.__time_varname: str = TIME_VARNAME
        self.__fs = fsspec.filesystem("file")
        self.__chunk_size = dask.utils.parse_bytes("2MiB")
        self.__cst = NcSimpleConstants()

    def database_from_nc(self, path_out: str | Path, mode: str = "w") -> None:

        if mode in ["o", "overwrite"] and os.path.exists(path_out):
            shutil.rmtree(path_out)

        path_out = Path(path_out)
        append = path_out.exists()

        with (
            dask.distributed.LocalCluster(processes=True) as cluster,
            dask.distributed.Client(cluster) as _,
        ):
            xr_ds = NcSimpleReader(
                path=self.path_in,
                variables=self.variables,
                area_of_interest=self.area_of_interest,
                conditions=self.conditions,
            )

            xr_ds.open_mfdataset(
                orbit_info=True,
            )

            # Convert time from object type to datetime type to avoid problem with dask
            xr_ds_xarray = xr_ds.to_xarray().drop_vars(
                self.__cst.default_added_points_name
            )

            time = xr_ds_xarray["time"].values

            time2 = pandas.DatetimeIndex(time).tz_localize(None).to_numpy()
            xr_ds_xarray = xr_ds_xarray.assign_coords(time=("points", time2))

            # Approximate the original 2 MiB chunk-size target.
            npoints = xr_ds_xarray.sizes.get("points", 0)
            if npoints:
                bytes_per_point = max(1, xr_ds_xarray.nbytes // npoints)
                chunk_points = max(1, CHUNK_SIZE // bytes_per_point)
                xr_ds_xarray = xr_ds_xarray.chunk({"points": int(chunk_points)})

            if append:
                xr_ds_xarray.to_zarr(
                    path_out,
                    mode="a",
                    append_dim="points",
                    consolidated=True,
                    zarr_version=2,
                )
            else:
                xr_ds_xarray.to_zarr(
                    path_out,
                    mode="w",
                    consolidated=True,
                    zarr_version=2,
                )
