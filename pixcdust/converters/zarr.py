"""Zarr converter."""

import os
import shutil
from pathlib import Path
from typing import Optional, Iterable

import fsspec
from typing import Tuple, List, Union

import geopandas as gpd
import zcollection
import zcollection.indexing
import dask
import dask.utils


from pixcdust.converters.core import Converter
from pixcdust.readers.netcdf import NcSimpleReader, NcSimpleConstants

TIME_VARNAME = 'time'


class Nc2ZarrConverter(Converter):
    """Converter from official SWOT Pixel Cloud Netcdf to Shapefile database

    Attributes:
        path_in: List of path of files to convert.
        variables: Optionally only read these variables.
        area_of_interest: Optionally only read points in area_of_interest.
    """

    def __init__(
        self,
        path_in: str | Iterable[str] | Path | Iterable[Path],
        variables: Optional[list[str]] = None,
        area_of_interest: Optional[gpd.GeoDataFrame] = None,
    ):
        """Basic initialisation of a pixcdust converter.

        They convert from official SWOT Pixel Cloud Netcdf to the supported format.

        Attributes:
            path_in: Path or list of path of file(s) to convert.
            variables: Optionally only read these variables.
            area_of_interest: Optionally only read points in area_of_interest.
        """
        super().__init__(path_in=path_in,
                         variables=variables,
                         area_of_interest=area_of_interest)
        self.collection: zcollection.collection.Collection = None
        self.__time_varname: str = TIME_VARNAME
        self.__fs = fsspec.filesystem("file")
        self.__chunk_size = dask.utils.parse_bytes('2MiB')
        self.__cst = NcSimpleConstants()

    def database_from_nc(self, path_out: str | Path, mode: str = "w") -> None:

        if mode in ['o', 'overwrite'] and os.path.exists(path_out):
            shutil.rmtree(path_out)

        with dask.distributed.LocalCluster(processes=True) as cluster, \
                dask.distributed.Client(cluster) as client:

            xr_ds = NcSimpleReader(
                self.path_in,
                self.variables,
                self.area_of_interest,
            )

            xr_ds.open_mfdataset(
                orbit_info=True,
            )

            zc_ds = zcollection.Dataset.from_xarray(
                xr_ds.to_xarray().drop_vars(self.__cst.default_added_points_name),
                )
            zc_ds.block_size_limit = self.__chunk_size
            zc_ds.chunks = {
                list(zc_ds.dimensions.keys())[0]: self.__chunk_size
            }

            init = True
            if not os.path.exists(path_out) and init:

                partition_handler = zcollection.partitioning.Date(
                    (xr_ds.cst.default_added_time_name, ),
                    's',
                )

                self.collection = zcollection.create_collection(
                    axis=self.__time_varname,
                    ds=zc_ds,
                    partition_handler=partition_handler,
                    partition_base_dir=path_out,
                    filesystem=self.__fs,
                )
                init = False

            else:
                self.collection = zcollection.open_collection(
                    path_out,
                    filesystem=self.__fs,
                    mode='w',
                    )
            self.collection.insert(
                zc_ds,
                merge_callable=zcollection.collection.merging.merge_time_series
            )
