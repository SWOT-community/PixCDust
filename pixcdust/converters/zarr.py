import os
import shutil
import fsspec
from typing import Tuple, List, Union

import zcollection
import zcollection.indexing
import dask
import dask.utils


from pixcdust.converters.core import PixCConverter
from pixcdust.readers.netcdf import PixCNcSimpleReader

TIME_VARNAME = 'time'


class PixCNc2ZarrConverter(PixCConverter):
    """Class for converting Pixel Cloud files to Zarr database

    """
    collection: zcollection.collection.Collection = None
    __time_varname: str = TIME_VARNAME
    __fs = fsspec.filesystem("file")
    __chunk_size = dask.utils.parse_bytes('2MiB')

    def database_from_nc(self):
        """function to create a database from a multiple netcdf PIXC files
        """
        if self.mode in ['o', 'overwrite'] and os.path.exists(self.path_out):
            shutil.rmtree(self.path_out)

        with dask.distributed.LocalCluster(processes=False) as cluster, \
                dask.distributed.Client(cluster) as client:

            xr_ds = PixCNcSimpleReader(self.path_in, self.variables)

            xr_ds.open_mfdataset(
                orbit_info=True,
            )

            zc_ds = zcollection.Dataset.from_xarray(
                xr_ds.to_xarray(),
                )
            zc_ds.block_size_limit = self.__chunk_size
            zc_ds.chunks = {
                list(zc_ds.dimensions.keys())[0]: self.__chunk_size
            }

            init = True 
            if not os.path.exists(self.path_out) and init:

                partition_handler = zcollection.partitioning.Date(
                    ('time', ),
                    's',
                )

                self.collection = zcollection.create_collection(
                    axis=self.__time_varname,
                    ds=zc_ds,
                    partition_handler=partition_handler,
                    partition_base_dir=self.path_out,
                    filesystem=self.__fs,
                )
                init = False

            else:
                self.collection = zcollection.open_collection(
                    self.path_out,
                    filesystem=self.__fs,
                    mode='w',
                    )

            self.collection.insert(
                zc_ds,
                merge_callable=zcollection.collection.merging.merge_time_series
            )
