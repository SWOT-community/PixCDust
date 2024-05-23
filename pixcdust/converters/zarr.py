import os
import shutil
import fsspec

import zcollection
import dask


from pixcdust.converters.core import PixCConverter
from pixcdust.readers.netcdf import PixCNcSimpleReader


class PixCNc2ZarrConverter(PixCConverter):
    """Class for converting Pixel Cloud files to Zarr database

    """

    def database_from_single_nc(self):
        """function to create a database from a single netcdf PIXC file
        (calls the corresponding multifile function, this function exist for future compatibility)
        """
        self.database_from_mf_nc()

    def database_from_mf_nc(self):
        """function to create a database from a multiple netcdf PIXC files
        """
        if self.mode in ['o', 'overwrite'] and os.path.exists(self.path_out):
            shutil.rmtree(self.path_out)

        filesystem = fsspec.filesystem("file")

        with dask.distributed.LocalCluster(processes=False) as cluster:
            client = dask.distributed.Client(cluster)

            init = True
            for file in self.path_in:
                print(file)
                xr_ds = PixCNcSimpleReader(self.path_in, self.variables)

                xr_ds.open_mfdataset(orbit_info=True)
                


                if not os.path.exists(self.path_out) and init:

                    partition_handler = zcollection.partitioning.Sequence(
                        ("cycle_num", "pass_num", "tile_num")
                    )

                    zc_ds = zcollection.Dataset.from_xarray(xr_ds.to_xarray())

                    collection = zcollection.create_collection(
                        axis="time",
                        ds=zc_ds,
                        partition_handler=partition_handler,
                        partition_base_dir=self.path_out,
                        filesystem=filesystem,
                    )
                    init = False

                elif os.path.exists(self.path_out):
                    collection = zcollection.open_collection(
                        self.path_out,
                        filesystem=filesystem,
                        mode='w',
                        )
                    zc_ds = zcollection.Dataset.from_xarray(xr_ds.to_xarray())

                else:
                    raise Exception(f"this should not happen {os.path.exists(self.path_out)}, {init}")

                collection.insert(zc_ds)
