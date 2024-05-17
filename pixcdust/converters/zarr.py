import zcollection
import fsspec

from pixcdust.converters.core import PixCConverter
from pixcdust.readers.netcdf import PixCNcSimpleReader


class PixCNc2ZarrConverter(PixCConverter):
    """missing docstring"""

    def database_from_single_nc(self):
        """missing Docstring"""
        self.database_from_mf_nc()

    def database_from_mf_nc(self):
        """missing Docstring"""

        xr_ds = PixCNcSimpleReader(self.path_in, self.variables)

        xr_ds.open_mfdataset(orbit_info=True)
        
        # partition_handler = zcollection.partitioning.Date(('time', ), resolution='D')
        partition_handler = zcollection.partitioning.Sequence(("cycle_num", "pass_num", "tile_num" ))

        zc_ds = zcollection.Dataset.from_xarray(xr_ds.to_xarray())
        filesystem = fsspec.filesystem("file")
        collection = zcollection.create_collection(
            axis="time",
            ds=zc_ds,
            partition_handler=partition_handler,
            partition_base_dir=self.path_out,
            filesystem=filesystem
        )
        collection.insert(zc_ds)