import os
import fiona

from pixcdust.converters.core import PixCConverter
from pixcdust.readers.netcdf import PixCNcSimpleReader


class PixCNc2GpkgConverter(PixCConverter):
    """missing docstring"""

    def database_from_single_nc(self):
        """missing Docstring"""
        self.database_from_mf_nc()

    def database_from_mf_nc(self):
        """missing Docstring"""

        for path in self.path_in:
            ncsimple = PixCNcSimpleReader(path, self.variables)
            time_start, _, cycle_number, pass_number, tile_number = (
                ncsimple.extract_info_from_nc_attrs(path)
            )

            layer_name = f"{cycle_number}_{pass_number}_\
                {tile_number}_{time_start}"

            # cheking if output file and layer already exist
            if os.path.exists(self.path_out) and self.mode == "a":
                if layer_name in fiona.listlayers(self.path_out):
                    print(
                        f"skipping layer {layer_name} \
                            (already in geopackage {self.path_out})"
                    )
                    continue

            ncsimple.open_dataset()
            gdf = ncsimple.to_geodataframe(
                area_of_interest=self.area_of_interest
            )

            if gdf.size == 0:
                print(
                    f"--File {path} combined with area of interest\
                        returned empty. Skipping it"
                )
                continue

            # writing pixc layer in output file
            gdf.to_file(self.path_out, layer=layer_name, driver="GPKG")
            print(f"--File{path} processed")
