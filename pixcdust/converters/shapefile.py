"""Shapefile converter."""

import os
from pathlib import Path

from tqdm import tqdm

from pixcdust.converters.core import PixCConverter
from pixcdust.readers.netcdf import PixCNcSimpleReader


class PixCNc2ShpConverter(PixCConverter):
    """Converter from official SWOT Pixel Cloud Netcdf to Shapefile database

    Attributes:
        path_in: List of path of files to convert.
        variables: Optionally only read these variables.
        area_of_interest: Optionally only read points in area_of_interest.
    """

    def database_from_nc(self, path_out: str | Path, mode: str = "w") -> None:
        path_out = str(path_out)
        try:
            os.mkdir(path_out)
        except FileExistsError:
            pass
        for path in tqdm(self.path_in):
            ncsimple = PixCNcSimpleReader(path,
                                          variables=self.variables,
                                          area_of_interest=self.area_of_interest)

            filename_out = os.path.splitext(os.path.basename(path))[0]
            path_shp = os.path.join(path_out, filename_out + '.shp',)
            # cheking if output file and layer already exist
            if os.path.exists(path_shp) and mode == "w":
                continue

            # converting data from xarray to geodataframe
            ncsimple.open_dataset()
            gdf = ncsimple.to_geodataframe()

            if gdf.size == 0:
                tqdm.write(
                    f"--File {path} combined with area of interest\
                        returned empty. Skipping it"
                )
                continue

            # writing pixc layer in output file, shapefile
            gdf.to_file(path_shp)
            tqdm.write(f"--File{path} processed")
