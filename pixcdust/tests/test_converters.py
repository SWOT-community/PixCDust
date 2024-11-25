import random
from pathlib import PosixPath

import numpy as np
import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from pixcdust.converters.gpkg import PixCNc2GpkgConverter
from pixcdust.converters.shapefile import PixCNc2ShpConverter
from pixcdust.converters.zarr import PixCNc2ZarrConverter
from pixcdust.readers import PixCGpkgReader
from pixcdust.readers.zarr import PixCZarrReader
from pixcdust.readers.netcdf import PixCNcSimpleReader

LIM_AREA_POL = Polygon(
        [(-1.50580, 43.39543), (-1.36597, 43.39543), (-1.36597, 43.56471), (-1.50580, 43.56471), (-1.50580, 43.39543)])
LIM_AREA_GEOM = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[LIM_AREA_POL])

def validate_conversion_to_nc(read_data, converted_vars, first_file):
    ncsimple = PixCNcSimpleReader(str(first_file))
    ncsimple.open_dataset()
    validate_conversion(read_data, converted_vars, ncsimple.data,True)

def validate_conversion(read_data, converted_vars, expected_data, is_longer):
    for var in converted_vars:
        read_var = read_data[var].data
        np.testing.assert_allclose(read_var[0:30], expected_data[var][0:30])
        last = len(expected_data[var])
        np.testing.assert_allclose(read_var[last-30:last-1], expected_data[var][last-30:last-1])
        r = random.randrange(30,last)
        np.testing.assert_allclose(read_var[r-30:r-1], expected_data[var][r-30:r-1])
        print(read_var)
        if is_longer:
            assert last < len(read_var)
        else:
            assert last == len(read_var)

def test_convert_zarr_full_area(input_files, first_file, tmp_folder):
    # Conversion
    output =  str(tmp_folder / "zarr_conv_test_full")
    converted_vars = ['height', 'sig0', 'classification']
    pixc = PixCNc2ZarrConverter(
            input_files,
            output,
            variables=converted_vars,
            mode='o',
    )
    pixc.database_from_nc()

    # Validation
    pixc_read = PixCZarrReader(output)
    pixc_read.read()
    validate_conversion_to_nc(pixc_read.data, converted_vars, first_file)

@pytest.fixture(scope="session")
def converted_lim_gpkg(input_files, tmp_folder):
    output_gpkg = str(tmp_folder / "gpkg_conv_test_lim")
    converted_vars = ['height', 'sig0', 'classification']
    PixCNc2GpkgConverter(
            input_files,
            output_gpkg,
            variables=converted_vars,
            area_of_interest=LIM_AREA_GEOM,
            mode='o',
    ).database_from_nc()
    return output_gpkg


def test_convert_gpkg_and_zarr_limited_area(input_files, first_file, tmp_folder):
    # Conversion
    output_zarr = str(tmp_folder / "zarr_conv_test_lim")
    converted_vars = ['height', 'sig0', 'classification']
    output_gpkg = str(tmp_folder / "gpkg_conv_test_lim")

    PixCNc2GpkgConverter(
            input_files,
            output_gpkg,
            variables=converted_vars,
            area_of_interest=LIM_AREA_GEOM,
            mode='o',
    ).database_from_nc()

    PixCNc2ZarrConverter(
            input_files,
            output_zarr,
            variables=converted_vars,
            area_of_interest=LIM_AREA_GEOM,
            mode='o',
    ).database_from_nc()

    # Validation
    gpkg_read = PixCGpkgReader(output_gpkg)
    gpkg_read.read()
    zarr_read = PixCZarrReader(output_zarr)
    zarr_read.read()
    validate_conversion(gpkg_read.data, converted_vars, zarr_read.data, False)


def test_convert_shape_limited_area(input_files, first_file, tmp_folder):
    # Conversion
    output =  str(tmp_folder / "shp_conv_test_full")
    converted_vars = ['height', 'sig0', 'classification']

    pixc = PixCNc2ShpConverter(
            input_files,
            output,
            variables=converted_vars,
            area_of_interest=LIM_AREA_GEOM,
            mode='o',
    )
    pixc.database_from_nc()

    # TODO : read the data

    # Validation
    # pixc_read = PixCZarrReader(output)
    # pixc_read.read()
    # validate_conversion(gpkg_read.data, converted_vars, zarr_read.data, False)
