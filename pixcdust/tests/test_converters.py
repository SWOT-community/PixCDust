import random
from pathlib import PosixPath, Path
from typing import List, Union

import numpy as np
import geopandas as gpd
import pytest
from shapely.geometry import Polygon
import xarray as xr

from pixcdust.converters.gpkg import PixCNc2GpkgConverter
from pixcdust.converters.shapefile import PixCNc2ShpConverter
from pixcdust.converters.zarr import PixCNc2ZarrConverter
from pixcdust.readers import PixCGpkgReader
from pixcdust.readers.zarr import PixCZarrReader
from pixcdust.readers.netcdf import PixCNcSimpleReader

LIM_AREA_POL = Polygon(
        [(-1.50580, 43.39543), (-1.36597, 43.39543), (-1.36597, 43.56471), (-1.50580, 43.56471), (-1.50580, 43.39543)])
LIM_AREA_GEOM = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[LIM_AREA_POL])
"""Geometry used as area of interest of limited area tests."""

def validate_conversion_to_nc(read_data: xr.Dataset, converted_vars:List[str], first_file: Union[str, Path])\
        -> None:
    """Compare the start of a converted database to the first original netcdf file.

    Args:
        read_data: Data to validate.
        converted_vars: Names of variables to compare.
        first_file: Netcdf file containing the expected data.
    Raises:
        AssertionError: If the data are too different.
    """
    ncsimple = PixCNcSimpleReader(str(first_file))
    ncsimple.open_dataset()
    validate_conversion(read_data, converted_vars, ncsimple.data,is_longer=True)

def validate_conversion(
        read_data: xr.Dataset,
        converted_vars:List[str],
        expected_data: xr.Dataset,
        is_longer: bool,
        len_tol: int = 0,
        sort_var: bool = False
) -> None:
    """Compare the read data to the expected data.

    Args:
        read_data: Data to validate.
        converted_vars: Names of variables to compare.
        expected_data: Expected data.
        is_longer: Os the expected_data expected to only contain the start of the read_data.
        len_tol: Tolerance for len_tol missing point because of numeric error on lon/lat.
        sort_var: Is the data ordering different and do we neer to sort them.

    Raises:
        AssertionError: If the data are too different.
    """

    for var in converted_vars:
        read_var = read_data[var].data
        expected_var = expected_data[var].data
        if sort_var:
            # can't do something like sorting by longitude as minor conversion error in the longitude
            # result in random swaps (then a few massive errors because we mismatch the points).
            read_var = np.sort(read_var)
            expected_var = np.sort(expected_var)
        np.testing.assert_allclose(read_var[0:30], expected_var[0:30])
        expected_last = len(expected_var)
        if is_longer:
            last = expected_last
        else:
            last = len(read_var)

        np.testing.assert_allclose(read_var[last-30:last-1], expected_var[expected_last-30:expected_last-1])
        r = random.randrange(30,last)
        if len_tol == 0:
            np.testing.assert_allclose(read_var[r-30:r-1], expected_var[r-30:r-1])
        if is_longer:
            assert len(read_var) > expected_last
        else:
            assert expected_last + len_tol >= len(read_var) >= expected_last - len_tol


def test_convert_zarr_full_area(input_files, first_file, tmp_folder):
    """Test zarr conversion without area_of_interest.

    It is compared to the input data.
    """
    # Conversion
    output =  str(tmp_folder / "zarr_conv_test_full")
    converted_vars = ['height', 'sig0', 'classification']
    pixc = PixCNc2ZarrConverter(
            input_files,
            variables=converted_vars,
    )
    pixc.database_from_nc(output, mode="o")

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
            variables=converted_vars,
            area_of_interest=LIM_AREA_GEOM,
    ).database_from_nc(output_gpkg, mode="o")
    return output_gpkg


def test_convert_gpkg_and_zarr_limited_area(input_files, first_file, tmp_folder, converted_lim_gpkg):
    """Test geopackage and zarr conversion with area_of_interest.

    They are compared to each other.
    Note that they are ordered differently with some missing points due tu lon/lat casting.
    """
    # Conversion
    output_zarr = str(tmp_folder / "zarr_conv_test_lim")
    converted_vars = ['height', 'sig0', 'classification']
    output_gpkg = converted_lim_gpkg

    PixCNc2ZarrConverter(
            input_files,
            variables=converted_vars,
            area_of_interest=LIM_AREA_GEOM,
    ).database_from_nc(output_zarr, mode="o")

    # Validation
    gpkg_read = PixCGpkgReader(output_gpkg)
    gpkg_read.read()
    zarr_read = PixCZarrReader(output_zarr)
    zarr_read.read()
    validate_conversion(gpkg_read.data, converted_vars, zarr_read.data, is_longer=False, len_tol=2, sort_var="longitude")


def test_convert_shape_limited_area(input_files, first_file, tmp_folder):
    """Test shapefile conversion with area_of_interest.
    """
    # Conversion
    output =  str(tmp_folder / "shp_conv_test_full")
    converted_vars = ['height', 'sig0', 'classification']

    pixc = PixCNc2ShpConverter(
            input_files,
            variables=converted_vars,
            area_of_interest=LIM_AREA_GEOM,
    )
    pixc.database_from_nc(output, mode="o")

    # TODO : read the data

    # Validation
    # pixc_read = PixCZarrReader(output)
    # pixc_read.read()
    # validate_conversion(gpkg_read.data, converted_vars, zarr_read.data, False)
