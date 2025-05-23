import random
from pathlib import PosixPath, Path
from typing import List, Union

import fiona
import numpy as np
import geopandas as gpd
import pytest
from shapely.geometry import Polygon
import xarray as xr

from pixcdust.converters.gpkg import Nc2GpkgConverter, GpkgDGGSProjecter
from pixcdust.converters.shapefile import Nc2ShpConverter
from pixcdust.converters.zarr import Nc2ZarrConverter
from pixcdust.readers import GpkgReader
from pixcdust.readers.zarr import ZarrReader
from pixcdust.readers.netcdf import NcSimpleReader

LIM_AREA_POL = Polygon(
        [(-1.50580, 43.39543), (-1.36597, 43.39543), (-1.36597, 43.56471), (-1.50580, 43.56471), (-1.50580, 43.39543)])
LIM_AREA_GEOM = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[LIM_AREA_POL])
"""Geometry used as area of interest of limited area tests."""


def test_nc_simple_reader_conditions(input_files):
    """Test NcSimpleReader with conditions on variables."""
    # Define conditions
    conditions = {"classification": {'operator': "ge", 'threshold': 4}, # classification >= 4
                  "classification": {'operator': "le", 'threshold': 3}, # classification <= 3
                  "sig0": {'operator': "gt", 'threshold': 15} # sig0 > 15
                  }

    converted_vars = ['height', 'sig0', 'classification']

    # Instantiate the NcSimpleReader with conditions
    reader = NcSimpleReader(
        input_files,
        variables=converted_vars,
        conditions=conditions,
    )

    # Read the dataset and apply filtering
    reader.read()

    # Validate the data after filtering
    for var, condition in conditions.items():
        op = condition.get("operator")
        val = condition.get("threshold")
        if op == 'ge':
            assert (reader.data[var] >= val).all(), f"{var} not >= {val}"
        elif op == 'le':
            assert (reader.data[var] <= val).all(), f"{var} not <= {val}"
        elif op == 'gt':
            assert (reader.data[var] > val).all(), f"{var} not > {val}"
        elif op == 'lt':
            assert (reader.data[var] < val).all(), f"{var} not < {val}"


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
    ncsimple = NcSimpleReader(str(first_file))
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
    pixc = Nc2ZarrConverter(
            input_files,
            variables=converted_vars,
    )
    pixc.database_from_nc(output, mode="o")

    # Validation
    pixc_read = ZarrReader(output)
    pixc_read.read()
    validate_conversion_to_nc(pixc_read.data, converted_vars, first_file)

@pytest.fixture(scope="session")
def converted_lim_gpkg(input_files, tmp_folder):
    output_gpkg = str(tmp_folder / "gpkg_conv_test_lim")
    converted_vars = ['height', 'sig0', 'classification']
    Nc2GpkgConverter(
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

    Nc2ZarrConverter(
            input_files,
            variables=converted_vars,
            area_of_interest=LIM_AREA_GEOM,
    ).database_from_nc(output_zarr, mode="o")

    # Validation
    gpkg_read = GpkgReader(output_gpkg)
    gpkg_read.read()
    zarr_read = ZarrReader(output_zarr)
    zarr_read.read()
    validate_conversion(gpkg_read.data, converted_vars, zarr_read.data, is_longer=False, len_tol=2, sort_var="longitude")


def test_convert_shape_limited_area(input_files, first_file, tmp_folder):
    """Test shapefile conversion with area_of_interest.
    """
    # Conversion
    output =  str(tmp_folder / "shp_conv_test_full")
    converted_vars = ['height', 'sig0', 'classification']

    pixc = Nc2ShpConverter(
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


# Test for GpkgDGGSProjecter
def test_gpkg_dggs_projecter(converted_lim_gpkg, tmp_folder):
    """Test the GpkgDGGSProjecter class by converting a sample Gpkg to a DGGS projection."""

    input_gpkg = converted_lim_gpkg
    # Define parameters
    dggs_res = 10
    healpix = False
    dggs_layer_pattern = '_h3'
    output_path = str(tmp_folder / "gpkg_dggs_output")

    # Create an instance of GpkgDGGSProjecter
    projecter = GpkgDGGSProjecter(
        path=input_gpkg,
        dggs_res=dggs_res,
        healpix=healpix,
        dggs_layer_pattern=dggs_layer_pattern,
        path_out=output_path
    )

    # Validate initialization
    assert projecter.path == input_gpkg
    assert projecter.dggs_res == dggs_res
    assert projecter.healpix == healpix
    assert projecter.dggs_layer_pattern == dggs_layer_pattern
    assert projecter.path_out == output_path

    # Validate layers are correctly loaded
    for layer in fiona.listlayers(input_gpkg):
        if not layer.endswith(dggs_layer_pattern):
            assert layer in projecter.database.layers

    # Mock computation for a single layer and validate the conversion
    projecter.compute_layers()

    # Validate output file existence and content
    for layer in projecter.database.layers:
        layername_out = f"{layer}_{dggs_res}_{dggs_layer_pattern}"
        # Check if the file was created with the right name
        assert layername_out in fiona.listlayers(output_path)

    # Test the same with HEALPix projection
    projecter.healpix = True
    projecter.compute_layers()

    # Validate output for HEALPix
    for layer in projecter.database.layers:
        layername_out = f"{layer}_{dggs_res}_{dggs_layer_pattern}"
        assert layername_out in fiona.listlayers(output_path)
