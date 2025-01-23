from datetime import datetime
import numpy as np

import xarray as xr
import geopandas as gpd

from pixcdust.readers.netcdf import NcSimpleConstants


def mock_xarray(length: int = 10000) -> xr.Dataset:
    """Locks an xarray extracted from a typical SWOT PixC netcdf file
    and enhanced with PixCNcSimpleReader and orbit infos.

    Args:
        length: length of the array. Defaults to 10000.

    Returns:
        dataset with some typical variables
    """
    cst = NcSimpleConstants()

    # mocking dimension
    dims = (cst.default_dim_name,)

    # mocking coordinates
    coord_step = 0.001
    coords = {
        cst.default_long_name: np.linspace(
            10.0,
            10.0 + length * coord_step,
            length,
            dtype=np.float64,
        ),
        cst.default_lat_name: np.linspace(
            40.0,
            40.0 + length * coord_step,
            length,
            dtype=np.float64,
        ),
    }

    # mocking data
    x = coords[cst.default_lat_name]
    cst_time_array = np.ones(len(x)).astype(datetime)
    cst_time_array[:] = datetime(2024, 6, 5, 11, 40, 12)

    data_vars = {
        "height": (dims, np.sin(x) + np.random.normal(scale=10, size=len(x))),
        "geoid": (dims, np.sin(x) + np.random.normal(scale=10, size=len(x))),
        "wse": (dims, np.sin(x) + np.random.normal(scale=0.1, size=len(x))),
        "sig0": (dims, np.random.uniform(low=0.5, high=90.0, size=(len(x),))),
        "classification": (
            dims,
            np.random.randint(
                low=0,
                high=7,
                size=(len(x),),
            ).astype(np.float32),
        ),
        cst.default_tile_num_name: (
            dims,
            np.random.randint(1, 200) * np.ones_like(x, dtype=np.uint16),
        ),
        cst.default_cyc_num_name: (
            dims,
            np.random.randint(1, 500) * np.ones_like(x, dtype=np.uint16),
        ),
        cst.default_pass_num_name: (
            dims,
            np.random.randint(1, 500) * np.ones_like(x, dtype=np.uint16),
        ),
        cst.default_added_time_name: (
            dims,
            cst_time_array,
        ),
    }

    return xr.Dataset(data_vars=data_vars, coords=coords)


def mock_area_of_interest() -> gpd.GeoDataFrame:
    raise NotImplementedError