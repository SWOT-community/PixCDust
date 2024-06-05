from datetime import datetime
import xarray as xr
import numpy as np
from pixcdust.readers.netcdf import PixCNcSimpleConstants


def mock_netcdf_xarray(length: int = 10000) -> xr.Dataset:
    """mocks an xarray extracted from a typical SWOT PixC netcdf file
    and enhanced with PixCNcSimpleReader and orbit infos

    Args:
        length (int, optional): length of the array. Defaults to 10000.

    Returns:
        xr.Dataset: dataset with some typical variables
    """
    cst = PixCNcSimpleConstants()
    dims = [cst.default_dim_name]
    coord_step = 0.001
    coords = {
        cst.default_long_name: np.linspace(
            10., 10.+length*coord_step, length,
            dtype=np.float64,
        ),
        cst.default_lat_name: np.linspace(
            40., 40.+length*coord_step, length,
            dtype=np.float64,
        )
    }
    x = coords[cst.default_lat_name]
    data_vars = {
        'height': (dims, np.sin(x) + np.random.normal(scale=0.1, size=len(x))),
        'sig0': (dims, np.random.uniform(low=0.5, high=90., size=(len(x),))),
        'classification': (
            dims,
            np.random.randint(
                low=0, high=7, size=(len(x),),
            ).astype(np.float32),
        ),
        cst.default_tile_num_name: (
            dims,
            np.random.randint(1, 200)*np.ones_like(x, dtype=np.uint16),
        ),
        cst.default_cyc_num_name: (
            dims,
            np.random.randint(1, 500)*np.ones_like(x, dtype=np.uint16),
        ),
        cst.default_pass_num_name: (
            dims,
            np.random.randint(1, 500)*np.ones_like(x, dtype=np.uint16),
        ),
        cst.default_added_time_name: (
            dims,
            np.array([datetime(2024, 6, 5, 11, 40, 12) for i in range(len(x))])
        ),
    }

    return xr.Dataset(data_vars=data_vars, coords=coords)
