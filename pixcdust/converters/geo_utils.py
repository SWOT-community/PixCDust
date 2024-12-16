"""Converters utility"""

import xarray as xr
import geopandas as gpd


def geoxarray_to_geodataframe(
    ds: xr.Dataset,
        *args, **kwargs) -> gpd.GeoDataFrame:
    """Converts an xarray.Dataset with points coordinates  into\
        a geopandas.GeodataFrame with xvec

    Args:
        ds: Dataset with geometry points coordinates
        args: Cf xvec.to_geodataframe
        kwargs: Cf xvec.to_geodataframe


    Returns:
        A geodataframe with information from ds.
    """

    return ds.xvec.to_geodataframe(*args, **kwargs)
