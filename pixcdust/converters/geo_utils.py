import xarray as xr
import geopandas as gpd


def geoxarray_to_geodataframe(
    ds: xr.Dataset,
        *args, **kwargs) -> gpd.GeoDataFrame:
    """converts an xarray.Dataset with points coordinates  into\
        a geopandas.GeodataFrame with xvec

    Args:
        ds (xr.Dataset): Dataset with geometry points coordinates


    Returns:
        gpd.GeoDataFrame: a geodataframe with information from file
    """

    return ds.xvec.to_geodataframe(*args, **kwargs)
