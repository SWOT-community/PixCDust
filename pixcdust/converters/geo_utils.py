import xarray as xr
import geopandas as gpd


def geoxarray_to_geodataframe(
    ds: xr.Dataset,
    crs: str | int = 4326,
    long_name: str = 'longitude',
    lat_name: str = 'latitude',
    area_of_interest: gpd.GeoDataFrame = None,
        ) -> gpd.GeoDataFrame:
    """converts an xarray.Dataset with long/lat variables into\
        a geopandas.GeodataFrame

    Args:
        ds (xr.Dataset): Dataset with longitude/latitude-like variables
        crs (str | int, optional): Coordinate Reference System.\
            Defaults to 4326.
        long_name (str, optional): _description_. Defaults to 'longitude'.
        lat_name (str, optional): _description_. Defaults to 'latitude'.
        area_of_interest (gpd.GeoDataFrame, optional):  a geodataframe\
            containing polygons of interest where data will be restricted.\
            Defaults to None.

    Returns:
        gpd.GeoDataFrame: a geodataframe with information from file
    """

    df = ds.to_dataframe()

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(
            df[long_name],
            df[lat_name],
            ),
        crs=crs,
    )
    if area_of_interest is not None:
        gdf = gdf.overlay(area_of_interest, how="intersection")

    return gdf
