
import geopandas as gpd
import h3
import h3.unstable.vect
import h3.api.numpy_int

from shapely.geometry import Polygon


def cell_to_shapely(cell):
    coords = h3.h3_to_geo_boundary(cell)
    flipped = tuple(coord[::-1] for coord in coords)
    return Polygon(flipped)


def get_h3_res_name(res: int):
    return "h3_" + str(res).zfill(2)


def gdf_to_h3_gdf(
    gdf: gpd.GeoDataFrame,
    resolution: int,
    var: str,
        ) -> gpd.GeoDataFrame:

    h3_col = get_h3_res_name(resolution)

    gdf[h3_col] = gdf.apply(
        lambda row: str(h3.geo_to_h3(
            row.geometry.y,
            row.geometry.x,
            resolution
            )),
        axis=1,
        )

    # compute statistics in each H3 cell in a new dataframe
    h3_df = gdf.groupby(h3_col)[var].describe().reset_index()

    # add the geometry of each H3 cell in a new geodataframe
    h3_geoms = h3_df[h3_col].apply(lambda x: cell_to_shapely(x))
    # copy the geometries in the previous statiscal dataframe
    return gpd.GeoDataFrame(data=h3_df, geometry=h3_geoms, crs=4326)