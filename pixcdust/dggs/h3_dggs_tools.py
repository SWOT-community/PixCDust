import h3
import healpy as hp
import numpy as np
import xarray as xr
from xarray import Dataset
import xdggs
from pixcdust.readers.netcdf import PixCNcSimpleReader
from scipy.interpolate import griddata


def prepare_dataset_h3(ds: Dataset, resolution: int) -> Dataset:
    """
    Convert an xarray.Dataset with latitude and longitude coordinates into an H3-indexed grid.

    This function computes H3 hexagonal grid indices for each latitude/longitude pair in the dataset,
    applies the H3 indexing, and interpolates the data to the new H3 grid. It also fills a bounding
    box with H3 indices and interpolates the dataset accordingly.

    Args:
        ds: The input dataset with latitude ('latitude') and longitude ('longitude') coordinates.
        resolution: The resolution of the H3 grid. Valid values are from 0 (coarse) to 15 (fine).

    Returns:
        The dataset interpolated onto the H3 grid, with 'cell_ids' (H3 indices) as the primary dimension.
    """
    lon = ds.longitude
    lat = ds.latitude

    # Compute H3 index for each latitude and longitude pair
    geo_to_h3_vec = np.vectorize(h3.geo_to_h3)
    index = geo_to_h3_vec(lat.values, lon.values, resolution)
    index.shape = lon.shape

    # Add the computed H3 index as a new coordinate to the dataset
    ds.coords["index"] = "points", index

    # Compute the bounding box for the dataset in lat/lon
    lon_min, lon_max = ds.longitude.min().values.item(), ds.longitude.max().values.item()
    lat_min, lat_max = ds.latitude.min().values.item(), ds.latitude.max().values.item()

    # Define the bounding box coordinates
    bbox_coords = [
        (lon_min, lat_min),
        (lon_min, lat_max),
        (lon_max, lat_max),
        (lon_max, lat_min),
        (lon_min, lat_min),
    ]

    # H3 expects latitudes first, so reorder bbox coordinates
    bbox_coords_lat_first = [(lat, lon) for lon, lat in bbox_coords]

    # Use polyfill to generate H3 indices within the bounding box
    bbox_indexes = np.array(
        list(h3.api.basic_int.polyfill_polygon(bbox_coords_lat_first, resolution))
    )

    # Convert the H3 indices back to lat/lon coordinates
    ll_points = np.array([h3.api.basic_int.h3_to_geo(i) for i in bbox_indexes])

    # Prepare the new H3-indexed coordinates
    coords = {"cell_ids": bbox_indexes}

    # Interpolate the dataset to the new H3 grid
    # Interpolate for each data variable in the dataset
    interpolated_data = {}
    for var in ds.data_vars:
        # Retrieve the variable values and the corresponding coordinates
        values = ds[var].values

        # Interpolate the values onto the H3 grid
        interpolated_values = griddata(
            points=(lat, lon),
            values=values,
            xi=ll_points,
            method='linear'
        )
        interpolated_data[var] = interpolated_values

    ds_h3 = xr.Dataset(
        {var: (('cell_ids',), interpolated_data[var]) for var in interpolated_data},
        coords=coords,
        attrs=ds.attrs
    )

    ds_h3.cell_ids.attrs = {"grid_name": "h3", "resolution": resolution}
    ds_h3 = ds_h3.pipe(xdggs.decode)
    return ds_h3


def prepare_dataset_healpix(ds: Dataset, resolution: int=8) -> Dataset:
    nside = hp.order2nside(resolution)
    npix = hp.nside2npix(nside)

    # Get HEALPix pixel centers
    healpix_coords = hp.pix2ang(nside, np.arange(npix), lonlat=True)
    healpix_lon, healpix_lat = healpix_coords

    lats = ds['latitude'].values
    lons = ds['longitude'].values

    interp_points = np.vstack((healpix_lon, healpix_lat)).T
    # Interpolate for each data variable in the dataset
    interpolated_data = {}
    for var in ds.data_vars:
        # Retrieve the variable values and the corresponding coordinates
        values = ds[var].values

        # Interpolate the values onto the HEALPix grid
        interpolated_values = griddata(
            points=(lons, lats),
            values=values,  # Your original data
            xi=interp_points,  # The HEALPix grid centers
            method='linear'  # You can also try 'nearest' or 'cubic'
        )
        interpolated_data[var] = interpolated_values

    coords = {
        'cell_ids': np.arange(npix),  # Par exemple, les IDs des cellules HEALPix
        'healpix_lon': ('cell_ids', healpix_lon),
        'healpix_lat': ('cell_ids', healpix_lat)
    }
    ds_healpix = xr.Dataset(
        {var: (('cell_ids',), interpolated_data[var]) for var in interpolated_data},
        coords=coords,
        attrs=ds.attrs
    )
    ds_healpix = ds_healpix.dropna(dim="cell_ids", how="any")

    ds_healpix.cell_ids.attrs = {
        "grid_name": "healpix",
        "nside": nside,
        "nest": True,
    }
    if "cell_ids" in ds_healpix.indexes:
        ds_healpix = ds_healpix.reset_index("cell_ids")
    ds_healpix = ds_healpix.set_xindex("cell_ids", xdggs.DGGSIndex)
    ds_healpix.pipe(xdggs.decode)

    return ds_healpix


if __name__ == "__main__":
    ds = xr.tutorial.load_dataset("air_temperature").load()
    ds = ds.rename({'lat': 'latitude', 'lon': 'longitude'})
    path = "/home/vschaffn/Documents/swot_data/pixc/SWOT_L2_HR_PIXC_482_016_077L_20230406T094608_20230406T094619_PGC0_01.nc"
    reader = PixCNcSimpleReader(path)
    reader.open_dataset()
    ds = reader.to_xarray()
    new_ds = ds[['height']]
    dsi = prepare_dataset_h3(new_ds, 8)    # dsi = prepare_dataset_h3(ds, resolution=11)
    print(dsi)