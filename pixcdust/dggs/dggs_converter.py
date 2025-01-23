#
# Copyright (C) 2024 Centre National d'Etudes Spatiales (CNES)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#

import h3
import healpy as hp
import numpy as np
import xarray as xr
from xarray import Dataset
import xdggs
from scipy.interpolate import griddata


def prepare_dataset_h3(ds: Dataset, resolution: int, interp: bool=False, method: str = 'linear') -> Dataset:
    """
    Convert a Dataset with latitude and longitude coordinates into an H3-indexed grid.

    This function computes H3 hexagonal grid indices for each latitude/longitude pair in the dataset,
    applies the H3 indexing, and interpolates the data to the new H3 grid. It also fills a bounding
    box with H3 indices and interpolates the dataset accordingly.

    Args:
        ds: The input dataset with latitude ('latitude') and longitude ('longitude') coordinates.
        resolution: The resolution of the H3 grid. Valid values are from 0 (coarse) to 15 (fine).
        method: ('nearest', 'linear', 'cubic') The interpolation method used by`scippy.interpolate.griddata`.

    Returns:
        A new dataset with data variables interpolated onto the H3 grid. The output dataset includes:
        - `h3_lon`: longitudes of H3 grid centers.
        - `h3_lat`: latitudes of H3 grid centers.
        - `cell_ids`: unique H3 pixel indices.
        The dataset retains any global attributes from the original dataset and stores additional metadata on the
        H3 grid.
    """
    lon = ds.longitude
    lat = ds.latitude

    if interp:
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
        h3_indices = np.array(
            list(h3.api.basic_int.polyfill_polygon(bbox_coords_lat_first, resolution))
        )

        # Convert the H3 indices back to lat/lon coordinates
        ll_points = np.array([h3.api.basic_int.h3_to_geo(i) for i in h3_indices])

        # Interpolate the dataset to the new H3 grid
        # Interpolate for each data variable in the dataset
        data = {}
        for var in ds.data_vars:
            # Retrieve the variable values and the corresponding coordinates
            values = ds[var].values

            # Interpolate the values onto the H3 grid
            interpolated_values = griddata(
                points=(lat, lon),
                values=values,
                xi=ll_points,
                method=method
            )
            data[var] = interpolated_values

    else:
        # Compute H3 index for each point in the dataset
        h3_indices = np.array([h3.api.basic_int.geo_to_h3(lat_, lon_, resolution) for lat_, lon_ in zip(lat.values, lon.values)])
        h3_data = dict()
        for h3_id in np.unique(h3_indices):
            h3_data[h3_id] = []

        ll_points = np.array([h3.api.basic_int.h3_to_geo(i) for i in np.unique(h3_indices)])

        for var in ds.data_vars:
            values = ds[var].values
            for idx, h3_id in enumerate(h3_indices):
                h3_data[h3_id].append(values[idx])

        data = {var: np.array([np.mean(np.array(h3_data[h3_id])) for h3_id in h3_data]) for var in ds.data_vars}

    coords = {
        "cell_ids": np.unique(h3_indices),
        'h3_lon': ('cell_ids', ll_points[:, 1]),
        'h3_lat': ('cell_ids', ll_points[:, 0])
    }

    ds_h3 = xr.Dataset(
        {var: (('cell_ids',), data[var]) for var in data},
        coords=coords,
        attrs=ds.attrs
    )

    ds_h3.cell_ids.attrs = {"grid_name": "h3", "resolution": resolution}
    ds_h3 = ds_h3.pipe(xdggs.decode)
    return ds_h3


def prepare_dataset_healpix(ds: Dataset, resolution: int = 8, nest: bool = False, interp: bool=False, method: str = 'linear') -> Dataset:
    """
    Convert a Dataset with latitude and longitude coordinates into an HEALPix-indexed grid.

    This function computes Healpix grid indices for each latitude/longitude pair in the dataset,
    applies the HEALPix indexing, and interpolates the data to the new HEALPix grid.

    Args:
        ds: The input dataset with latitude ('latitude') and longitude ('longitude') coordinates.
        resolution: The resolution of the HEALPix grid.
        nest: If True, uses the nested HEALPix ordering scheme. Otherwise, uses the ring ordering scheme (default)
        method: ('nearest', 'linear', 'cubic') The interpolation method used by`scippy.interpolate.griddata`.

    Returns:
        A new dataset with data variables interpolated onto the HEALPix grid. The output dataset includes:
        - `healpix_lon`: longitudes of HEALPix grid centers.
        - `healpix_lat`: latitudes of HEALPix grid centers.
        - `cell_ids`: unique HEALPix pixel indices.
        The dataset retains any global attributes from the original dataset and stores additional metadata on the
        HEALPix grid.
    """
    nside = hp.order2nside(resolution)
    # Get HEALPix pixel centers
    lats = ds['latitude'].values
    lons = ds['longitude'].values
    pix_indices = np.array(hp.ang2pix(nside, lons, lats, nest=nest, lonlat=True))
    healpix_lon, healpix_lat = hp.pix2ang(nside, np.unique(pix_indices), nest=nest, lonlat=True)

    if interp:
        pix_indices = np.unique(pix_indices)

        interp_points = np.vstack((healpix_lon, healpix_lat)).T

        # Interpolate for each data variable in the dataset
        data = {}
        for var in ds.data_vars:
            # Retrieve the variable values and the corresponding coordinates
            values = ds[var].values

            # Interpolate the values onto the HEALPix grid
            interpolated_values = griddata(
                points=(lons, lats),
                values=values,
                xi=interp_points,
                method=method
            )
            data[var] = interpolated_values

    else:
        healpix_data = dict()
        for h3_id in np.unique(pix_indices):
            healpix_data[h3_id] = []

        for var in ds.data_vars:
            values = ds[var].values
            for idx, h3_id in enumerate(pix_indices):
                healpix_data[h3_id].append(values[idx])

        data = {var: np.array([np.mean(np.array(healpix_data[h3_id])) for h3_id in healpix_data]) for var in ds.data_vars}

    coords = {
        'cell_ids': np.unique(pix_indices),
        'healpix_lon': ('cell_ids', healpix_lon),
        'healpix_lat': ('cell_ids', healpix_lat)
    }
    ds_healpix = xr.Dataset(
        {var: (('cell_ids',), data[var]) for var in data},
        coords=coords,
        attrs=ds.attrs
    )

    ds_healpix.cell_ids.attrs = {
        "grid_name": "healpix",
        "nside": nside,
        "nest": nest,
    }
    if "cell_ids" in ds_healpix.indexes:
        ds_healpix = ds_healpix.reset_index("cell_ids")
    ds_healpix = ds_healpix.set_xindex("cell_ids", xdggs.DGGSIndex)
    ds_healpix.pipe(xdggs.decode)

    return ds_healpix
