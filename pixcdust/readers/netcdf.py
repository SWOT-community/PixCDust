from dataclasses import dataclass, field
from datetime import datetime

import xarray as xr
import geopandas as gpd


@dataclass
class PixCNcSimpleConstants:
    LONG_NAME: str = "longitude"
    LAT_NAME: str = "latitude"
    CYCLE_NUM_NAME: str = 'cycle_number'
    PASS_NUM_NAME: str = 'pass_number'
    TILE_NUM_NAME: str = 'tile_number'
    TIME_START_NAME: str = 'time_granule_start'
    TIME_FORMAT_FILENAME: str = "%Y%m%dT%H%M%S"
    TIME_FORMAT_ATTRS: str = '%Y-%m-%dT%H:%M:%S.%fZ'


@dataclass
class PixCNcSimpleReader:
    """
    class for reading SWOT Pixel cloud official format files reader
    for most simple uses cases:
    It only reads in the pixel_cloud group
    """

    path: list[str] | str
    variables: list[str] = None

    trusted_group: str = "pixel_cloud"
    forbidden_variables: list[str] = field(
        default_factory=lambda: [
            "pixc_line_qual",
            "pixc_line_to_tvp",
            "data_window_first_valid",
            "data_window_last_valid",
            "data_window_first_cross_track",
            "data_window_last_cross_track",
            "interferogram",
        ]
    )
    data: xr.Dataset = None

    @staticmethod
    def extract_info_from_nc_attrs(filename):
        """missing docstring"""
        cst = PixCNcSimpleConstants()

        with xr.open_dataset(filename) as ds_glob:
            tile_number = ds_glob.attrs[cst.TILE_NUM_NAME]
            pass_number = ds_glob.attrs[cst.PASS_NUM_NAME]
            cycle_number = ds_glob.attrs[cst.CYCLE_NUM_NAME]
            time_granule_start = ds_glob.attrs[cst.TIME_START_NAME]
            dt_time_start = datetime.strptime(
                time_granule_start,
                cst.TIME_FORMAT_ATTRS
            )

        return time_granule_start, dt_time_start, \
            cycle_number, pass_number, tile_number

    def open_dataset(self):
        """
        reads one pixc file and returns an xarray
        """
        self.data = xr.open_dataset(
            self.path,
            group=self.trusted_group,
            engine="netcdf4",
        )
        if self.variables:
            self.data = self.data[self.variables]

    def open_mfdataset(self, orbit_info: bool = False):
        """
        reads one or multiple pixc files and returns a nested xarray
        In this case, variables that are not one-dimensional
        along `points` dimension are not allowed and will be dropped:
            - 'pixc_line_qual',
            - 'pixc_line_to_tvp',
            - 'interferogram'
        """

        if not orbit_info:
            self.data = xr.open_mfdataset(
                self.path,
                group=self.trusted_group,
                engine="netcdf4",
                drop_variables=self.forbidden_variables,
                combine="nested",
                concat_dim="points",
            )
        else:
            self.data = xr.open_mfdataset(
                self.path,
                group=self.trusted_group,
                engine="netcdf4",
                drop_variables=self.forbidden_variables,
                combine="nested",
                concat_dim="points",
                preprocess=self.add_orbit_info,
            )

        if self.variables:
            # TODO: check if variables in forbidden variables before loading
            if self.orbit_info:
                self.variables.extend(['tile_num', 'cycle_num', 'pass_num', 'time'])
            self.data = self.data[self.variables]

    def add_orbit_info(self, ds):
        """missing docstring"""
        filename = ds.encoding['source']

        _, dt_time_start, cycle_number, \
            pass_number, tile_number = self.extract_info_from_nc_attrs(filename)

        ds['tile_num'] = tile_number
        ds['pass_num'] = pass_number
        ds['cycle_num'] = cycle_number
        ds['time'] = dt_time_start

        return ds

    def to_xarray(self):
        """missing docstring"""
        return self.data

    def to_dataframe(self):
        """missing docstring"""
        return self.data.to_dataframe()

    def to_geodataframe(
            self,
            crs=4326,
            area_of_interest: gpd.GeoDataFrame = None,
        ) -> gpd.GeoDataFrame:
        """missing docstring"""

        cst = PixCNcSimpleConstants()
        df = self.to_dataframe()

        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df[cst.LONG_NAME], df[cst.LAT_NAME]),
            crs=crs,
        )
        if area_of_interest is not None:
            gdf = gdf.overlay(area_of_interest, how="intersection")

        return gdf
