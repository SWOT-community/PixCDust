from dataclasses import dataclass, field
from datetime import datetime

import xarray as xr
import geopandas as gpd



@dataclass
class PixCNcSimpleConstants:
    LONG_NAME: str = "longitude"
    LAT_NAME: str = "latitude"
    LOC_CYCLE_NUM_FILENAME: int = 4
    LOC_PASS_NUM_FILENAME: int = 5
    LOC_TILE_NUM_FILENAME: int = 6
    LOC_TIME_START_FILENAME: int = 7
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
    def extract_info_from_nc_filename(filename):
        cst = PixCNcSimpleConstants()
        filename_split = filename.split("_")
        time_start = filename_split[cst.LOC_TIME_START_FILENAME]

        # dt_time_start = datetime.strptime(
        #     time_start,
        #     cst.TIME_FORMAT_FILENAME
        # )

        cycle_number = int(filename_split[cst.LOC_CYCLE_NUM_FILENAME])

        pass_number = int(filename_split[cst.LOC_PASS_NUM_FILENAME])

        tile_number = filename_split[cst.LOC_TILE_NUM_FILENAME]
        return time_start, cycle_number, pass_number, tile_number

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
            if self.add_orbit_info:
                self.variables.extend(['tile_num', 'cycle_num', 'pass_num', 'time'])
            self.data = self.data[self.variables]

    @staticmethod
    def add_orbit_info(ds):
        cst = PixCNcSimpleConstants()
        filename = ds.encoding['source']
        ds_glob = xr.open_dataset(filename)
        ds['tile_num'] = ds_glob.attrs['tile_number']
        ds['pass_num'] = ds_glob.attrs['pass_number']
        ds['cycle_num'] = ds_glob.attrs['cycle_number']
        time_granule_start = ds_glob.attrs['time_granule_start']
        dt_time_start = datetime.strptime(
            time_granule_start,
            cst.TIME_FORMAT_ATTRS
        )
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
