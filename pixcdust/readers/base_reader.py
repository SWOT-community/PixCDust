"""Interface used by all Pixcdust Readers."""

import re
from typing import Optional, Iterable, Union, List
from pathlib import Path
import xarray as xr
import pandas as pd
import geopandas as gpd


PIXC_DATE_RE=re.compile(r'_\d{8}T\d{6}_\d{8}T\d{6}_')
"""Regex patern used to extract the date (daystartThourstart_dayxendThoursend) 
from a pixc file name.
"""

def sorted_by_date(file_list: Iterable[Union[str, Path]]) -> List[Union[str, Path]]:
    """Sort the filenames by date as some converters need monotonic dates.
    The date is parsed from the filename according to PIXC_DATE_RE.
    Args:
        file_list: List or iterable of pixc filenames.

    Returns:
        Sorted file_list.
    """
    def file_name_to_date(file_name: Union[str, Path]):
        date_founds = PIXC_DATE_RE.findall(str(file_name))
        if date_founds:
            return date_founds[-1]
        return file_name
    return sorted(file_list, key = file_name_to_date) # sort by date


class BaseReader:
    """Abstract class parent of pixcdust database readers.

    They read a database from a folder, file or list of files.
    You can then request a xr.Dataset, pd.DataFrame or gpd.GeoDataFrame
    view of the database.

    Attributes:
        path: Path or list of path to read.
        variables: Optionally only read these variables.
        area_of_interest: Optionally only read points in area_of_interest.
        MULTI_FILE_SUPPORT: Static variable describing if the class support opening a list of path.
    """
    MULTI_FILE_SUPPORT=False
    def __init__(self,
                 path: str | Iterable[str] | Path | Iterable[Path],
                 variables: Optional[list[str]] = None,
                 area_of_interest: Optional[gpd.GeoDataFrame] = None
                 ):
        """Basic pixcdust database reader configuration.

        Args:
            path: Path or list of path to read.
            variables: Optionally only read these variables.
            area_of_interest: Optionally only read points in area_of_interest.
        """
        if isinstance(path, str | Path):
            path = str(path)
            self.multi_file_db = False
        else:
            path = [str(p) for p in path]
            self.multi_file_db = True
            # sort the filenames by date as some converters need monotonic dates.
            path = sorted_by_date(path)
        if self.multi_file_db and not self.MULTI_FILE_SUPPORT:
            raise ValueError("This reader does not support opening multiple files.")
        self.path:  str | Iterable[str] = path
        self.area_of_interest = area_of_interest
        self._data: Optional[xr.Dataset] = None
        self.variables = variables

    @property
    def data(self) ->  xr.Dataset:
        """Return an xarray.Dataset view from the database loaded.

        Equivalent to to_xarray.

        Returns:
            Dataset read
        """
        return self._data

    @data.setter
    def data(self, obj: xr.Dataset) -> None:
        self._data = obj

    def to_xarray(self) -> xr.Dataset:
        """Return an xarray.Dataset view from the database loaded.

        Returns:
            Dataset read.
        """

        return self.data

    def to_dataframe(self) -> pd.DataFrame:
        """Return a pandas.DataFrame view from the database loaded.

        Returns:
            DataFrame read.
        """
        return self.data.to_dataframe()


    def to_geodataframe(
        self,
    ) -> gpd.GeoDataFrame:
        """Convert the database read to a gpd.GeoDataFrame.
        Only points in self.area_of_interest are included.

        Returns:
            GeoDataFrame read.
        """

        gdf = self.data.xvec.to_geodataframe()

        if self.area_of_interest is not None:
            gdf = gdf.overlay(self.area_of_interest, how="intersection")

        return gdf
