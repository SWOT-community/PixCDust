from typing import Optional, Iterable
from pathlib import Path
import xarray as xr
import pandas as pd
import geopandas as gpd



class BaseReader:
    """Class to read a database from path
    """
    MULTI_FILE_SUPPORT=False
    def __init__(self,
                 path: str | Iterable[str] | Path | Iterable[Path],
                 variables: Optional[list[str]] = None,
                 area_of_interest: Optional[gpd.GeoDataFrame] = None
                 ):
        if isinstance(path, str | Path):
            path = str(path)
            self.multi_file_db = False
        else:
            path = [str(p) for p in path]
            self.multi_file_db = True
        if self.multi_file_db and not self.MULTI_FILE_SUPPORT:
            raise ValueError("This reader does not support opening multiple files.")
        self.path:  str | Iterable[str] = path
        self.area_of_interest = area_of_interest
        self.data: Optional[xr.Dataset] = None
        self.variables = variables

    def to_xarray(self) -> xr.Dataset:
        """returning an xarray.Dataset from object
        (this function exists for potential future compatibility)

        Returns:
            xr.Dataset: Dataset with information from file
        """

        return self.data

    def to_dataframe(self) -> pd.DataFrame:
        """returns a pandas.DataFrame from object

        Returns:
            pd.DataFrame: Dataframe with information from file
        """
        return self.data.to_dataframe()


    def to_geodataframe(
        self,
    ) -> gpd.GeoDataFrame:
        """

        Returns:
            gpd.GeoDataFrame: a geodataframe with information from file
        """

        gdf = self.data.xvec.to_geodataframe()

        if self.area_of_interest is not None:
            gdf = gdf.overlay(self.area_of_interest, how="intersection")

        return gdf