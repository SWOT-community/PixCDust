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
"""Downloaders for hydroweb.next. Require an API-Key see HELP_MESSAGE."""

import datetime
import os
import zipfile
from abc import ABC, abstractmethod
from pathlib import Path

import geopandas
import py_hydroweb
import shapely
import tqdm
from eodag import EODataAccessGateway, SearchResult, setup_logging

HELP_MESSAGE = """
Download products from hydroweb.next (https://hydroweb.next.theia-land.fr)
using py_hydroweb (https://pypi.org/project/py-hydroweb/)

Follow these steps:
1a. Generate an API-Key from hydroweb.next portal in your user settings
1b. Carefully store your API-Key in an environment variable 
`export HYDROWEB_API_KEY="PLEASE_CHANGE_ME"`
2. You can change download directory by modifying the variable path_out.
By default, current path is used.

For more information, please refer to py_hydroweb Documentation 
https://pypi.org/project/py-hydroweb/
"""


class Downloader(ABC):
    """Downloader class for hydroweb.next STAC API.

    Attributes:
        collection_name: Name of the collection in hydroweb.next catalog
        geometry: Geometry used as search criteria. Defaults to None.
        dates: Minimum and maximum dates to be used as search criteria. Defaults to None.
        path_download: Download path. Defaults to "/tmp/hydroweb_next".
        query_args: Query filters to request from hydroweb.next generated from parameters.
        search_results: Products founds matching the query_args (and downloaded).
        dag: Hydroweb.next API



    """

    PROVIDER = "hydroweb_next"

    def __init__(
        self,
        collection_name: str,
        geometry: str | list[str] | geopandas.GeoDataFrame | None = (None,),
        dates: tuple[datetime.date, datetime.date] | None = None,
        path_download: str | Path = "/tmp/hydroweb_next",
        verbose: int | None = 0,
    ):
        """Downloader for hydroweb.next STAC API initialization.

        Args:
            collection_name: Name of the collection in hydroweb.next catalog.
            geometry: A geometry used as search criteria. Defaults to None.
            dates: Minimum and maximum dates to be used as search criteria.
                Defaults to None.
            path_download:
                download path. Defaults to "/tmp/hydroweb_next".
            verbose: Verbose level (0: nothing, 1: only progress bars, 2: INFO, 3: DEBUG).
                Defaults to 0.

        Raises:
            AttributeError: if the geometry is not one
                of (str, tuple, list, geopandas.GeoDataFrame)
        """
        self.collection_name = collection_name
        self.geometry = geometry
        self.dates = dates
        self.path_download = str(path_download)
        self.verbose = verbose

        self.query_args = {}
        self.search_results: list[SearchResult] = []

        if not os.path.isdir(self.path_download):
            os.mkdir(self.path_download)

        self.setup()
        self.query_args = self.define_query()

    @staticmethod
    def _explode_simplify_geometry(
        geometry: geopandas.GeoDataFrame, tolerance: float | None = None
    ) -> geopandas.GeoDataFrame:
        """this method explodes geodataframe containing multipolygons
        into single polygons. It allows to simplify the polygons in order to
        descrease their number of nodes. It also checks the number of nodes
        in the polygon in case it goes over a threshold

        Args:
            geometry (geopandas.GeoDataFrame): a geodataframe containing search
                polygons of multipolygons
            tolerance (float | None, optional): Maximum tolerance of the geometry simplification.
                Defaults to None.
                All parts of a simplified geometry will be no more than
                `tolerance` distance from the original. It has the same units
                as the coordinate reference system of the GeoSeries.

        Raises:
            AttributeError: if the number of nodes in a single polygon
                is over 200

        Returns:
            geopandas.GeoDataFrame: exploded geodataframe with simplified polygons
                if required
        """
        geom = geometry.explode(index_parts=True)

        if tolerance:
            geom["geometry"] = geom.geometry.simplify(
                tolerance=tolerance,
            )
        # verifying the number of nodes in each polygon
        geom["nodes_count"] = geom.apply(
            lambda row: len(row.geometry.exterior.coords),
            axis=1,
        )
        if (geom["nodes_count"] > 200).any():
            raise AttributeError(
                "One or several of your search polygons have too many nodes,"
                "consider using the tolerance parameter"
                "in order to simplify the polygons."
            )

        return geom

    def search_download(self, tolerance: float | None = None) -> None:
        """Search files according to the query and download them.

        Args:
            tolerance: Maximum tolerance of the geometry simplification.
                Cf `self._explode_simplify_geometry`.

        """
        if self.geometry is None:
            self._search(self.geometry)
        elif isinstance(self.geometry, str):
            geom = shapely.from_wkt(self.geometry)
            self._search(geom.__geo_interface__)
        elif isinstance(self.geometry, geopandas.GeoDataFrame):
            geometries = self._explode_simplify_geometry(
                self.geometry,
                tolerance,
            )
            for geom in geometries.geometry.values:
                self._search(geom.__geo_interface__)
        else:
            raise AttributeError(
                "geometry should string (WKT) or geopandas.GeoDataFrame, "
                f"received {type(self.geometry)} instead"
            )

        # This command actually downloads the matching products
        downloaded_paths = self._download()

        if not downloaded_paths:
            print(
                f"No files downloaded! Verify API-KEY and/or product search configuration. {self.search_results}"
            )

    @abstractmethod
    def setup(self) -> None:
        pass

    @abstractmethod
    def define_query(self) -> dict:
        pass

    @abstractmethod
    def _search(self, geom: str | None = None) -> None:
        pass

    @abstractmethod
    def _download(self) -> list:
        pass


class EODownloader(Downloader):
    """Downloader for SWOT Pixel Cloud files from  hydroweb.next."""

    def __init__(self, *args, **kwargs):
        """Downloader for SWOT Pixel Cloud files from  hydroweb.next initialization.

        Keyword Args:
            geometry: A geometry used as search criteria. Defaults to None.
            dates: Minimum and maximum dates to be used as search criteria.
                Defaults to None.
            path_download:
                download path. Defaults to "/tmp/hydroweb_next".
            verbose: Verbose level (0: nothing, 1: only progress bars, 2: INFO, 3: DEBUG).
                Defaults to 0.

        Raises:
            AttributeError: if the geometry is not one
                of (str, tuple, list, geopandas.GeoDataFrame)
        """
        super().__init__(*args, **kwargs)

    def setup(self) -> None:
        self.dag = EODataAccessGateway()

        setup_logging(
            self.verbose
        )  # 0: nothing, 1: only progress bars, 2: INFO, 3: DEBUG

        # Set timeout to 30s
        os.environ["EODAG__HYDROWEB_NEXT__SEARCH__TIMEOUT"] = "30"

        self.__check_collection_name()

    def __check_collection_name(self) -> None:
        list_collections = [
            d.id for d in self.dag.list_collections(provider=self.PROVIDER)
        ]

        if self.collection_name not in list_collections:
            raise ValueError(
                "Did not find collection_name in "
                f"list of available collections in {self.PROVIDER}."
                f"\nAvailable collections are: {list_collections}"
            )

    def define_query(self) -> dict:
        # Default search criteria when iterating over collection pages
        default_search_criteria = {
            "limit": 2000,
            "provider": self.PROVIDER,
        }

        self.query_args = {
            "collection": self.collection_name,
        }

        if self.dates is not None:
            self.query_args["start"] = self.dates[0].strftime("%Y-%m-%dT%H:%M:%SZ")
            self.query_args["end"] = self.dates[1].strftime("%Y-%m-%dT%H:%M:%SZ")

        self.query_args.update(default_search_criteria)

        return self.query_args

    def _search(self, geom: str | None = None) -> None:
        if geom is not None:
            self.query_args["geom"] = geom

        self.search_results = self.dag.search_all(**self.query_args)
        # Iterate over all pages to find all products
        # for page_results in self.dag.search_iter_page(**self.query_args):
        #    self.search_results.extend(page_results)

    def _download(self) -> list:
        # donwload only .nc asset
        downloaded_paths = self.dag.download_all(
            self.search_results, asset=r".*\.nc$", output_dir=self.path_download
        )
        return downloaded_paths


class DefaultDownloader(Downloader):
    """Downloader for SWOT Pixel Cloud files from  hydroweb.next."""

    def __init__(self, *args, **kwargs):
        """Downloader for SWOT Pixel Cloud files from  hydroweb.next initialization.

        Keyword Args:
            geometry: A geometry used as search criteria. Defaults to None.
            dates: Minimum and maximum dates to be used as search criteria.
                Defaults to None.
            path_download:
                download path. Defaults to "/tmp/hydroweb_next".
            verbose: Verbose level (0: nothing, 1: only progress bars, 2: INFO, 3: DEBUG).
                Defaults to 0.

        Raises:
            AttributeError: if the geometry is not one
                of (str, tuple, list, geopandas.GeoDataFrame)
        """
        super().__init__(*args, **kwargs)

    def setup(self) -> None:
        apikey = os.environ["HYDROWEB_API_KEY"]
        self.client = py_hydroweb.Client(api_key=apikey)
        self.downloaded_paths = []

    def define_query(self) -> dict:
        if self.dates is not None:
            self.query_args["start_datetime"] = {
                "gte": self.dates[0].isoformat(timespec="milliseconds")
            }
            self.query_args["end_datetime"] = {
                "lte": self.dates[1].isoformat(timespec="milliseconds")
            }

        return self.query_args

    def _search(self, geom: str | None = None) -> None:
        # This command actually downloads the matching products
        basket = py_hydroweb.DownloadBasket("pixcdust_basket")

        kwargs = {
            "collection_id": self.collection_name,
            "query": self.query_args,
            "folder": self.collection_name,
        }
        if geom is not None:
            kwargs.update({"intersects": geom})
        basket.add_collection(**kwargs)

        self.download_id = self.client.submit_download(download_basket=basket)

    def _download(self) -> list:
        downloaded_zip_path = self.client.download_zip(
            download_id=self.download_id, output_folder=self.path_download
        )

        with zipfile.ZipFile(downloaded_zip_path, "r") as zf:
            # Liste des chemins à extraire
            files = [name for name in zf.namelist() if name.lower().endswith(".nc")]
            for member in tqdm.tqdm(files, desc="Extracting zip"):
                zf.extract(member, path=self.path_download)
                downloaded_path = os.path.join(self.path_download, member)
                self.downloaded_paths.append(downloaded_path)

        os.remove(downloaded_zip_path)

        self.client.delete_download(download_id=self.download_id)

        return self.downloaded_paths


def PixCDownloader(*args, backend="default", **kwargs):
    """Downloader for SWOT Pixel Cloud files from  hydroweb.next initialization.

    Keyword Args:
        geometry: A geometry used as search criteria. Defaults to None.
        dates: Minimum and maximum dates to be used as search criteria.
            Defaults to None.
        path_download:
            download path. Defaults to "/tmp/hydroweb_next".
        verbose: Verbose level (0: nothing, 1: only progress bars, 2: INFO, 3: DEBUG).
            Defaults to 0.

    Raises:
        AttributeError: if the geometry is not one
            of (str, tuple, list, geopandas.GeoDataFrame)
    """
    if backend == "eodag":
        print(f"using backend: {backend}")
        return EODownloader("SWOT_L2_HR_PIXC", *args, **kwargs)
    else:
        print("using default backend: py-hydroweb")
        return DefaultDownloader("SWOT_L2_HR_PIXC", *args, **kwargs)
