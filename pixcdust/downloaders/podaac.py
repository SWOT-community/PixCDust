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
"""Downloaders for podacc. Require an API-Key see HELP_MESSAGE."""

import earthaccess
import os
import geopandas
import shapely
from shapely.geometry import shape
from datetime import date, datetime, time
from pathlib import Path

from pixcdust.downloaders.hydroweb_next import Downloader


HELP_MESSAGE = """
Download products from podaac
"""


class PodaacDownloader(Downloader):
    """Downloader for SWOT PIXC products from NASA PO.DAAC."""

    PROVIDER = "podaac"

    def __init__(self, *args, **kwargs):
        """Downloader for SWOT Pixel Cloud files from podaac.

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
        """Initialize the Earthdata session."""
        # Test if token is ok
        token = os.environ.get("EARTHDATA_TOKEN")

        if not token:
            raise RuntimeError(
                "Earthdata token required. "
                "Please set the EARTHDATA_TOKEN environment variable."
            )

        self.auth = earthaccess.login(strategy="environment")
        self.downloaded_paths: list[str] = []

    def define_query(self) -> dict:
        """Build temporal search parameters."""
        query = {}

        if self.dates is not None:
            start_date, end_date = self.dates
            query["temporal"] = (
                self._format_date(start_date, end=False),
                self._format_date(end_date, end=True),
            )

        return query

    @staticmethod
    def _format_date(value: date | datetime, *, end: bool) -> str:
        """Format a date for Earthaccess temporal searches."""
        if isinstance(value, datetime):
            return value.isoformat(timespec="seconds")

        boundary = time.max if end else time.min
        return datetime.combine(value, boundary).isoformat(timespec="seconds")

    def _search(self, geom=None) -> None:
        """Search granules intersecting the requested geometry."""


        kwargs = {
            "short_name": self.collection_name,
            **self.query_args,
        }

        if geom is not None:
            geometry = shape(geom) if isinstance(geom, dict) else shapely.from_wkt(geom)

            # earthaccess expects coordinates in longitude/latitude.
            if not geometry.is_empty:
                if geometry.geom_type == "Polygon":
                    kwargs["polygon"] = list(geometry.exterior.coords)
                elif geometry.geom_type == "MultiPolygon":
                    # Search each polygon separately; search_download()
                    # already handles GeoDataFrames one geometry at a time.
                    kwargs["polygon"] = list(
                        max(geometry.geoms, key=lambda polygon: polygon.area)
                        .exterior.coords
                    )
                else:
                    raise ValueError(
                        f"Unsupported geometry type: {geometry.geom_type}"
                    )

        results = earthaccess.search_data(**kwargs)

        # A GeoDataFrame may contain multiple polygons. Avoid duplicates.
        existing_ids = {
            result["meta"]["concept-id"]
            for result in self.search_results
            if isinstance(result, dict) and "meta" in result
        }

        for result in results:
            metadata = result.get("meta", {}) if isinstance(result, dict) else {}
            concept_id = metadata.get("concept-id")

            if concept_id is None or concept_id not in existing_ids:
                self.search_results.append(result)
                if concept_id is not None:
                    existing_ids.add(concept_id)

    def _download(self) -> list[str]:
        """Download the matching granules and return local NetCDF paths."""
        if not self.search_results:
            return []

        paths = earthaccess.download(
            self.search_results,
            local_path=self.path_download,
        )

        # Filter on .nc file
        self.downloaded_paths = [
            str(Path(path))
            for path in paths
            if Path(path).suffix.lower() == ".nc"
        ]


def PodaacPixCDownloader(*args, **kwargs):
    """Downloader for SWOT Pixel Cloud files from  PO.DAAC

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
    return PodaacDownloader("SWOT_L2_HR_PIXC_D", *args, **kwargs)
    
    

