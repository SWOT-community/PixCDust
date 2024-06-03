"""
Download products from hydroweb.next \(https://hydroweb.next.theia-land.fr) using EODAG (https://github.com/CS-SI/eodag)

Follow these steps:
1a. Generate an API-Key from hydroweb.next portal in your user settings
1b. Carefully store your API-Key
- either in your eodag configuration file (usually ~/.config/eodag/eodag.yml, automatically generated the first time you use eodag) in auth/credentials/apikey="PLEASE_CHANGE_ME"
- or in an environment variable `export EODAG__HYDROWEB_NEXT__AUTH__CREDENTIALS__APIKEY="PLEASE_CHANGE_ME"`
2. You can change download directory by modifying the variable path_out. By default, current path is used.

For more information, please refer to EODAG Documentation https://eodag.readthedocs.io
"""

import os
import logging
from typing import Optional, Tuple
import datetime

from eodag import EODataAccessGateway
from eodag import setup_logging


class PixCDownloader:

    PIXC_COLLECTION_NAME = "SWOT_L2_HR_PIXC"

    def __init__(
        self,
        geometry: Optional[str] | None = None,
        dates: Optional[Tuple[datetime.date, datetime.date]] | None = None,
        path_download: str = '/tmp/pixc',
        verbose: Optional[int] = 0,
            ):

        self.geometry = geometry
        self.dates = dates
        self.path_download = path_download

        setup_logging(verbose)  # 0: nothing, 1: only progress bars, 2: INFO, 3: DEBUG

        # Add custom logger
        self.custom_logger = logging.getLogger("hydroweb.next")
        self.custom_logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)-15s %(name)-32s [%(levelname)-8s] %(message)s')
        handler = logging.StreamHandler()  # Use a different handler if needed
        handler.setFormatter(formatter)
        self.custom_logger.addHandler(handler)

        # Set timeout to 30s
        os.environ["EODAG__HYDROWEB_NEXT__SEARCH__TIMEOUT"] = "30"

        self.dag = EODataAccessGateway()

        if not os.path.isdir(self.path_download):
            os.mkdir(self.path_download)

        # Default search criteria when iterating over collection pages
        default_search_criteria = {
            "items_per_page": 2000,
        }

        query_args = {
            "productType": self.PIXC_COLLECTION_NAME,
        }
        if self.geometry is not None:
            query_args['geom'] = self.geometry
        if dates is not None:
            query_args["start"] = self.dates[0].strftime('%Y-%m-%dT%H:%M:%SZ')
            query_args["end"] = self.dates[1].strftime('%Y-%m-%dT%H:%M:%SZ')

        query_args.update(default_search_criteria)

        # Iterate over all pages to find all products
        self.search_results = ([])
        self.custom_logger.info(
            f"Searching for products matching {query_args}..."
        )

        for i, page_results in enumerate(
            self.dag.search_iter_page(**query_args)
            ):
            self.custom_logger.info(
                f"{len(page_results)} product(s) found on page {i+1}"
            )
            self.search_results.extend(page_results)

        self.custom_logger.info(
            f"Total products found : {len(self.search_results)}"
        )

    def download(self):
        # Download all found products
        self.custom_logger.info(
            f"Downloading {len(self.search_results)} products..."
        )

        # This command actually downloads the matching products
        downloaded_paths = self.dag.download_all(
            self.search_results, outputs_prefix=self.path_download
        )
        if downloaded_paths:
            distinct_values = list(set(downloaded_paths))
            # Check is distinct values length is equal to all results length
            if len(distinct_values) != len(self.search_results):
                self.custom_logger.warning(
                    f"Distinct values length is \
                        not equal to all results length. \
                        {len(distinct_values)} != {len(self.search_results)}"
                )
                self.custom_logger.warning(
                    f"Some files have not been downloaded"
                )
            else:
                self.custom_logger.info(
                    f"All {len(self.search_results)} files \
                        have been successfully downloaded."
                )
        else:
            print(
                f"No files downloaded! Verify API-KEY and/or \
                    product search configuration."
            )

