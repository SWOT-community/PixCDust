import argparse
import json
import os
from datetime import UTC, datetime, timezone
from pathlib import Path

from pixcdust.downloaders.hydroweb_next import PixCDownloader


class JsonTestsSettings:
    """Reader-writer for the test configuration."""

    CONFIG_FILE_NAME = "conftest.json"

    def __init__(self):
        try:
            with open(self._config_path) as f:
                self._settings = json.load(f)
        except FileNotFoundError:
            self._settings = {}

    @property
    def input_folder(self) -> Path:
        """Path to folder where test input data are downloaded and stored."""
        try:
            return Path(self._settings["input_folder"])
        except KeyError:
            raise KeyError("Test input folder not set. Configure it with init_tests.py")

    @input_folder.setter
    def input_folder(self, value: Path | str) -> None:
        self._settings["input_folder"] = str(value)

    @property
    def tmp_folder(self) -> Path:
        """Path to folder where test outputs data are written."""
        return Path(self._settings.get("tmp_folder", "/tmp/pixcdust-test"))

    @tmp_folder.setter
    def tmp_folder(self, value: Path | str) -> None:
        self._settings["tmp_folder"] = str(value)

    @property
    def hydroweb_auth(self) -> str:
        """Hydroweb.next personal API key."""
        return self._settings.get("hydroweb_auth", "")

    @hydroweb_auth.setter
    def hydroweb_auth(self, value: str) -> None:
        self._settings["hydroweb_auth"] = value

    def write(self) -> None:
        """Write the config in JSON to self._config_path."""
        with open(self._config_path, mode="w") as f:
            json.dump(self._settings, f)
        os.chmod(self._config_path, 0o600)

    @property
    def _config_path(self) -> Path:
        """Path of the JSON config file.

        Should be the absolute path of tests/conftest.json
        """
        return Path(__file__).parent.absolute() / self.CONFIG_FILE_NAME


def init_hydroweb_env(test_settings: JsonTestsSettings) -> None:
    """Configure the Hydroweb.next API key of the current environment.

    Args:
        test_settings: the key is read from `test_settings.hydroweb_auth`.
    """
    apikey = test_settings.hydroweb_auth
    if apikey:
        os.environ["HYDROWEB_API_KEY"] = apikey
        os.environ["EODAG__HYDROWEB_NEXT__AUTH__CREDENTIALS__APIKEY"] = apikey


def download_test_data(path_download: Path, backend: str) -> None:
    """Download the test data from hydroweb.next.

    Args:
        path_download: where to store the test data.
    """
    dates = (
        # LOCALE timezone
        datetime(2024, 8, 1, tzinfo=timezone.utc),
        datetime(2024, 8, 3, tzinfo=timezone.utc),
    )

    # geometry = "POLYGON((-1.50580 43.39543,-1.36597 43.39543,-1.36597 43.56471,-1.50580 43.56471,-1.50580 43.39543))"
    geometry = "POLYGON((-1.50580 43.49543,-1.49597 43.49543,-1.49597 43.50471,-1.50580 43.50471,-1.50580 43.49543))"

    pixcdownloader = PixCDownloader(
                geometry, dates, backend=backend, verbose=3, path_download=str(path_download)
        #        geometry, dates, backend=backend, verbose=0, path_download=str(path_download)
    )
    pixcdownloader.search_download()


TEST_DATA_COUNT = 2


def check_test_data(path_download: Path) -> bool:
    data_list = list(path_download.glob("**/*.nc"))
    return len(data_list) == TEST_DATA_COUNT


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="init_tests",
        description="Configure the tests and if missing download the test data",
    )
    parser.add_argument(
        "-I", "--input_folder", help="path where is downloaded the test input data"
    )
    parser.add_argument(
        "-T", "--tmp_folder", help="path where is writen the temporary data"
    )
    parser.add_argument(
        "-H", "--hydroweb_auth", help="api key to download from hydroweb"
    )
    parser.add_argument(
        "-D",
        "--download",
        help="force download of the test data",
        nargs="?",
        const="true",
    )

    args = parser.parse_args()
    settings = JsonTestsSettings()
    dl_cfg_changed = False
    if args.input_folder:
        dl_cfg_changed = True
        settings.input_folder = args.input_folder
    if args.tmp_folder:
        settings.tmp_folder = args.tmp_folder
    if args.hydroweb_auth:
        dl_cfg_changed = True
        settings.hydroweb_auth = args.hydroweb_auth

    settings.write()
    path_download = settings.input_folder

    init_hydroweb_env(settings)
    if args.download is None:
        if not check_test_data(path_download):
            # config changed and the data is missing.
            download_test_data(path_download, "default")
    else:
        if args.download.lower() == "true":
            # download requested by user
            download_test_data(path_download, "default")
