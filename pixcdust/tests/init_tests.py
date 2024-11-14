import argparse
import os
from typing import Union

from pixcdust.downloaders.hydroweb_next import PixCDownloader
from datetime import datetime
import json
from pathlib import Path

class JsonTestsSettings:
    CONFIG_FILE_NAME = 'conftest.json'
    def __init__(self):
        try:
            with open(self._config_path) as f:
                self._settings = json.load(f)
        except FileNotFoundError :
            self._settings = {}

    @property
    def input_folder(self) -> Path:
        try:
            return Path(self._settings["input_folder"])
        except KeyError:
            raise KeyError("Test input folder not set. Configure it with init_tests.py")

    @input_folder.setter
    def input_folder(self, value: Union[Path, str]) -> None:
        self._settings["input_folder"] = str(value)

    @property
    def tmp_folder(self) -> Path:
        return Path(self._settings.get("tmp_folder", "/tmp/pixcdust-test"))

    @tmp_folder.setter
    def tmp_folder(self, value: Union[Path, str]) -> None:
        self._settings["tmp_folder"] = str(value)

    @property
    def hydroweb_auth(self) -> str:
        return self._settings.get("hydroweb_auth", "")

    @hydroweb_auth.setter
    def hydroweb_auth(self, value: str) -> None:
        self._settings["hydroweb_auth"] = value

    def write(self) -> None:
        with open(self._config_path, mode='w') as f:
            json.dump(self._settings, f)
        os.chmod(self._config_path,0o600)

    @property
    def _config_path(self) -> Path:
        return Path(__file__).parent.absolute()/self.CONFIG_FILE_NAME

def init_hydroweb_env(test_settings):
    apikey = test_settings.hydroweb_auth
    if apikey:
        os.environ["EODAG__HYDROWEB_NEXT__AUTH__CREDENTIALS__APIKEY"] = apikey

#polygon = Polygon([(-0.945, 43.522),(-0.945, 43.537),(-0.823, 43.537),(-0.823 ,43.522)])
#polygon = Polygon([(-1.50580, 43.39543),(-1.36597, 43.39543),(-1.36597, 43.56471),(-1.50580 ,43.56471),(-1.50580, 43.39543)])
#geometry = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[polygon])

def download_test_data(path_download: Path) -> None:
    dates = (
        datetime(2024,8,1),
        datetime(2024,8,15),
    )

    geometry = "POLYGON((-1.50580 43.39543,-1.36597 43.39543,-1.36597 43.56471,-1.50580 43.56471,-1.50580 43.39543))"
    pixcdownloader = PixCDownloader(
        geometry,
        dates,
        verbose=0,
        path_download=str(path_download)
        )
    pixcdownloader.search_download()

TEST_DATA_COUNT = 2
def check_test_data(path_download: Path):
    data_list = list(path_download.glob("*/*nc"))
    return len(data_list) == TEST_DATA_COUNT


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            prog='init_tests',
            description='Configure the tests and if missing download the test data'
    )
    parser.add_argument("-I","--input_folder", help="path where is downloaded the test input data")
    parser.add_argument("-T","--tmp_folder", help="path where is writen the temporary data")
    parser.add_argument("-H","--hydroweb_auth", help="api key to download from hydroweb")
    parser.add_argument("-D","--download", help="force download of the test data", nargs='?', const='true')

    args = parser.parse_args()
    settings = JsonTestsSettings()
    dl_cfg_changed=False
    if args.input_folder:
        dl_cfg_changed=True
        settings.input_folder = args.input_folder
    if args.tmp_folder:
        settings.tmp_folder = args.tmp_folder
    if args.hydroweb_auth:
        dl_cfg_changed=True
        settings.hydroweb_auth = args.hydroweb_auth


    settings.write()
    path_download = settings.input_folder #Path('/home/gbeaucha/pixcdust/test-data')

    init_hydroweb_env(settings)
    if args.download is None:
        if not check_test_data(path_download):
            # config changed and the data is missing.
            download_test_data(path_download)
    else:
        if args.download.lower() == "true":
            # download requested by user
            download_test_data(path_download)

