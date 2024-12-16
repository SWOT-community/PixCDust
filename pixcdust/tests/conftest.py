import os
from pathlib import Path
from typing import List

import pytest

from pixcdust.tests.init_tests import JsonTestsSettings, init_hydroweb_env


def pytest_addoption(parser):
    parser.addoption("--dl", action="store_true", default=False, help="run dowloaders tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "downloader: mark test as testing downloads")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--dl"):
        # option given: do not skip  tests
        return
    skip_dl = pytest.mark.skip(reason="need --dl option to run")
    for item in items:
        if "downloader" in item.keywords:
            item.add_marker(skip_dl)


@pytest.fixture(scope="session")
def tests_settings() -> JsonTestsSettings:
    return JsonTestsSettings()

@pytest.fixture(scope="session")
def input_folder(tests_settings) -> Path:
    return tests_settings.input_folder

@pytest.fixture(scope="session")
def input_files(input_folder) -> List[Path]:
    return  list(input_folder.glob("*/*nc"))

@pytest.fixture(scope="session")
def first_file(input_folder) -> Path:
    return  next(input_folder.glob("*/*_20240803T*nc"))

@pytest.fixture(scope="session")
def tmp_folder(tests_settings) -> Path:
    tests_settings.tmp_folder.mkdir(exist_ok=True)
    return tests_settings.tmp_folder

@pytest.fixture()
def hydroweb_env(tests_settings) -> None:
    init_hydroweb_env(tests_settings)