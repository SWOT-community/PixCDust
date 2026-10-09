import pytest

from pixcdust.tests.init_tests import download_test_data


@pytest.mark.downloader
def test_hydroweb_next(hydroweb_env, input_folder, tmp_folder):
    """Test hydroweb.next default downloader.

    Require to have configured --hydroweb_auth with init_tests.py.
    Only run with the option --ddl.
    """
    dl_dir = tmp_folder / "download_test"
    download_test_data(dl_dir, "default")

    dl_files = sorted(dl_dir.glob("**/*.nc"))
    all_input_files = sorted(input_folder.glob("**/*.nc"))
    assert len(dl_files) == len(all_input_files)

    #    for dl_f, input_f in zip(dl_files, all_input_files):
    #        assert dl_f.stat().st_size == input_f.stat().st_size
    for dl_f in dl_files:
        assert dl_f.stat().st_size == 160376049


@pytest.mark.downloader
def test_hydroweb_next_eodag(hydroweb_env, input_folder, tmp_folder):
    """Test hydroweb.next eodag downloader.

    Require to have configured --hydroweb_auth with init_tests.py.
    Only run with the option --ddl.
    """
    dl_dir = tmp_folder / "download_test_eodag"
    download_test_data(dl_dir, "eodag")

    # fix: avoid listing eodag's .download repo
    dl_files = sorted(dl_dir.glob("**/[!.]*.nc"))
    all_input_files = sorted(input_folder.glob("**/*.nc"))
    assert len(dl_files) == len(all_input_files)

    #    for dl_f, input_f in zip(dl_files, all_input_files):
    #        assert dl_f.stat().st_size == input_f.stat().st_size
    for dl_f in dl_files:
        assert dl_f.stat().st_size == 160376049
