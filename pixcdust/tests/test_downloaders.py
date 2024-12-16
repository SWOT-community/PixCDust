import pytest

from pixcdust.tests.init_tests import download_test_data


@pytest.mark.downloader
def test_hydroweb_next(hydroweb_env, input_folder, tmp_folder):
    """Test h3ydorewb.next downloader.

    Require to have configured --hydroweb_auth with init_tests.py.
    Only run with the option --ddl.
    """
    dl_dir = tmp_folder / "download_test"
    download_test_data(dl_dir)

    dl_files = sorted(dl_dir.glob("**/*"))
    all_input_files = sorted(input_folder.glob("**/*"))
    assert len(dl_files) == len(all_input_files)

    for dl_f, input_f in zip(dl_files, all_input_files):
        assert dl_f.stat().st_size == input_f.stat().st_size
