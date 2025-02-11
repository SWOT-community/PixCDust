import xarray as xr
from pixcdust.readers import NcSimpleReader


def test_h3_conversion(first_file):
    """
    Test the conversion of the dataset to the H3 grid.
    """
    # Resolution can be parameterized based on what you want to test
    resolution = 8

    reader = NcSimpleReader(first_file)
    reader.read()
    # Run the H3 conversion function
    ds_h3 = reader.to_h3(variables='height', resolution=resolution)

    # Assertions to verify correct output
    assert isinstance(ds_h3, xr.Dataset), "The result should be an xarray Dataset"

    # Verify that H3 cell IDs exist and have expected properties
    assert 'cell_ids' in ds_h3.coords, "H3 cell IDs should be present in the output"

    # Ensure the data is not empty after conversion
    assert len(ds_h3['cell_ids']) > 0, "The output dataset should have H3 cell IDs"


def test_healpix_conversion(first_file):
    """
    Test the conversion of the dataset to the HEALPix grid.
    """
    resolution = 10  # You can adjust resolution as needed

    reader = NcSimpleReader(first_file)
    reader.read()
    # Run the HEALPix conversion function
    ds_healpix = reader.to_healpix(variables='height', resolution=resolution)

    # Assertions to verify correct output
    assert isinstance(ds_healpix, xr.Dataset), "The result should be an xarray Dataset"

    # Verify that HEALPix cell IDs and coordinates exist
    assert 'cell_ids' in ds_healpix.coords, "HEALPix cell IDs should be present"

    # Ensure the data is not empty after conversion
    assert len(ds_healpix['cell_ids']) > 0, "The output dataset should have HEALPix cell IDs"
