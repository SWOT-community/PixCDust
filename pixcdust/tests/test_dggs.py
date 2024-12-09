from pixcdust.converters.core import GeoLayerH3Projecter
from pixcdust.readers import GpkgReader
from .test_converters import converted_lim_gpkg

def test_h3_proj(converted_lim_gpkg):
    """Test h3 projection.
    """
    # need to run on a limited dataset to stay in memory.
    reader = GpkgReader(converted_lim_gpkg)

    reader.read()
    var = reader.to_geodataframe()
    projector = GeoLayerH3Projecter(var, "sig0", 7)
    projector.compute_h3_layer()
    print(projector.data)
