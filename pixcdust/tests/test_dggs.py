from pixcdust.converters.core import GeoLayerH3Projecter
from pixcdust.readers import PixCGpkgReader
from .test_converters import converted_lim_gpkg

def test_h3_proj(converted_lim_gpkg):
    # need to run on a limited dataset to stay in memory.
    reader = PixCGpkgReader(converted_lim_gpkg)

    reader.read()
    var = reader.data
    projector = GeoLayerH3Projecter(var, "sig0", 1200)
    projector.compute_h3_layer()
    print(projector.data)
    assert(False)