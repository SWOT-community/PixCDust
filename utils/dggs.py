import h3
import h3.unstable.vect
import h3.api.numpy_int

from shapely.geometry import Polygon



def cell_to_shapely(cell):
    coords = h3.h3_to_geo_boundary(cell)
    flipped = tuple(coord[::-1] for coord in coords)
    return Polygon(flipped)


def get_h3_res_name(res: int):
    return "h3_" + str(res).zfill(2)