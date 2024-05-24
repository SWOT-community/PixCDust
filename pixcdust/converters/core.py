import geopandas as gpd


class PixCConverter:
    """missing docstring"""

    def __init__(
        self,
        path_in: list[str] | str,
        path_out: str,
        variables: list[str] = None,
        area_of_interest: gpd.GeoDataFrame = None,
        mode: str = "w",
    ):

        if isinstance(path_in, list):
            self.path_in = path_in
        elif isinstance(path_in, str):
            self.path_in = [path_in]
        else:
            raise IOError(
                f"Expected `path_in` list or str, received {type(path_in)}"
            )

        self.path_out = path_out
        self.variables = variables
        self.area_of_interest = area_of_interest
        if mode in ["w", "o"]:
            self.mode = mode
        else:
            raise IOError(
                f"Expected optional argument `mode` \
                to be 'w'(write/append) or 'o'(overwrite), \
                received {mode} instead"
            )

    def database_from_nc(self):
        """missing Docstring"""
        raise NotImplementedError
