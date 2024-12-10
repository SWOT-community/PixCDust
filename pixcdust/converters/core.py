import copy
from dataclasses import dataclass
import operator

from typing import Optional, Union

import geopandas as gpd

from pixcdust.readers.base_reader import sorted_by_date


class PixCConverter:
    """missing docstring"""

    def __init__(
        self,
        path_in: list[str] | str,
        path_out: str,
        variables: Optional[list[str]] = None,
        area_of_interest: Optional[gpd.GeoDataFrame] = None,
        mode: str = "w",
        compute_wse: bool = True,
    ):

        if isinstance(path_in, list):
            self.path_in = self._sort_input_files(path_in)
        elif isinstance(path_in, str):
            self.path_in = [path_in]
        else:
            raise IOError(
                f"Expected `path_in` list or str, received {type(path_in)}"
            )

        self.path_out = path_out
        self.variables = copy.copy(variables)
        self.area_of_interest = area_of_interest
        if mode in ["w", "o"]:
            self.mode = mode
        else:
            raise IOError(
                f"Expected optional argument `mode` \
                to be 'w'(write/append) or 'o'(overwrite), \
                received {mode} instead"
            )

        self._wse = compute_wse
        # we need some vars to compute wse
        if self._wse and self.variables is not None:
            for var in self._get_vars_wse_computation():
                self.variables.append(var)

    def database_from_nc(self) -> None:
        """missing Docstring"""
        raise NotImplementedError

    @staticmethod
    def _get_vars_wse_computation() -> list[str]:
        return ['height', 'geoid']

    @staticmethod
    def _get_name_wse_var() -> str:
        return 'wse'

    @staticmethod
    def _sort_input_files(files: list[str]) -> list[str]:
        """method sorting files in ascending order based on their names.
        Warning this works with SWOT Pixel Cloud original file names only.
        """
        return sorted_by_date(files)


@dataclass
class GeoLayerH3Projecter:
    """Class for adding H3 projections to databases

    """
    data: gpd.GeoDataFrame
    variable: str
    resolution: int

    def filter_variable(self, conditions: dict[str,dict[str, Union[str, float]]]) -> None:
        """filters from xarray dataset based 
        on operator and threshold on specific variables

        Args:
            conditions (dict): specifies the filters. \
                Example: {\
                    "sig0":{'operator': "ge", 'treshold': 20},\
                    "classification":{'operator': "ge", 'threshold': 3},\
                    }

        Raises:
            IOError: if variable provided in conditions are not\
                in self.data.columns
            IOError: if 'operator' and 'to' keys are not\
                in conditions
            IOError: if operator is not the function name of\
                the operator module
        """
        _k_operator = 'operator'
        _k_to = 'threshold'
        # Test if conditions dict meets specifications
        print(conditions)
        for k in conditions.keys():
            if k not in self.data.columns:
                raise IOError(
                    f'dict conditions expected existing\
                        variables (in {self.data.columns}),\
                        received {k}'
                )
            for instructions in conditions[k].keys():
                if instructions not in [_k_operator, _k_to]:
                    raise IOError(
                        f'dict conditions expected {_k_to} and {_k_operator}\
                        keys in dict {conditions},\
                        received {instructions}'
                    )
                if conditions[k][_k_operator] not in operator.__dict__:
                    raise IOError(
                        f'operator expected a function name\
                            from the operator built-in module\
                            {operator.__dict__},\
                            found {conditions[k][_k_operator]} instead'
                    )
            print(f"operator.{conditions[k][_k_operator]}")
            ope = getattr(operator, conditions[k][_k_operator])
            self.data = self.data[
                ope(
                    self.data[k],
                    conditions[k][_k_to],
                )
            ]

    def compute_h3_layer(self) -> None:
        from pixcdust.dggs import h3_tools
        print(type(self.data))
        self.data = h3_tools.gdf_to_h3_gdf(
            self.data,
            self.resolution,
            self.variable,
        )
