"""Interface used by all Pixcdust Converters."""

import copy
from dataclasses import dataclass
import operator
from pathlib import Path

from typing import Optional, Union, Iterable

import geopandas as gpd


class PixCConverter:
    """Abstract class parent of pixcdust converters.

    They convert from official SWOT Pixel Cloud Netcdf to the supported format.

    Attributes:
        path_in: List of path of files to convert.
        path_out: Output path of the convertion.
        variables: Optionally only read these variables.
        area_of_interest: Optionally only read points in area_of_interest.
        mode: Writing mode of the outpout. Must be 'w'(write/append) or 'o'(overwrite).
    """

    def __init__(
        self,
        path_in: str | Iterable[str] | Path | Iterable[Path],
        path_out: str,
        variables: Optional[list[str]] = None,
        area_of_interest: Optional[gpd.GeoDataFrame] = None,
        mode: str = "w",
        compute_wse: bool = True,
    ):
        """Basic initialisation of a pixcdust converter.

        They convert from official SWOT Pixel Cloud Netcdf to the supported format.

        Attributes:
            path_in: Path or list of path of file(s) to convert.
            path_out: Output path of the convertion.
            variables: Optionally only read these variables.
            area_of_interest: Optionally only read points in area_of_interest.
            mode: Writing mode of the outpout. Must be 'w'(write/append) or 'o'(overwrite).
            compute_wse:  toggle water surface elevation computation.
        """
        if isinstance(path_in, str | Path):
            self.path_in = [str(path_in)]
        else:
            self.path_in = [str(p) for p in path_in]

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
        """Convert the path_in files to path_out."""
        raise NotImplementedError

    @staticmethod
    def _get_vars_wse_computation() -> list[str]:
        """Names of fields used to compute wse."""
        return ['height', 'geoid']

    @staticmethod
    def _get_name_wse_var() -> str:
        """Output name for wse."""
        return 'wse'

@dataclass
class GeoLayerH3Projecter:
    """Class for adding H3 projections to databases

    Attributes:
        data: data getting projected
        variable: FIXME
        resolution: Resolution

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
            ValueError: if 'operator' and 'to' keys are not\
                in conditions
            AttributeError: if operator is not the function name of\
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
                    raise ValueError(
                        f'dict conditions expected {_k_to} and {_k_operator}\
                        keys in dict {conditions},\
                        received {instructions}'
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
        """Project data to h3."""
        from pixcdust.dggs import h3_tools
        self.data = h3_tools.gdf_to_h3_gdf(
            self.data,
            self.resolution,
            self.variable,
        )
