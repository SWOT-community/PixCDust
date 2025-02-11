#
# Copyright (C) 2024 Centre National d'Etudes Spatiales (CNES)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import click

import geopandas as gpd

from pixcdust.converters.gpkg import Nc2GpkgConverter
from pixcdust.converters.zarr import Nc2ZarrConverter
from pixcdust.converters.shapefile import Nc2ShpConverter
from pixcdust.converters.core import Converter

def paths_glob(ctx, param, paths):
    return list(paths)


@click.command()
@click.option(
    "-v",
    "--variables",
    type=click.STRING,
    default=None,
    help="list of variables of interest to extract from SWOT PIXC files,\
        separated with commas ','",
)
@click.option("--aoi", type=click.File(mode='r'), default=None)
@click.option(
    "-m", "--mode",
    type=click.Choice(['w', 'o']),
    help="Mode for writing in database",
    default=('w'),
)
@click.argument(
    'format_out',
    type=click.Choice(
        ['gpkg', 'zarr', 'shp'],
        case_sensitive=False
    ),
)
@click.argument(
    'path_out',
    type=click.Path(),
)
@click.argument(
    'paths_in',
    nargs=-1,
    callback=paths_glob,
)
def cli(
    format_out: str,
    paths_in: list[str],
    path_out: str,
    variables: str,
    aoi: str,
    mode: str,
        ):
    """_summary_

    Args:
        format_out (str): file format to convert to.
        path_in: List of path of files to convert.
        path_out: Output path of the convertion.
        variables: Optionally only read these variables.
        aoi: Optionally only read points in this area of interest.
        path_out (str): _description_
        variables (list[str]): _description_
        mode: Writing mode of the output. Must be 'w'(write/append) or 'o'(overwrite).


    Raises:
        NotImplementedError: _description_
    """
    if variables is not None:
        variables.strip('()')
        variables.strip('[]')
        list_vars = variables.split(',')
        for var in list_vars:
            if any(not c.isalnum() for c in var):
                raise click.BadOptionUsage(
                    'variables',
                    "apart from the commas, no special caracter may be used",
                )

    else:
        list_vars = None

    if aoi is not None:
        gdf_aoi = gpd.read_file(aoi)
    else:
        gdf_aoi = None

    if format_out.lower() == 'gpkg':
        pixc : Converter = Nc2GpkgConverter(
            paths_in,
            variables=list_vars,
            area_of_interest=gdf_aoi,
        )
    elif format_out.lower() == 'zarr':
        pixc = Nc2ZarrConverter(
            sorted(paths_in),
            variables=list_vars,
            area_of_interest=gdf_aoi,
        )
    elif format_out.lower() == 'shp':
        pixc = Nc2ShpConverter(
            paths_in,
            variables=list_vars,
            area_of_interest=gdf_aoi,
        )
    else:
        raise NotImplementedError(
            f'the conversion format {format_out} has not been implemented yet',
            )

    pixc.database_from_nc(path_out, mode=mode)


def main():
    cli(prog_name="convert_pixc")


if __name__ == "__main__":
    main()
