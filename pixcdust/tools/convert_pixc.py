import click

import geopandas as gpd

from pixcdust.converters.gpkg import PixCNc2GpkgConverter
from pixcdust.converters.zarr import PixCNc2ZarrConverter
from pixcdust.converters.shapefile import PixCNc2ShpConverter


# dir_swot = "/home/hysope2/STUDIES/SWOT_Sudan/DATA/Raw_Data"
# files_swot_pxc = os.path.join(dir_swot, "SWOT*.nc")
# paths = sorted(glob(files_swot_pxc))
# list_vars = ["height", "sig0", "classification", "geoid", "cross_track"]

# aoi_file = "/home/hysope2/STUDIES/SWOT_Kakhovka/DATA/kakhovka.gpkg"

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
    variables: list[str],
    aoi: str,
    mode: str,
        ):
    """_summary_

    Args:
        format_out (str): _description_
        paths_in (list[str]): _description_
        path_out (str): _description_
        variables (list[str]): _description_
        aoi (str): _description_
        mode (str): mode for writing in database

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
        pixc = PixCNc2GpkgConverter(
            paths_in,
            path_out,
            variables=list_vars,
            area_of_interest=gdf_aoi,
            mode=mode,
        )
    elif format_out.lower() == 'zarr':
        pixc = PixCNc2ZarrConverter(
            paths_in,
            path_out,
            variables=list_vars,
            area_of_interest=gdf_aoi,
            mode=mode,
        )
    elif format_out.lower() == 'shp':
        pixc = PixCNc2ShpConverter(
            paths_in,
            path_out,
            variables=list_vars,
            area_of_interest=gdf_aoi,
            mode=mode,
        )
    else:
        raise NotImplementedError(
            f'the conversion format {format_out} has not been implemented yet',
            )

    pixc.database_from_nc()


def main():
    cli(prog_name="convert_pixc")


if __name__ == "__main__":
    main()
