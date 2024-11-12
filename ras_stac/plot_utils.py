import contextily as ctx
import matplotlib.pyplot as plt
import geopandas as gpd

from matplotlib.lines import Line2D
from typing import Optional, Union
import dataretrieval.nwis as nwis
import pandas as pd
from rashdf import RasGeomHdf, RasPlanHdf
from rashdf.plan import RasPlanHdfError
import logging


def read_model_plot_data(ras_hdf: Union[RasPlanHdf, RasGeomHdf]):
    """Attempt to read breaklines, boundary condition lines, and mesh polygons from the RAS HDF dataset for thumbnail."""

    try:
        model_breaklines = ras_hdf.breaklines()
    except Exception:
        model_breaklines = None

    try:
        bc_lines = ras_hdf.bc_lines()
    except Exception:
        bc_lines = None

    try:
        mesh_polygons = ras_hdf.mesh_cell_polygons()
    except Exception:
        mesh_polygons = None

    return model_breaklines, bc_lines, mesh_polygons


def create_model_thumbnail(
    ras_hdf: Union[RasPlanHdf, RasGeomHdf],
    gages_df: Optional[gpd.GeoDataFrame],
    title: str,
    crs: str = "EPSG:4326",
) -> plt.Figure:
    """
    Creates a model thumbnail plot with breaklines, boundary condition lines, mesh polygons, and USGS gage locations.

    Args:
        ras_hdf (Union[RasPlanHdf, RasGeomHdf]): The RAS HDF dataset (either plan or geometry) which
            breaklines, boundary condition lines, and mesh polygons are retrieved from.
        gages_df (Optional[gpd.GeoDataFrame]): Geodataframe containing USGS gage data, if available.
        title (str): Title for the plot.
        crs (str, optional): Coordinate reference system. Defaults to "EPSG:4326".

    Returns:
        plt.Figure: The created figure.
    """
    breaklines_gdf, bc_lines_gdf, mesh_polygons_gdf = read_model_plot_data(ras_hdf)

    breaklines_geo = breaklines_gdf.to_crs(crs) if breaklines_gdf is not None else None
    bc_lines_geo = bc_lines_gdf.to_crs(crs) if bc_lines_gdf is not None else None
    mesh_polygons_geo = (
        mesh_polygons_gdf.to_crs(crs) if mesh_polygons_gdf is not None else None
    )
    gages_geo = (
        gages_df.to_crs(crs) if gages_df is not None and not gages_df.empty else None
    )

    fig, ax = plt.subplots(figsize=(12, 12))
    legend_handles = []

    # Plot mesh polygons if available
    if mesh_polygons_geo is not None:
        mesh_polygons_geo.plot(
            ax=ax,
            edgecolor="silver",
            facecolor="none",
            linestyle="-",
            alpha=0.7,
            label="Mesh Polygons",
        )
        legend_handles.append(
            Line2D(
                [0],
                [0],
                color="silver",
                linestyle="-",
                linewidth=2,
                label="Mesh Polygons",
            )
        )

    # Plot breaklines if available
    if breaklines_geo is not None:
        breaklines_geo.plot(
            ax=ax, edgecolor="red", linestyle="-", alpha=0.3, label="Breaklines"
        )
        legend_handles.append(
            Line2D(
                [0],
                [0],
                color="red",
                linestyle="-",
                alpha=0.4,
                linewidth=2,
                label="Breaklines",
            )
        )

    # Add a heading for "BC Lines" in the legend if boundary condition lines are available
    if bc_lines_geo is not None:
        legend_handles.append(
            Line2D([0], [0], color="none", linestyle="None", label="")
        )
        legend_handles.append(
            Line2D([0], [0], color="none", linestyle="None", label="BC Lines")
        )
        colors = plt.cm.get_cmap("Dark2", len(bc_lines_geo))

        # Plot each boundary condition line with a unique color
        for bc_line, color in zip(bc_lines_geo.itertuples(), colors.colors):
            x_coords, y_coords = bc_line.geometry.xy
            ax.plot(
                x_coords,
                y_coords,
                color=color,
                linestyle="-",
                linewidth=2,
                label=bc_line.name,
            )
            legend_handles.append(
                Line2D(
                    [0],
                    [0],
                    color=color,
                    linestyle="-",
                    linewidth=2,
                    label=bc_line.name,
                )
            )

    # Plot gages if available
    if gages_geo is not None:
        legend_handles.append(
            Line2D([0], [0], color="none", linestyle="None", label="")
        )
        legend_handles.append(
            Line2D([0], [0], color="none", linestyle="None", label="USGS Gages")
        )
        gage_colors = plt.cm.get_cmap("Set1", len(gages_geo))

        for gage, color in zip(gages_geo.itertuples(), gage_colors.colors):
            ax.plot(
                gage.geometry.x,
                gage.geometry.y,
                color=color,
                marker="*",
                markersize=15,
                label=gage.site_no,
            )
            legend_handles.append(
                Line2D(
                    [0],
                    [0],
                    color=color,
                    marker="*",
                    markersize=15,
                    linestyle="None",
                    label=gage.site_no,
                )
            )

    # Add OpenStreetMap basemap
    ctx.add_basemap(
        ax,
        crs=crs,
        source=ctx.providers.OpenStreetMap.Mapnik,
        alpha=0.4,
    )

    ax.set_title(title, fontsize=15)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.legend(handles=legend_handles, loc="center left", bbox_to_anchor=(1, 0.5))

    return fig


def get_gage_data(
    ras_hdf: Union[RasPlanHdf, RasGeomHdf],
    crs: str = "EPSG:4326",
    buffer_increase=0.0001,
    max_buffer=0.01,
):
    """Retrieve USGS gage data from the model reference lines in the RAS HDF dataset.

    Retrieves USGS gage data for each reference line in the provided HEC-RAS plan or geometry HDF file.
    Each reference line is buffered to attempt gage data retrieval, and the buffer expands incrementally
    until a maximum buffer distance is reached or a gage is found.

    Args:
        ras_hdf (Union[RasPlanHdf, RasGeomHdf]): The HEC-RAS plan or geometry HDF file containing reference lines.
        crs (str): Coordinate reference system. Defaults to "EPSG:4326".
        buffer_increase (float): Buffer distance to expand search area for gages. Defaults to 0.0001.
        max_buffer (float): Maximum buffer distance before stopping gage search. Defaults to 0.01.

    Returns:
        all_usgs_gages (pd.DataFrame): DataFrame of unique gages found for each reference line.
    """

    logging.info("Retrieving USGS gage data.")

    try:
        ref_line = ras_hdf.reference_lines()
    except RasPlanHdfError:
        logging.info("No reference lines found in the model.")
        return None

    ref_line = ref_line.to_crs(crs)

    if len(ref_line) == 0:
        logging.info("No reference lines found in the model.")
        return None

    all_usgs_gages = pd.DataFrame()

    for _, row in ref_line.iterrows():
        buffer_increment = 0  # Start with no buffer

        while True:
            try:
                if buffer_increment == 0:
                    bbox = [*row.geometry.bounds]
                else:
                    bbox = [*row.geometry.buffer(buffer_increment).bounds]

                # bbox must be rounded to work with usgs data retrieval
                rounded_bbox = [round(coord, 7) for coord in bbox]

                usgs_data = nwis.what_sites(bBox=rounded_bbox)

                usgs_gages = usgs_data[0]
                # Filter out any gages where 'site_no' has more than 8 digits, assuming those are invalid
                valid_gages = usgs_gages[usgs_gages["site_no"].str.len() == 8]
                valid_gages["refln_name"] = row.refln_name

                # If there are valid gages, append them to all_usgs_gages and break the loop
                if not valid_gages.empty:
                    logging.info(f"Found gage for reference line {row.refln_name}.")
                    all_usgs_gages = pd.concat(
                        [all_usgs_gages, valid_gages], ignore_index=True
                    )
                    break

                logging.debug(
                    f"No valid gages found. Increasing buffer by {buffer_increase} for reference line {row.refln_name}."
                )
                buffer_increment += buffer_increase

                if buffer_increment > max_buffer:
                    logging.info(
                        f"Max buffer of {max_buffer} reached. No gages found for reference line {row.refln_name}."
                    )
                    break
            except ValueError:
                logging.debug(
                    f"No gages found. Increasing buffer by {buffer_increase} for reference line {row.refln_name}."
                )
                buffer_increment += buffer_increase

                if buffer_increment > max_buffer:
                    logging.info(
                        f"Max buffer of {max_buffer} reached. No gages found for reference line {row.refln_name}."
                    )
                    break

    # Remove duplicate gages
    all_usgs_gages = all_usgs_gages.drop_duplicates(subset="site_no").reset_index(
        drop=True
    )
    logging.info(f"Found {len(all_usgs_gages)} unique gages.")
    return all_usgs_gages


def create_usgs_gage_links(
    gage_df, usgs_gage_url_prefix="https://waterdata.usgs.gov/monitoring-location"
):
    """Format item links for each USGS gage in the given gages Dataframe."""

    usgs_gage_links = []

    for _, row in gage_df.iterrows():
        gage_link = {
            "rel": "USGS_Gages",
            "href": f"{usgs_gage_url_prefix}/{row['site_no']}",
            "title": f"{row['site_no']} {row['station_nm']}",
            "extra_fields": {
                "Associated Model Reference Line": row["refln_name"],
                "Site Number": row["site_no"],
                "Site Lat, Lon": f"{row['dec_lat_va']}, {row['dec_long_va']}",
            },
        }
        usgs_gage_links.append(gage_link)

    return usgs_gage_links
