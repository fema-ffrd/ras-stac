import datetime
import math
import os
from functools import wraps
from typing import List

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point

from ras_stac.ras1d.utils.common import file_location
from ras_stac.ras1d.utils.ras_utils import (
    data_pairs_from_text_block,
    search_contents,
    text_block_from_start_end_str,
    text_block_from_start_str_length,
    text_block_from_start_str_to_empty_line,
)
from ras_stac.ras1d.utils.s3_utils import str_from_s3
from ras_stac.utils.s3_utils import get_basic_object_metadata


# Decorator functions
def cache_data(func):
    """Check if asset already downloaded."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.file_str is None:
            self.download_asset_str()
        return func(self, *args, **kwargs)

    return wrapper


def check_crs(func):
    """Check CRS decorator."""

    def wrapper(self, *args, **kwargs):
        if self.crs is None:
            raise ValueError("Projection cannot be None")
        return func(self, *args, **kwargs)

    return wrapper


# Classes
class GenericAsset:

    def __init__(self, url: str):
        self.url = url
        self.file_str = None
        self.roles = []
        self.description = None
        self.title = None
        self.extra_fields = {}
        self.loc = file_location(url)

    def __str__(self):
        return self.url

    @property
    def suffix(self):
        return self.url.split(".")[-1]

    def download_asset_str(self) -> None:
        if self.loc == "local":
            with open(self.url) as f:
                self.file_str = f.read()
        else:
            self.file_str = str_from_s3(self.url)

    def endswith(self, suffix: str) -> bool:
        if self.url.endswith(suffix):
            return True
        else:
            return False

    @cache_data
    @property
    def is_ras_prj(self) -> bool:
        if not self.endswith(".prj"):
            return False
        if "Proj Title" in self.file_str.split("\n")[0]:
            return True
        else:
            return False

    @property
    def basic_metadata(self):
        if self.loc == "local":
            last_mod = os.path.getmtime(self.url)
            last_mod = datetime.fromtimestamp(last_mod)
            last_mod = last_mod.isoformat()
            return {"file:size": os.path.getsize(self.url), "last_modified": last_mod}
        else:
            return get_basic_object_metadata(self.url)

    def add_extra_fields(self):
        for k, v in self.basic_metadata:
            self.extra_fields[k] = v


class ProjectAsset(GenericAsset):

    @cache_data
    @property
    def plans(self):
        """Get the plans associated with this project."""
        return search_contents(self.file_str, "Plan File", expect_one=False)

    @cache_data
    @property
    def current_plan(self):
        """Get the current plan of this project"""
        return search_contents(self.file_str, "Current Plan", expect_one=True)

    @property
    def title(self):
        """Title of the HEC-RAS project."""
        return search_contents(self.file_str, "Proj Title")


class PlanAsset(GenericAsset):

    @cache_data
    @property
    def is_encroached(self) -> bool:
        return "Encroach Node" in self.file_str

    @cache_data
    @property
    def geometry(self) -> str:
        """Get the geometry listed in the plan file."""
        return search_contents(self.file_str, "Geom File", expect_one=True)

    @cache_data
    @property
    def title(self) -> str:
        return search_contents(self.file_str, "Plan Title", expect_one=True)


class SteadyFlowAsset(GenericAsset):

    @cache_data
    @property
    def title(self):
        return search_contents(self.file_str, "Flow Title", expect_one=True)


class GeometryAsset(GenericAsset):

    def __init__(self, url: str):
        super().__init__(url)
        self.contents = self.download_asset_str().splitlines()
        self.crs = None
        self._concave_hull = None

    def set_crs(self, crs: str) -> None:
        self.crs = crs

    @property
    def title(self):
        """Title of the HEC-RAS Geometry file."""
        return search_contents(self.contents, "Geom Title")

    @property
    def version(self):
        """The HEC-RAS version."""
        return search_contents(self.contents, "Program Version", expect_one=False)

    @property
    @check_crs
    def reaches(self) -> dict:
        """A dictionary of the reaches contained in the HEC-RAS geometry file."""
        river_reaches = search_contents(self.contents, "River Reach", expect_one=False)
        reaches = {}
        for river_reach in river_reaches:
            reaches[river_reach] = Reach(self.contents, river_reach, self.crs)
        return reaches

    @property
    @check_crs
    def rivers(self) -> dict:
        """A nested river-reach dictionary of the rivers/reaches contained in the HEC-RAS geometry file."""
        rivers = {}
        for reach in self.reaches.values():
            rivers[reach.river] = {}
            rivers[reach.river].update({reach.reach: reach})
        return rivers

    @property
    @check_crs
    def junctions(self) -> dict:
        """A dictionary of the junctions contained in the HEC-RAS geometry file."""
        juncts = search_contents(self.contents, "Junct Name", expect_one=False)
        junctions = {}
        for junct in juncts:
            junctions[junct] = Junction(self.contents, junct, self.crs)
        return junctions

    @property
    @check_crs
    def cross_sections(self) -> dict:
        """A dictionary of the cross sections contained in the HEC-RAS geometry file."""
        cross_sections = {}
        for reach in self.reaches.values():
            cross_sections.update(reach.cross_sections)

        return cross_sections

    @property
    @check_crs
    def structures(self) -> dict:
        """A dictionary of the structures contained in the HEC-RAS geometry file."""
        structures = {}
        for reach in self.reaches.values():
            structures.update(reach.structures)

        return structures

    @property
    @check_crs
    def reach_gdf(self):
        """A GeodataFrame of the reaches contained in the HEC-RAS geometry file."""
        return pd.concat([reach.gdf for reach in self.reaches.values()], ignore_index=True)

    @property
    @check_crs
    def junction_gdf(self):
        """A GeodataFrame of the junctions contained in the HEC-RAS geometry file."""
        if self.junctions:
            return pd.concat(
                [junction.gdf for junction in self.junctions.values()],
                ignore_index=True,
            )

    @property
    @check_crs
    def xs_gdf(self):
        """Geodataframe of all cross sections in the geometry text file."""
        return pd.concat([xs.gdf for xs in self.cross_sections.values()], ignore_index=True)

    @property
    @check_crs
    def structures_gdf(self):
        """Geodataframe of all structures in the geometry text file."""
        return pd.concat([structure.gdf for structure in self.structures.values()], ignore_index=True)

    @property
    @check_crs
    def gdfs(self):
        """Group all geodataframes into a dictionary"""
        gdfs = {}
        gdfs["XS"] = self.xs_gdf
        gdfs["River"] = self.reach_gdf
        if self.junction_gdf:
            gdfs["Junction"] = self.junction_gdf
        if self.structures_gdf:
            gdfs["Structure"] = self.structures_gdf
        return gdfs

    @property
    @check_crs
    def n_cross_sections(self):
        """Number of cross sections in the HEC-RAS geometry file."""
        return len(self.cross_sections)

    @property
    @check_crs
    def n_structures(self):
        """Number of structures in the HEC-RAS geometry file."""
        return len(self.structures)

    @property
    @check_crs
    def n_reaches(self):
        """Number of reaches in the HEC-RAS geometry file."""
        return len(self.reaches)

    @property
    @check_crs
    def n_junctions(self):
        """Number of junctions in the HEC-RAS geometry file."""
        return len(self.junctions)

    @property
    @check_crs
    def n_rivers(self):
        """Number of rivers in the HEC-RAS geometry file."""
        return len(self.rivers)

    @check_crs
    def to_gpkg(self, gpkg_path: str):
        """Write the HEC-RAS Geometry file to geopackage."""
        self.xs_gdf.to_file(gpkg_path, driver="GPKG", layer="XS")
        self.reach_gdf.to_file(gpkg_path, driver="GPKG", layer="River")
        if self.junctions:
            self.junction_gdf.to_file(gpkg_path, driver="GPKG", layer="Junction")
        if self.structures:
            self.structures_gdf.to_file(gpkg_path, driver="GPKG", layer="Structure")

    @property
    def concave_hull(self):
        """Compute and return the concave hull (polygon) for cross sections."""
        if self._concave_hull is not None:  # cached hull
            return self._concave_hull
        polygons = []
        xs_df = self.xs_gdf  # shorthand
        for river_reach in xs_df["river_reach"].unique():
            xs_subset = xs_df[xs_df["river_reach"] == river_reach]
            points = xs_subset.boundary.explode(index_parts=True).unstack()
            points_last_xs = [Point(coord) for coord in xs_subset["geometry"].iloc[-1].coords]
            points_first_xs = [Point(coord) for coord in xs_subset["geometry"].iloc[0].coords[::-1]]
            polygon = Polygon(points_first_xs + list(points[0]) + points_last_xs + list(points[1])[::-1])
            if isinstance(polygon, MultiPolygon):
                polygons += list(polygon.geoms)
            else:
                polygons.append(polygon)
        if junction is not None:
            for _, j in junction.iterrows():
                polygons.append(junction_hull(xs, j))
        out_hull = [union_all([make_valid(p) for p in polygons])]
        self._concave_hull = gpd.GeoDataFrame({"geometry": out_hull}, geometry="geometry", crs=self.crs)
        return self._concave_hull

    @property
    def last_update(self):
        """Get the latest node last updated entry for this geometry"""
        dts = search_contents(self.file_str, "Node Last Edited Time", expect_one=False)
        if len(dts) >= 1:
            dts = [datetime.strptime(d, "%b/%d/%Y %H:%M:%S") for d in dts]
            return max(dts)
        else:
            return None

    @property
    def ras_version(self) -> str:
        """Version of ras (ex: '631') in geometry file"""
        version = search_contents(self.contents, "Program Version", expect_one=False)
        if len(version) >= 1:
            return version[0]
        else:
            return None
        return

    @property
    def units(self):
        """Units of the HEC-RAS project."""
        if "English Units" in self.file_str:
            return "English"
        else:
            return "Metric"

    def get_river_miles(self) -> float:
        """Compute the total length of the river centerlines in miles."""
        if "units" not in self.river_gdf.crs.to_dict().keys():
            raise RuntimeError("No units specified. The coordinate system may be Geographic.")
        units = self.river_gdf.crs.to_dict()["units"]
        if units in ["ft-us", "ft", "us-ft"]:
            conversion_factor = 1 / 5280
        elif units in ["m", "meters"]:
            conversion_factor = 1 / 1609
        else:
            raise RuntimeError(f"Expected feet or meters; got: {units}")
        return round(self.river_gdf.length.sum() * conversion_factor, 2)

    @property
    def has_2d(self):
        """Check if RAS geometry has any 2D areas"""
        lines = self.file_str.splitlines()
        for line in lines:
            if line.startswith("Storage Area Is2D=") and int(line[len("Storage Area Is2D=") :].strip()) in (1, -1):
                # RAS mostly uses "-1" to indicate True and "0" to indicate False. Checking for "1" also here.
                return True
        return False


class XS:
    """HEC-RAS Cross Section."""

    def __init__(self, ras_data: list, river_reach: str, river: str, reach: str, crs: str):
        self.ras_data = ras_data
        self.crs = crs
        self.river = river
        self.reach = reach
        self.river_reach = river_reach
        self.river_reach_rs = f"{river} {reach} {self.river_station}"

    def split_xs_header(self, position: int):
        """
        Split cross section header.

        Example: Type RM Length L Ch R = 1 ,83554.  ,237.02,192.39,113.07.
        """
        header = search_contents(self.ras_data, "Type RM Length L Ch R ", expect_one=True)

        return header.split(",")[position]

    @property
    def river_station(self):
        """Cross section river station."""
        return float(self.split_xs_header(1).replace("*", ""))

    @property
    def left_reach_length(self):
        """Cross section left reach length."""
        return float(self.split_xs_header(2))

    @property
    def channel_reach_length(self):
        """Cross section channel reach length."""
        return float(self.split_xs_header(3))

    @property
    def right_reach_length(self):
        """Cross section right reach length."""
        return float(self.split_xs_header(4))

    @property
    def number_of_coords(self):
        """Number of coordinates in cross section."""
        try:
            return int(search_contents(self.ras_data, "XS GIS Cut Line", expect_one=True))
        except ValueError as e:
            return 0
            # raise NotGeoreferencedError(f"No coordinates found for cross section: {self.river_reach_rs} ")

    @property
    def thalweg(self):
        """Cross section thalweg elevation."""
        if self.station_elevation_points:
            _, y = list(zip(*self.station_elevation_points))
            return min(y)

    @property
    def xs_max_elevation(self):
        """Cross section maximum elevation."""
        if self.station_elevation_points:
            _, y = list(zip(*self.station_elevation_points))
            return max(y)

    @property
    def coords(self):
        """Cross section coordinates."""
        lines = text_block_from_start_str_length(
            f"XS GIS Cut Line={self.number_of_coords}",
            math.ceil(self.number_of_coords / 2),
            self.ras_data,
        )
        if lines:
            return data_pairs_from_text_block(lines, 32)

    @property
    def number_of_station_elevation_points(self):
        """Number of station elevation points."""
        return int(search_contents(self.ras_data, "#Sta/Elev", expect_one=True))

    @property
    def station_elevation_points(self):
        """Station elevation points."""
        try:
            lines = text_block_from_start_str_length(
                f"#Sta/Elev= {self.number_of_station_elevation_points} ",
                math.ceil(self.number_of_station_elevation_points / 5),
                self.ras_data,
            )
            return data_pairs_from_text_block(lines, 16)
        except ValueError as e:
            return None

    @property
    def bank_stations(self):
        """Bank stations."""
        return search_contents(self.ras_data, "Bank Sta", expect_one=True).split(",")

    @property
    def gdf(self):
        """Cross section geodataframe."""
        return gpd.GeoDataFrame(
            {
                "geometry": [LineString(self.coords)],
                "river": [self.river],
                "reach": [self.reach],
                "river_reach": [self.river_reach],
                "river_station": [self.river_station],
                "river_reach_rs": [self.river_reach_rs],
                "thalweg": [self.thalweg],
                "xs_max_elevation": [self.xs_max_elevation],
                "left_reach_length": [self.left_reach_length],
                "right_reach_length": [self.right_reach_length],
                "channel_reach_length": [self.channel_reach_length],
                "ras_data": ["\n".join(self.ras_data)],
                "station_elevation_points": [self.station_elevation_points],
                "bank_stations": [self.bank_stations],
                "number_of_station_elevation_points": [self.number_of_station_elevation_points],
                "number_of_coords": [self.number_of_coords],
                # "coords": [self.coords],
            },
            crs=self.crs,
            geometry="geometry",
        )


class Structure:
    """Structure."""

    def __init__(self, ras_data: list, river_reach: str, river: str, reach: str, crs: str, us_xs: XS):
        self.ras_data = ras_data
        self.crs = crs
        self.river = river
        self.reach = reach
        self.river_reach = river_reach
        self.river_reach_rs = f"{river} {reach} {self.river_station}"
        self.us_xs = us_xs

    def split_structure_header(self, position: int):
        """
        Split Structure header.

        Example: Type RM Length L Ch R = 3 ,83554.  ,237.02,192.39,113.07.
        """
        header = search_contents(self.ras_data, "Type RM Length L Ch R ", expect_one=True)

        return header.split(",")[position]

    @property
    def river_station(self):
        """Structure river station."""
        return float(self.split_structure_header(1))

    @property
    def type(self):
        """Structure type."""
        return int(self.split_structure_header(0))

    def structure_data(self, position: int):
        """Structure data."""
        if self.type in [2, 3, 4]:  # culvert or bridge
            data = text_block_from_start_str_length(
                "Deck Dist Width WeirC Skew NumUp NumDn MinLoCord MaxHiCord MaxSubmerge Is_Ogee", 1, self.ras_data
            )
            return data[0].split(",")[position]
        elif self.type == 5:  # inline weir
            data = text_block_from_start_str_length(
                "IW Dist,WD,Coef,Skew,MaxSub,Min_El,Is_Ogee,SpillHt,DesHd", 1, self.ras_data
            )
            return data[0].split(",")[position]
        elif self.type == 6:  # lateral structure
            return 0

    @property
    def distance(self):
        """Distance to upstream cross section."""
        return float(self.structure_data(0))

    @property
    def width(self):
        """Structure width."""
        # TODO check units of the RAS model
        return float(self.structure_data(1))

    @property
    def gdf(self):
        """Structure geodataframe."""
        return gpd.GeoDataFrame(
            {
                "geometry": [LineString(self.us_xs.coords).offset_curve(self.distance)],
                "river": [self.river],
                "reach": [self.reach],
                "river_reach": [self.river_reach],
                "river_station": [self.river_station],
                "river_reach_rs": [self.river_reach_rs],
                "type": [self.type],
                "distance": [self.distance],
                "width": [self.width],
                "ras_data": ["\n".join(self.ras_data)],
            },
            crs=self.crs,
            geometry="geometry",
        )


class Reach:
    """HEC-RAS River Reach."""

    def __init__(self, ras_data: list, river_reach: str, crs: str):
        reach_lines = text_block_from_start_end_str(f"River Reach={river_reach}", ["River Reach"], ras_data, -1)
        self.ras_data = reach_lines
        self.crs = crs
        self.river_reach = river_reach
        self.river = river_reach.split(",")[0].rstrip()
        self.reach = river_reach.split(",")[1].rstrip()

        us_connection: str = None
        ds_connection: str = None

    @property
    def us_xs(self):
        """Upstream cross section."""
        return self.cross_sections[
            self.xs_gdf.loc[
                self.xs_gdf["river_station"] == self.xs_gdf["river_station"].max(),
                "river_reach_rs",
            ][0]
        ]

    @property
    def ds_xs(self):
        """Downstream cross section."""
        return self.cross_sections[
            self.xs_gdf.loc[
                self.xs_gdf["river_station"] == self.xs_gdf["river_station"].min(),
                "river_reach_rs",
            ][0]
        ]

    @property
    def number_of_cross_sections(self):
        """Number of cross sections."""
        return len(self.cross_sections)

    @property
    def number_of_coords(self):
        """Number of coordinates in reach."""
        return int(search_contents(self.ras_data, "Reach XY"))

    @property
    def coords(self):
        """Reach coordinates."""
        lines = text_block_from_start_str_length(
            f"Reach XY= {self.number_of_coords} ",
            math.ceil(self.number_of_coords / 2),
            self.ras_data,
        )
        return data_pairs_from_text_block(lines, 32)

    @property
    def reach_nodes(self):
        """Reach nodes."""
        return search_contents(self.ras_data, "Type RM Length L Ch R ", expect_one=False)

    @property
    def cross_sections(self):
        """Cross sections."""
        cross_sections = {}
        for header in self.reach_nodes:
            type, _, _, _, _ = header.split(",")[:5]
            if int(type) != 1:
                continue
            xs_lines = text_block_from_start_end_str(
                f"Type RM Length L Ch R ={header}",
                ["Type RM Length L Ch R", "River Reach"],
                self.ras_data,
            )
            cross_section = XS(xs_lines, self.river_reach, self.river, self.reach, self.crs)
            cross_sections[cross_section.river_reach_rs] = cross_section

        return cross_sections

    @property
    def structures(self):
        """Structures."""
        structures = {}
        for header in self.reach_nodes:
            type, _, _, _, _ = header.split(",")[:5]
            if int(type) == 1:
                xs_lines = text_block_from_start_end_str(
                    f"Type RM Length L Ch R ={header}",
                    ["Type RM Length L Ch R", "River Reach"],
                    self.ras_data,
                )
                cross_section = XS(xs_lines, self.river_reach, self.river, self.reach, self.crs)
                continue
            elif int(type) in [2, 3, 4, 5, 6]:  # culvert or bridge or multiple openeing
                structure_lines = text_block_from_start_end_str(
                    f"Type RM Length L Ch R ={header}",
                    ["Type RM Length L Ch R", "River Reach"],
                    self.ras_data,
                )
            else:
                raise TypeError(
                    f"Unsupported structure type: {int(type)}. Supported structure types are 2, 3, 4, 5, and 6 corresponding to culvert, bridge, multiple openeing, inline structure, lateral structure, respectively"
                )

            structure = Structure(structure_lines, self.river_reach, self.river, self.reach, self.crs, cross_section)
            structures[structure.river_reach_rs] = structure

        return structures

    @property
    def gdf(self):
        """Reach geodataframe."""
        return gpd.GeoDataFrame(
            {
                "geometry": [LineString(self.coords)],
                "river": [self.river],
                "reach": [self.reach],
                "river_reach": [self.river_reach],
                # "number_of_coords": [self.number_of_coords],
                # "coords": [self.coords],
                "ras_data": ["\n".join(self.ras_data)],
            },
            crs=self.crs,
            geometry="geometry",
        )

    @property
    def xs_gdf(self):
        """Cross section geodataframe."""
        return pd.concat([xs.gdf for xs in self.cross_sections.values()])

    @property
    def structures_gdf(self):
        """Structures geodataframe."""
        return pd.concat([structure.gdf for structure in self.structures.values()])


class Junction:
    """HEC-RAS Junction."""

    def __init__(self, ras_data: List[str], junct: str, crs: str):
        self.crs = crs
        self.name = junct
        self.ras_data = text_block_from_start_str_to_empty_line(f"Junct Name={junct}", ras_data)

    def split_lines(self, lines: str, token: str, idx: int):
        """Split lines."""
        return list(map(lambda line: line.split(token)[idx].rstrip(), lines))

    @property
    def x(self):
        """Junction x coordinate."""
        return self.split_lines([search_contents(self.ras_data, "Junct X Y & Text X Y")], ",", 0)

    @property
    def y(self):
        """Junction y coordinate."""
        return self.split_lines([search_contents(self.ras_data, "Junct X Y & Text X Y")], ",", 1)

    @property
    def point(self):
        """Junction point."""
        return Point(self.x, self.y)

    @property
    def upstream_rivers(self):
        """Upstream rivers."""
        return ",".join(
            self.split_lines(
                search_contents(self.ras_data, "Up River,Reach", expect_one=False),
                ",",
                0,
            )
        )

    @property
    def downstream_rivers(self):
        """Downstream rivers."""
        return ",".join(
            self.split_lines(
                search_contents(self.ras_data, "Dn River,Reach", expect_one=False),
                ",",
                0,
            )
        )

    @property
    def upstream_reaches(self):
        """Upstream reaches."""
        return ",".join(
            self.split_lines(
                search_contents(self.ras_data, "Up River,Reach", expect_one=False),
                ",",
                1,
            )
        )

    @property
    def downstream_reaches(self):
        """Downstream reaches."""
        return ",".join(
            self.split_lines(
                search_contents(self.ras_data, "Dn River,Reach", expect_one=False),
                ",",
                1,
            )
        )

    @property
    def junction_lengths(self):
        """Junction lengths."""
        return ",".join(self.split_lines(search_contents(self.ras_data, "Junc L&A", expect_one=False), ",", 0))

    @property
    def gdf(self):
        """Junction geodataframe."""
        return gpd.GeoDataFrame(
            {
                "geometry": [self.point],
                "junction_lengths": [self.junction_lengths],
                "us_rivers": [self.upstream_rivers],
                "ds_rivers": [self.downstream_rivers],
                "us_reaches": [self.upstream_reaches],
                "ds_reaches": [self.downstream_reaches],
                "ras_data": ["\n".join(self.ras_data)],
            },
            geometry="geometry",
            crs=self.crs,
        )
