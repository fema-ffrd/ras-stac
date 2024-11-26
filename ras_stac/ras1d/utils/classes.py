import math
from collections import defaultdict
from functools import cached_property
from pathlib import Path

import geopandas as gpd
import pandas as pd
from pystac import Asset, MediaType
from shapely.geometry import LineString, Point

from ras_stac.ras1d.data.us_geom import us_bounds
from ras_stac.ras1d.utils.ras_utils import (
    data_pairs_from_text_block,
    delimited_pairs_to_lists,
    search_contents,
    text_block_from_start_end_str,
    text_block_from_start_str_length,
    text_block_from_start_str_to_empty_line,
)


class GenericAsset(Asset):

    def __init__(self, href: str, *args, **kwargs):
        super().__init__(href, *args, **kwargs)
        self.name = Path(href).name
        self.stem = Path(href).stem
        with open(href) as f:
            self.file_str = f.read()

        if href.endswith(".hdf"):
            self.roles.append(MediaType.HDF5)

    def name_from_suffix(self, suffix: str) -> str:
        return self.stem + "." + suffix

    @cached_property
    def program_version(self) -> str:
        """The HEC-RAS version last used to modify the file."""
        return search_contents(self.contents, "Program Version", expect_one=False)


class ProjectAsset(GenericAsset):

    def __init__(self, href, *args, **kwargs):
        super().__init__(href, *args, **kwargs)
        self.extra_fields["ras1d:project_title"] = self.ras1d_title
        self.extra_fields["ras1d:plan_current"] = self.plan_current

        if not href.endswith(".hdf"):
            self.roles.append(MediaType.TEXT)

    @cached_property
    def ras1d_title(self) -> str:
        return search_contents(self.file_str.splitlines(), "Proj Title")

    @cached_property
    def units(self) -> str:
        for line in self.file_str.splitlines():
            if "Units" in line:
                return " ".join(line.split(" ")[:-1])

    @cached_property
    def plan_current(self) -> str:
        suffix = search_contents(self.file_str.splitlines(), "Current Plan", expect_one=True)
        return self.name_from_suffix(suffix)

    @cached_property
    def plan_files(self) -> list[str]:
        suffixes = search_contents(self.file_str.splitlines(), "Plan File", expect_one=False)
        return [self.name_from_suffix(i) for i in suffixes]

    @cached_property
    def geometry_files(self) -> list[str]:
        suffixes = search_contents(self.file_str.splitlines(), "Geometry File", expect_one=False)
        return [self.name_from_suffix(i) for i in suffixes]

    @cached_property
    def steady_flow_files(self) -> list[str]:
        suffixes = search_contents(self.file_str.splitlines(), "Flow File", expect_one=False)
        return [self.name_from_suffix(i) for i in suffixes]

    @cached_property
    def quasi_unsteady_flow_files(self) -> list[str]:
        suffixes = search_contents(self.file_str.splitlines(), "QuasiSteady File", expect_one=False)
        return [self.name_from_suffix(i) for i in suffixes]

    @cached_property
    def unsteady_flow_files(self) -> list[str]:
        suffixes = search_contents(self.file_str.splitlines(), "Unsteady File", expect_one=False)
        return [self.name_from_suffix(i) for i in suffixes]


class PlanAsset(GenericAsset):

    def __init__(self, href: str, *args, **kwargs):
        super().__init__(href, *args, **kwargs)
        self.extra_fields["ras1d:plan_title"] = self.ras1d_title
        self.extra_fields["ras1d:short_id"] = self.short_id
        self.extra_fields["ras1d:geometry"] = self.primary_geometry
        self.extra_fields["ras1d:flow"] = self.primary_flow

        if not href.endswith(".hdf"):
            self.roles.append(MediaType.TEXT)

    @cached_property
    def ras1d_title(self) -> str:
        return search_contents(self.file_str.splitlines(), "Plan Title")

    @cached_property
    def primary_geometry(self) -> str:
        suffix = search_contents(self.file_str.splitlines(), "Geom File", expect_one=True)
        return self.name_from_suffix(suffix)

    @cached_property
    def primary_flow(self) -> str:
        suffix = search_contents(self.file_str.splitlines(), "Flow File", expect_one=True)
        return self.name_from_suffix(suffix)

    @cached_property
    def short_id(self) -> str:
        return search_contents(self.file_str.splitlines(), "Short Identifier")


class GeometryAsset(GenericAsset):

    def __init__(self, href: str = "null", *args, **kwargs):
        super().__init__(href, *args, **kwargs)

        self.extra_fields = {
            "ras1d:geom_title": self.ras1d_title,
            "ras1d:rivers": len(self.rivers),
            "ras1d:reaches": len(self.reaches),
            "ras1d:cross_sections": {
                "total": len(self.cross_sections),
                "user_input_xss": len([xs for xs in self.cross_sections.values() if not xs.is_interpolated]),
                "interpolated": len([xs for xs in self.cross_sections.values() if xs.is_interpolated]),
            },
            "ras1d:culverts": len([s for s in self.structures.values() if s.type == 2]),
            "ras1d:bridges": len([s for s in self.structures.values() if s.type == 3]),
            "ras1d:multiple_openings": len([s for s in self.structures.values() if s.type == 4]),
            "ras1d:inline_structures": len([s for s in self.structures.values() if s.type == 5]),
            "ras1d:lateral_structures": len([s for s in self.structures.values() if s.type == 6]),
            "ras1d:storage_areas": 0,
            "ras1d:2d_flow_areas": {
                "2d_flow_areas": 0,
                "total_cells": 0,
            },
            "ras1d:sa_connections": 0,
        }
        self.geometry = us_bounds
        self.bbox = [0, 0, 0, 0]
        self._junctions = None
        self._cross_sections = None

        if not href.endswith(".hdf"):
            self.roles.append(MediaType.TEXT)

    @cached_property
    def ras1d_title(self) -> str:
        return search_contents(self.file_str.splitlines(), "Geom Title")

    @cached_property
    def rivers(self) -> dict[str, "River"]:
        """A dictionary of river_name: River (class) for the rivers contained in the HEC-RAS geometry file."""
        tmp_rivers = defaultdict(list)
        for reach in self.reaches.values():  # First, group all reaches into their respective rivers
            tmp_rivers[reach.river].append(reach.reach)
        for river, reaches in tmp_rivers.items():  # Then, create a River object for each river
            tmp_rivers[river] = River(river, reaches)
        return tmp_rivers

    @cached_property
    def reaches(self) -> dict[str, "Reach"]:
        """A dictionary of the reaches contained in the HEC-RAS geometry file."""
        river_reaches = search_contents(self.contents, "River Reach", expect_one=False)
        return {river_reach: Reach(self.contents, river_reach, self.crs) for river_reach in river_reaches}

    @cached_property
    def junctions(self) -> dict[str, "Junction"]:
        """A dictionary of the junctions contained in the HEC-RAS geometry file."""
        juncts = search_contents(self.contents, "Junct Name", expect_one=False)
        return {junction: Junction(self.contents, junction, self.crs) for junction in juncts}

    @cached_property
    def cross_sections(self) -> dict[str, "XS"]:
        """A dictionary of all the cross sections contained in the HEC-RAS geometry file."""
        cross_sections = {}
        for reach in self.reaches.values():
            cross_sections.update(reach.cross_sections)
        return cross_sections

    @cached_property
    def structures(self) -> dict[str, "Structure"]:
        """A dictionary of the structures contained in the HEC-RAS geometry file."""
        structures = {}
        for reach in self.reaches.values():
            structures.update(reach.structures)
        return structures

    @cached_property
    def storage_areas(self) -> dict[str, "StorageArea"]:
        """A dictionary of the storage areas contained in the HEC-RAS geometry file."""
        areas = search_contents(self.contents, "Storage Area", expect_one=False)
        return {a: StorageArea(a, self.crs) for a in areas}

    @cached_property
    def connections(self) -> dict[str, "Connection"]:
        """A dictionary of the SA/2D connections contained in the HEC-RAS geometry file."""
        connections = search_contents(self.contents, "Connection", expect_one=False)
        return {c: Connection(c, self.crs) for c in connections}

    def get_subtype_gdf(self, subtype: str) -> gpd.GeoDataFrame:
        """Get a geodataframe of a specific subtype of geometry asset."""
        tmp_objs = getattr(self, subtype)
        return gpd.GeoDataFrame(
            pd.concat([obj.gdf for obj in tmp_objs.values()], ignore_index=True)
        )  # TODO: may need to add some logic here for empy dicts

    def to_gpkg(self, gpkg_path: str):
        """Write the HEC-RAS Geometry file to geopackage."""
        for subtype in ["rivers", "reaches", "junctions", "cross_sections", "structures"]:
            gdf = self.get_subtype_gdf(subtype)
            gdf.to_file(gpkg_path, driver="GPKG", layer=subtype, ignore_index=True)


class SteadyFlowAsset(GenericAsset):

    def __init__(self, href, *args, **kwargs):
        super().__init__(href, *args, **kwargs)
        self.extra_fields["ras1d:flow_title"] = self.ras1d_title
        self.extra_fields["ras1d:number_of_profiles"] = self.n_profiles

    @property
    def ras1d_title(self) -> str:
        return search_contents(self.file_str.splitlines(), "Flow Title")

    @property
    def n_profiles(self) -> int:
        return int(search_contents(self.file_str.splitlines(), "Number of Profiles"))


class QuasiUnsteadyFlowAsset(GenericAsset):

    def __init__(self, href, *args, **kwargs):
        super().__init__(href, *args, **kwargs)
        self.extra_fields["ras1d:flow_title"] = self.ras1d_title


class UnsteadyFlowAsset(GenericAsset):

    def __init__(self, href, *args, **kwargs):
        super().__init__(href, *args, **kwargs)
        self.extra_fields["ras1d:flow_title"] = self.ras1d_title


### Geometry Assets ##


class River:

    def __init__(self, river: str, reaches: list[str] = []):
        self.river = river
        self.reaches = reaches


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
        except ValueError:
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
        except ValueError:
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

    @property
    def n_subdivisions(self):
        """Get the number of subdivisions (defined by manning's n)."""
        return int(search_contents(self.ras_data, "#Mann", expect_one=True).split(",")[0])

    @property
    def subdivision_type(self):
        """Get the subdivision type.

        -1 seems to indicate horizontally-varied n.  0 seems to indicate subdivisions by LOB, channel, ROB.
        """
        return int(search_contents(self.ras_data, "#Mann", expect_one=True).split(",")[1])

    @property
    def subdivisions(self):
        """Get the stations corresponding to subdivision breaks, along with their roughness."""
        try:
            header = [l for l in self.ras_data if l.startswith("#Mann")][0]
            lines = text_block_from_start_str_length(
                header,
                math.ceil(self.n_subdivisions / 3),
                self.ras_data,
            )

            return delimited_pairs_to_lists(lines)
        except ValueError:
            return None

    def wse_intersection_pts(self, wse: float) -> list[tuple[float]]:
        """Find where the cross-section terrain intersects the water-surface elevation."""
        section_pts = self.station_elevation_points
        intersection_pts = []

        # Iterate through all pairs of points and find any points where the line would cross the wse
        for i in range(len(section_pts) - 1):
            p1 = section_pts[i]
            p2 = section_pts[i + 1]

            if p1[1] > wse and p2[1] > wse:  # No intesection
                continue
            elif p1[1] < wse and p2[1] < wse:  # Both below wse
                continue

            # Define line
            m = (p2[1] - p1[1]) / (p2[0] - p1[0])
            b = p1[1] - (m * p1[0])

            # Find intersection point with Cramer's rule
            determinant = lambda a, b: (a[0] * b[1]) - (a[1] * b[0])
            div = determinant((1, 1), (-m, 0))
            tmp_y = determinant((b, wse), (-m, 0)) / div
            tmp_x = determinant((1, 1), (b, wse)) / div

            intersection_pts.append((tmp_x, tmp_y))
        return intersection_pts

    def get_wetted_perimeter(self, wse: float, start: float = None, stop: float = None) -> float:
        """Get the hydraulic radius of the cross-section at a given WSE."""
        df = pd.DataFrame(self.station_elevation_points, columns=["x", "y"])
        df = pd.concat([df, pd.DataFrame(self.wse_intersection_pts(wse), columns=["x", "y"])])
        if start is not None:
            df = df[df["x"] >= start]
        if stop is not None:
            df = df[df["x"] <= stop]
        df = df.sort_values("x", ascending=True)
        df = df[df["y"] <= wse]
        if len(df) == 0:
            return 0
        df["dx"] = df["x"].diff(-1)
        df["dy"] = df["y"].diff(-1)
        df["d"] = ((df["x"] ** 2) + (df["y"] ** 2)) ** (0.5)

        return df["d"].cumsum().values[0]

    def get_flow_area(self, wse: float, start: float = None, stop: float = None) -> float:
        """Get the flow area of the cross-section at a given WSE."""
        df = pd.DataFrame(self.station_elevation_points, columns=["x", "y"])
        df = pd.concat([df, pd.DataFrame(self.wse_intersection_pts(wse), columns=["x", "y"])])
        if start is not None:
            df = df[df["x"] >= start]
        if stop is not None:
            df = df[df["x"] <= stop]
        df = df.sort_values("x", ascending=True)
        df = df[df["y"] <= wse]
        if len(df) == 0:
            return 0
        df["d"] = wse - df["y"]  # depth
        df["d2"] = df["d"].shift(-1)
        df["x2"] = df["x"].shift(-1)
        df["a"] = ((df["d"] + df["d2"]) / 2) * (df["x2"] - df["x"])  # area of a trapezoid

        return df["a"].cumsum().values[0]

    def get_mannings_discharge(self, wse: float, slope: float, units: str) -> float:
        """Calculate the discharge of the cross-section according to manning's equation."""
        q = 0
        stations, mannings = self.subdivisions
        slope = slope**0.5  # pre-process slope for efficiency
        for i in range(self.n_subdivisions - 1):
            start = stations[i]
            stop = stations[i + 1]
            n = mannings[i]
            area = self.get_flow_area(wse, start, stop)
            if area == 0:
                continue
            perimeter = self.get_wetted_perimeter(wse, start, stop)
            rh = area / perimeter
            tmp_q = (1 / n) * area * (rh ** (2 / 3)) * slope
            if units == "english":
                tmp_q *= 1.49
            q += (1 / n) * area * (rh ** (2 / 3)) * slope
        return q


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
        if self.type in [2, 3, 4]:  # 1 = Cross Section, 2 = Culvert, 3 = Bridge, 4 = Multiple Opening
            data = text_block_from_start_str_length(
                "Deck Dist Width WeirC Skew NumUp NumDn MinLoCord MaxHiCord MaxSubmerge Is_Ogee", 1, self.ras_data
            )
            return data[0].split(",")[position]
        elif self.type == 5:  # 5 = Inline Structure
            data = text_block_from_start_str_length(
                "IW Dist,WD,Coef,Skew,MaxSub,Min_El,Is_Ogee,SpillHt,DesHd", 1, self.ras_data
            )
            return data[0].split(",")[position]
        elif self.type == 6:  # 6 = Lateral Structure
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

    def __init__(self, ras_data: list[str], junct: str, crs: str):
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


class StorageArea:

    def __init__(self, ras_data: list, crs: str):
        self.crs = crs
        self.ras_data = ras_data
        # TODO: Implement this


class Connection:

    def __init__(self, ras_data: list, crs: str):
        self.crs = crs
        self.ras_data = ras_data
        # TODO: Implement this
