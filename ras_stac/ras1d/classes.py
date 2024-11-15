from pathlib import Path

from pystac import Asset

from ras_stac.ras1d.data.us_geom import us_bounds
from ras_stac.ras1d.utils.ras_utils import (
    search_contents,
)


class GenericAsset(Asset):

    def __init__(self, href: str, *args, **kwargs):
        super().__init__(href, *args, **kwargs)
        self.name = Path(href).name
        self.stem = Path(href).stem
        with open(href) as f:
            self.file_str = f.read()

    def __str__(self):
        return self.name

    def name_from_suffix(self, suffix: str) -> str:
        return self.stem + "." + suffix


class ProjectAsset(GenericAsset):

    @property
    def ras1d_title(self) -> str:
        return search_contents(self.file_str.splitlines(), "Proj Title")

    @property
    def units(self) -> str:
        for line in self.file_str.splitlines():
            if "Units" in line:
                return " ".join(line.split(" ")[:-1])

    @property
    def plan_current(self) -> str:
        suffix = search_contents(self.file_str.splitlines(), "Current Plan", expect_one=True)
        return self.name_from_suffix(suffix)

    @property
    def plan_files(self) -> list[str]:
        suffixes = search_contents(self.file_str.splitlines(), "Plan File", expect_one=False)
        return [self.name_from_suffix(i) for i in suffixes]

    @property
    def geometry_files(self) -> list[str]:
        suffixes = search_contents(self.file_str.splitlines(), "Geometry File", expect_one=False)
        return [self.name_from_suffix(i) for i in suffixes]

    @property
    def steady_flow_files(self) -> list[str]:
        suffixes = search_contents(self.file_str.splitlines(), "Flow File", expect_one=False)
        return [self.name_from_suffix(i) for i in suffixes]

    @property
    def quasi_unsteady_flow_files(self) -> list[str]:
        suffixes = search_contents(self.file_str.splitlines(), "QuasiSteady File", expect_one=False)
        return [self.name_from_suffix(i) for i in suffixes]

    @property
    def unsteady_flow_files(self) -> list[str]:
        suffixes = search_contents(self.file_str.splitlines(), "Unsteady File", expect_one=False)
        return [self.name_from_suffix(i) for i in suffixes]


class PlanAsset(GenericAsset):

    @property
    def primary_geometry(self) -> str:
        suffix = search_contents(self.file_str.splitlines(), "Geom File", expect_one=True)
        return self.name_from_suffix(suffix)


class GeometryAsset(GenericAsset):

    def __init__(self, href: str, *args, **kwargs):
        super().__init__(href, *args, **kwargs)

        geom_fields = [
            "rivers",
            "reaches",
            "ras1d:cross_sections",
            "ras1d:culverts",
            "ras1d:bridges",
            "ras1d:multiple_openings",
            "ras1d:inline_structures",
            "ras1d:lateral_structures",
            "ras1d:storage_areas",
            "ras1d:2d_flow_areas",
            "ras1d:sa_connections",
        ]
        self.extra_fields = {f: None for f in geom_fields}
        self.geometry = us_bounds
        self.bbox = [0, 0, 0, 0]
