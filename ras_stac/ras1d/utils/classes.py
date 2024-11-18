from pathlib import Path

from pystac import Asset, MediaType

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

        if href.endswith(".hdf"):
            self.roles.append(MediaType.HDF5)

    def name_from_suffix(self, suffix: str) -> str:
        return self.stem + "." + suffix


class ProjectAsset(GenericAsset):

    def __init__(self, href, *args, **kwargs):
        super().__init__(href, *args, **kwargs)

        if not href.endswith(".hdf"):
            self.roles.append(MediaType.TEXT)

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

    def __init__(self, href: str, *args, **kwargs):
        super().__init__(href, *args, **kwargs)
        self.extra_fields = {"short_id": self.short_id}

        if not href.endswith(".hdf"):
            self.roles.append(MediaType.TEXT)

    @property
    def ras1d_title(self) -> str:
        return search_contents(self.file_str.splitlines(), "Plan Title")

    @property
    def primary_geometry(self) -> str:
        suffix = search_contents(self.file_str.splitlines(), "Geom File", expect_one=True)
        return self.name_from_suffix(suffix)

    @property
    def short_id(self) -> str:
        return search_contents(self.file_str.splitlines(), "Short Identifier")


class GeometryAsset(GenericAsset):

    def __init__(self, href: str = "null", *args, **kwargs):
        super().__init__(href, *args, **kwargs)

        self.extra_fields = {
            "ras1d:rivers": 0,
            "ras1d:reaches": 0,
            "ras1d:cross_sections": {
                "total": 0,
                "user_input_xss": 0,
                "interpolated": 0,
            },
            "ras1d:culverts": 0,
            "ras1d:bridges": 0,
            "ras1d:multiple_openings": 0,
            "ras1d:inline_structures": 0,
            "ras1d:lateral_structures": 0,
            "ras1d:storage_areas": 0,
            "ras1d:2d_flow_areas": {
                "2d_flow_areas": 0,
                "total_cells": 0,
            },
            "ras1d:sa_connections": 0,
        }
        self.geometry = us_bounds
        self.bbox = [0, 0, 0, 0]

        if not href.endswith(".hdf"):
            self.roles.append(MediaType.TEXT)

    @property
    def ras1d_title(self) -> str:
        return search_contents(self.file_str.splitlines(), "Geom Title")


class SteadyFlowAsset(GenericAsset):

    def __init__(self, href, *args, **kwargs):
        super().__init__(href, *args, **kwargs)


class QuasiUnsteadyFlowAsset(GenericAsset):

    def __init__(self, href, *args, **kwargs):
        super().__init__(href, *args, **kwargs)


class UnsteadyFlowAsset(GenericAsset):

    def __init__(self, href, *args, **kwargs):
        super().__init__(href, *args, **kwargs)
