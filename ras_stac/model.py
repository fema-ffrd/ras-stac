from datetime import datetime
from functools import cached_property
from pathlib import Path

import numpy as np
from pystac import Item

from ras_stac.ras1d.utils.classes import (
    GeometryAsset,
    PlanAsset,
    ProjectAsset,
    QuasiUnsteadyFlowAsset,
    SteadyFlowAsset,
    UnsteadyFlowAsset,
)
from ras_stac.ras1d.utils.stac_utils import asset_factory


class RasModel(Item):
    """An object representation of a HEC-RAS 1D model."""

    def __init__(self, prj_file: str) -> None:
        self.id = Path(prj_file).stem
        self._project = None
        self._dt_source = None
        self.collection_id: str | None = None
        self.collection = None
        self.assets = {}
        self.links = []
        self.stac_extensions = None
        self.set_self_href(prj_file)
        self.autofind_project_assets(Path(prj_file).parent)

        self.extra_fields = {}
        self.properties = {}
        self.properties["ras:project"] = self.project.name
        self.properties["ras:project_title"] = self.project.ras_title
        self.properties["ras:project_directory"] = str(Path(self.project.href).parent)
        self.properties["ras:units"] = self.project.units
        self.properties["ras:plan_files"] = self.plan_summary
        self.properties["ras:geometry_files"] = [i.short_summary for i in self.geometry_files]
        self.properties["ras:steady_flow_files"] = [i.short_summary for i in self.steady_flow_files]
        self.properties["ras:quasi_unsteady_flow_files"] = [i.short_summary for i in self.quasi_unsteady_flow_files]
        self.properties["ras:unsteady_flow_files"] = [i.short_summary for i in self.unsteady_flow_files]
        geom_summary_fields = [
            "ras:rivers",
            "ras:reaches",
            "ras:cross_sections",
            "ras:culverts",
            "ras:bridges",
            "ras:multiple_openings",
            "ras:inline_structures",
            "ras:lateral_structures",
            "ras:storage_areas",
            "ras:2d_flow_areas",
            "ras:sa_connections",
        ]
        for field in geom_summary_fields:
            self.properties[field] = self.geometry_current.extra_fields[field]
        self.properties["start_datetime"] = self.start_datetime
        self.properties["end_datetime"] = self.start_datetime
        self.properties["datetime"] = self.start_datetime

        self.stac_extensions = ["https://github.com/fema-ffrd/ras-stac/extensions/schema.json"]

    def to_stac(self) -> dict:
        """Convert this object to a PySTAC Item dictionary."""
        return super().to_dict()

    def autofind_project_assets(self, search_dir: str | Path) -> None:
        """Scan the project directory for files with the same stem and add them to the item."""
        fpaths = [f for f in Path(search_dir).iterdir() if f.stem == self.id]
        for f in fpaths:
            self.add_asset(f)

    def add_asset(self, url: str) -> None:
        """Add an asset to the item."""
        asset = asset_factory(url)
        super().add_asset(asset.title, asset)
        if isinstance(asset, ProjectAsset):
            if self._project is not None:
                f"Only one project asset is allowed. Found {str(asset)} when {str(self._project)} was already set."
            self._project = asset

    @property
    def bbox(self) -> list[float]:
        if len(self.geometry_files) == 0:
            return [0, 0, 0, 0]
        else:
            bboxes = np.array([i.bbox for i in self.geometry_files])
            bboxes = [bboxes[:, 0].min(), bboxes[:, 1].min(), bboxes[:, 2].max(), bboxes[:, 3].max()]
            return [float(i) for i in bboxes]

    @property
    def geometry(self) -> dict | None:
        if self.geometry_current is None:
            return None
        else:
            return self.geometry_current.geometry

    @property
    def datetime(self) -> str | None:
        dts = self.geometry_datetimes
        if len(dts) == 1:
            return str(dts[0])
        else:
            if max(dts) == min(dts):
                return str(dts[0])
            else:
                return None

    @property
    def start_datetime(self) -> str | None:
        dts = self.geometry_datetimes
        if len(dts) > 1:
            if max(dts) == min(dts):
                return None
            else:
                return str(min(dts))
        else:
            return None

    @property
    def end_datetime(self) -> str | None:
        dts = self.geometry_datetimes
        if len(dts) > 1:
            if max(dts) == min(dts):
                return None
            else:
                return str(max(dts))
        else:
            return None

    @property
    def plan_current(self) -> PlanAsset | None:
        return self.assets[self.project.plan_current]

    @property
    def geometry_current(self) -> GeometryAsset | None:
        if self.plan_current is None:
            return None
        else:
            return self.assets[self.plan_current.primary_geometry]

    @property
    def plan_files(self) -> list[PlanAsset]:
        return [self.assets[f] for f in self.project.plan_files]

    @property
    def geometry_files(self) -> list[GeometryAsset]:
        if self.project is None:
            return []
        else:
            return [self.assets[f] for f in self.project.geometry_files]

    @property
    def steady_flow_files(self) -> list[SteadyFlowAsset]:
        if self.project is None:
            return []
        else:
            return [self.assets[f] for f in self.project.steady_flow_files]

    @property
    def quasi_unsteady_flow_files(self) -> list[QuasiUnsteadyFlowAsset]:
        if self.project is None:
            return []
        else:
            return [self.assets[f] for f in self.project.quasi_unsteady_flow_files]

    @property
    def unsteady_flow_files(self) -> list[UnsteadyFlowAsset]:
        if self.project is None:
            return []
        else:
            return [self.assets[f] for f in self.project.unsteady_flow_files]

    @property
    def plan_summary(self) -> list[dict]:
        out_list = []
        for f in self.project.plan_files:
            tmp_plan = self.assets[f]
            tmp_geom = self.assets[tmp_plan.primary_geometry]
            tmp_flow = self.assets[tmp_plan.primary_flow]
            tmp_obj = {
                "current": f == self.project.plan_current,
                "title": tmp_plan.ras_title,
                "short_id": tmp_plan.short_id,
                "file": str(tmp_plan.name),
                "geometry": tmp_geom.short_summary,
                "flow": tmp_flow.short_summary,
            }
            out_list.append(tmp_obj)
        return out_list

    @property
    def project(self) -> ProjectAsset:
        return self._project

    @cached_property
    def geometry_datetimes(self) -> list[datetime]:
        dts = []
        for i in self.geometry_files:
            dts.extend(i.datetimes)
        if len(dts) == 0:
            self._dt_source = "processing time"
            dts = [datetime.now()]
        else:
            self._dt_source = "model geometry"
        return dts
