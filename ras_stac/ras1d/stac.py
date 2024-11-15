from datetime import datetime
from pathlib import Path

import numpy as np
from pystac import Asset, Item

from ras_stac.ras1d.classes import ProjectAsset
from ras_stac.utils.stac_utils import asset_factory


class Ras1dModel(Item):
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
        self.properties["ras1d:project"] = self.project.name
        self.properties["ras1d:project_title"] = self.project.ras1d_title
        self.properties["ras1d:units"] = self.project.units
        self.properties["ras1d:plan_current"] = str(self.plan_current)
        self.properties["ras1d:plan_files"] = [str(i) for i in self.plan_files]
        self.properties["ras1d:geometry_files"] = [str(i) for i in self.geometry_files]
        self.properties["ras1d:steady_flow_files"] = [str(i) for i in self.steady_flow_files]
        self.properties["ras1d:quasi_unsteady_flow_files"] = [str(i) for i in self.quasi_unsteady_flow_files]
        self.properties["ras1d:unsteady_flow_files"] = [str(i) for i in self.unsteady_flow_files]
        geom_summary_fields = [
            "ras1d:rivers",
            "ras1d:reaches",
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
        for field in geom_summary_fields:
            self.properties[field] = self.geometry_current.extra_fields[field]

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
            return [bboxes[:, 0].min(), bboxes[:, 1].min(), bboxes[:, 2].max(), bboxes[:, 3].max()]

    @property
    def geometry(self) -> dict | None:
        if self.geometry_current is None:
            return None
        else:
            return self.geometry_current.geometry

    @property
    def datetime(self) -> str | None:
        dts = self.get_geometry_datetimes()
        if len(dts) == 1:
            return dts[0]
        else:
            return None

    @property
    def start_datetime(self) -> str | None:
        dts = self.get_geometry_datetimes()
        if len(dts) > 1:
            return dts.min()
        else:
            return None

    @property
    def end_datetime(self) -> str | None:
        dts = self.get_geometry_datetimes()
        if len(dts) > 1:
            return dts.max()
        else:
            return None

    @property
    def plan_current(self) -> Asset | None:
        return self.assets[self.project.plan_current]

    @property
    def geometry_current(self) -> Asset | None:
        if self.plan_current is None:
            return None
        else:
            return self.assets[self.plan_current.primary_geometry]

    @property
    def plan_files(self) -> list[Asset]:
        return [self.assets[f] for f in self.project.plan_files]

    @property
    def geometry_files(self) -> list[Asset]:
        if self.project is None:
            return []
        else:
            return [self.assets[f] for f in self.project.geometry_files]

    @property
    def steady_flow_files(self) -> list[Asset]:
        if self.project is None:
            return []
        else:
            return [self.assets[f] for f in self.project.steady_flow_files]

    @property
    def quasi_unsteady_flow_files(self) -> list[Asset]:
        if self.project is None:
            return []
        else:
            return [self.assets[f] for f in self.project.quasi_unsteady_flow_files]

    @property
    def unsteady_flow_files(self) -> list[Asset]:
        if self.project is None:
            return []
        else:
            return [self.assets[f] for f in self.project.unsteady_flow_files]

    @property
    def project(self) -> Asset:
        return self._project

    def get_geometry_datetimes(self) -> list[str]:
        dts = []
        for i in self.geometry_files:
            dts.extend(i.get_datetimes())
        if len(dts) == 0:
            self._dt_source = "processing time"
            dts = [datetime.now()]
        else:
            self._dt_source = "model geometry"
        return dts
