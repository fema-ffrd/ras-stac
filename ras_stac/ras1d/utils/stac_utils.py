import re
from pathlib import Path

import pystac
from pystac import Asset
from pystac.extensions.storage import StorageExtension

from ras_stac.common.fileio import file_location
from ras_stac.common.s3_utils import get_metadata
from ras_stac.ras1d.utils.classes import (
    GeometryAsset,
    PlanAsset,
    ProjectAsset,
    QuasiUnsteadyFlowAsset,
    SteadyFlowAsset,
    UnsteadyFlowAsset,
)
from ras_stac.ras1d.utils.ras_utils import is_ras_prj


def asset_factory(url: str) -> Asset:
    url = str(url)
    file_extension = Path(url).suffix.lower()
    full_extension = url.rsplit("/")[-1].split(".", 1)[1]

    if file_extension == ".hdf":
        ras_extension = Path(url.replace(".hdf", "")).suffix
    else:
        ras_extension = file_extension
    ras_extension = ras_extension.lstrip(".")

    if ras_extension == "prj" and is_ras_prj(url):
        roles = ["project-file", "ras-file"]
        description = """The HEC-RAS project file."""
        asset = ProjectAsset(url, roles=roles, description=description)
    elif re.match("[Pp][0-9]{2}", ras_extension):
        roles = ["plan-file", "ras-file"]
        description = """The plan file which contains a list of associated input files and all simulation options."""
        asset = PlanAsset(url, roles=roles, description=description)
    elif re.match("[Gg][0-9]{2}", ras_extension):
        roles = ["geometry-file", "ras-file"]
        description = (
            "The geometry file which contains cross-sectional, hydraulic structures, and modeling approach data."
        )
        asset = GeometryAsset(url, roles=roles, description=description)
    elif re.match("[Ff][0-9]{2}", ras_extension):
        roles = ["steady-flow-file", "ras-file", pystac.MediaType.TEXT]
        description = """Steady Flow file which contains profile information, flow data, and boundary conditions."""
        asset = SteadyFlowAsset(url, roles=roles, description=description)
    elif re.match("[Qq][0-9]{2}", ras_extension):
        roles = ["quasi-unsteady-flow-file", "ras-file", pystac.MediaType.TEXT]
        description = """Quasi-Unsteady Flow file."""
        asset = QuasiUnsteadyFlowAsset(url, roles=roles, description=description)
    elif re.match("[Uu][0-9]{2}", ras_extension):
        roles = ["unsteady-file", "ras-file", pystac.MediaType.TEXT]
        description = """The unsteady file contains hydrographs amd initial conditions, as well as any flow options."""
        asset = UnsteadyFlowAsset(url, roles=roles, description=description)
    elif re.match("[Rr][0-9]{2}", ras_extension):
        roles = ["run-file", "ras-file", pystac.MediaType.TEXT]
        description = """Run file for steady flow analysis which contains all the necessary input data required for the RAS computational engine."""
        asset = Asset(url, roles=roles, description=description)
    elif re.match("hyd[0-9]{2}", ras_extension):
        roles = ["computational-level-output-file", "ras-file", pystac.MediaType.TEXT]
        description = """Detailed Computational Level output file."""
        asset = Asset(url, roles=roles, description=description)
    elif re.match("[Cc][0-9]{2}", ras_extension):
        roles = ["geometric-preprocessor-output-file", "ras-file", pystac.MediaType.TEXT]
        description = """Geomatric Pre-Processor output file. Contains the hydraulic properties tables, rating curves, and family of rating curves for each cross-section, bridge, culvert, storage area, inline and lateral structure."""
        asset = Asset(url, roles=roles, description=description)
    elif re.match("[Bb][0-9]{2}", ras_extension):
        roles = ["boundary-condition-file", "ras-file", pystac.MediaType.TEXT]
        description = """Boundary Condition file."""
        asset = Asset(url, roles=roles, description=description)
    elif re.match("bco[0-9]{2}", ras_extension):
        roles = ["unsteady-flow-log-file", "ras-file", pystac.MediaType.TEXT]
        description = """Unsteady Flow Log output file."""
        asset = Asset(url, roles=roles, description=description)
    elif re.match("[Ss][0-9]{2}", ras_extension):
        roles = ["sediment-data-file", "ras-file", pystac.MediaType.TEXT]
        description = """Sediment data file which contains flow data, boundary conditions, and sediment data."""
        asset = Asset(url, roles=roles, description=description)
    elif re.match("[Hh][0-9]{2}", ras_extension):
        roles = ["hydraulic-design-file", "ras-file", pystac.MediaType.TEXT]
        description = """Hydraulic Design data file."""
        asset = Asset(url, roles=roles, description=description)
    elif re.match("[Ww][0-9]{2}", ras_extension):
        roles = ["water-quality-file", "ras-file", pystac.MediaType.TEXT]
        description = """Water Quality data file which contains temperature boundary conditions, initial conditions, advection dispersion parameters and meteorological data."""
        asset = Asset(url, roles=roles, description=description)
    elif re.match("SedCap[0-9]{2}", ras_extension):
        roles = ["sediment-transport-capacity-file", "ras-file", pystac.MediaType.TEXT]
        description = """Sediment Transport Capacity data."""
        asset = Asset(url, roles=roles, description=description)
    elif re.match("SedXS[0-9]{2}", ras_extension):
        roles = ["xs-output-file", "ras-file", pystac.MediaType.TEXT]
        description = """Cross section output file."""
        asset = Asset(url, roles=roles, description=description)
    elif re.match("SedHeadXS[0-9]{2}", ras_extension):
        roles = ["xs-output-header-file", "ras-file", pystac.MediaType.TEXT]
        description = """Header file for the cross section output."""
        asset = Asset(url, roles=roles, description=description)
    elif re.match("wqrst[0-9]{2}", ras_extension):
        roles = ["water-quality-restart-file", "ras-file", pystac.MediaType.TEXT]
        description = """The water quality restart file."""
        asset = Asset(url, roles=roles, description=description)
    elif ras_extension == "sed":
        roles = ["sediment-output-file", "ras-file", pystac.MediaType.TEXT]
        description = """Detailed sediment output file."""
        asset = Asset(url, roles=roles, description=description)
    elif ras_extension == "blf":
        roles = ["binary-log-file", "ras-file", pystac.MediaType.TEXT]
        description = """Binary Log file."""
        asset = Asset(url, roles=roles, description=description)
    elif ras_extension == "dss":
        roles = ["ras-dss", "ras-file"]
        description = """The dss file contains the dss results and other simulation information."""
        asset = Asset(url, roles=roles, description=description)
    elif ras_extension == "log":
        roles = ["ras-log", "ras-file", pystac.MediaType.TEXT]
        description = """The log file contains the log information and other simulation information."""
        asset = Asset(url, roles=roles, description=description)
    elif ras_extension == "png":
        roles = ["thumbnail", pystac.MediaType.PNG]
        description = """PNG of geometry with OpenStreetMap basemap."""
        asset = Asset(url, roles=roles, description=description)
    elif ras_extension == "gpkg":
        roles = ["ras-geometry-gpkg", pystac.MediaType.GEOPACKAGE]
        description = """GeoPackage file with geometry data extracted from .gxx file."""
        asset = Asset(url, roles=roles, description=description)
    elif ras_extension == "rst":
        roles = ["restart-file", "ras-file", pystac.MediaType.TEXT]
        description = """Restart file."""
        asset = Asset(url, roles=roles, description=description)
    elif ras_extension == "SiamInput":
        roles = ["siam-input-file", "ras-file", pystac.MediaType.TEXT]
        description = """SIAM Input Data file."""
        asset = Asset(url, roles=roles, description=description)
    elif ras_extension == "SiamOutput":
        roles = ["siam-output-file", "ras-file", pystac.MediaType.TEXT]
        description = """SIAM Output Data file."""
        asset = Asset(url, roles=roles, description=description)
    elif re.match("bco[0-9]{2}", ras_extension):
        roles = ["water-quality-log", "ras-file", pystac.MediaType.TEXT]
        description = """Water quality log file."""
        asset = Asset(url, roles=roles, description=description)
    elif ras_extension == "color_scales":
        roles = ["color-scales", "ras-file", pystac.MediaType.TEXT]
        description = """File that contains the water quality color scale."""
        asset = Asset(url, roles=roles, description=description)
    elif full_extension == "comp_msgs.txt":
        roles = ["computational-message-file", "ras-file", pystac.MediaType.TEXT]
        description = """Computational Message text file which contains the computational messages that pop up in the computation window."""
        asset = Asset(url, roles=roles, description=description)
    elif re.match("[Xx][0-9]{2}", ras_extension):
        roles = ["run-file", "ras-file", pystac.MediaType.TEXT]
        description = """Run file for Unsteady Flow."""
        asset = Asset(url, roles=roles, description=description)
    elif re.match("[Oo][0-9]{2}", full_extension):
        roles = ["output-file", "ras-file", pystac.MediaType.TEXT]
        description = """Output file for ras which contains all of the computed results."""
        asset = Asset(url, roles=roles, description=description)
    elif re.match("IC.O[0-9]{2}", full_extension):
        roles = ["initial-conditions-file", "ras-file", pystac.MediaType.TEXT]
        description = """Initial conditions file for unsteady flow plan."""
        asset = Asset(url, roles=roles, description=description)
    elif re.match("[Pp][0-9]{2}.rst", full_extension):
        roles = ["restart-file", "ras-file", pystac.MediaType.TEXT]
        description = """Restart file."""
        asset = Asset(url, roles=roles, description=description)
    elif full_extension == "rasmap":
        roles = ["ras-mapper-file", "ras-file", pystac.MediaType.TEXT]
        description = """Ras Mapper file."""
        asset = Asset(url, roles=roles, description=description)
    elif full_extension == "rasmap.backup":
        roles = ["ras-mapper-file", "ras-file", pystac.MediaType.TEXT]
        description = """Backup Ras Mapper file."""
        asset = Asset(url, roles=roles, description=description)
    elif full_extension == "rasmap.original":
        roles = ["ras-mapper-file", "ras-file", pystac.MediaType.TEXT]
        description = """Original Ras Mapper file."""
        asset = Asset(url, roles=roles, description=description)
    elif file_extension == ".txt":
        roles = [pystac.MediaType.TEXT]
        description = """Miscellaneous text file."""
        asset = Asset(url, roles=roles, description=description)
    elif file_extension == ".xml":
        roles = [pystac.MediaType.XML]
        description = """Miscellaneous xml file."""
        asset = Asset(url, roles=roles, description=description)
    else:
        asset = Asset(url)
        asset.title = Path(url).name

    asset.title = Path(url).name
    asset = check_storage_extension(asset)
    return asset


def check_storage_extension(asset: Asset) -> Asset:
    """If the file is hosted on S3, add the storage extension."""
    if file_location(asset.href) == "s3":
        stor_ext = StorageExtension.ext(asset)
        meta = get_metadata(asset.href)
        stor_ext.apply(platform="AWS", region=meta["storage:region"], tier=meta["storage:tier"])
    return asset
