import datetime
import os
from functools import wraps

from ras_stac.ras_stac1d.utils.common import file_location
from ras_stac.ras_stac1d.utils.s3_utils import str_from_s3
from ras_stac.utils.s3_utils import get_basic_object_metadata


def cache_data(func):
    """Check if asset already downloaded."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.file_str is None:
            self.download_asset_str()
        return func(self, *args, **kwargs)

    return wrapper


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


class SteadyFlowAsset(GenericAsset):

    def __init__(url: str):
        super().__init__(url)

    def add_extra_fields(self):
        super().add_extra_fields()
        # and some more


class GeometryAsset(GenericAsset):

    def __init__(url: str):
        super().__init__(url)

    def add_extra_fields(self):
        super().add_extra_fields()
        # and some more
