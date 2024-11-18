from typing import (
    Generic,
    TypeVar,
)

import pystac
from pystac.extensions import item_assets

T = TypeVar("T", pystac.Item, pystac.Asset, item_assets.AssetDefinition)


class Ras1dExtension(Generic[T]):

    @classmethod
    def ext(cls, obj: T, add_if_missing: bool = False) -> Ras1dExtension[T]:
        if isinstance(obj, pystac.Item):
            cls.ensure_has_extension(obj, add_if_missing)
            return cast(ProjectionExtension[T], ItemProjectionExtension(obj))
        elif isinstance(obj, pystac.Asset):
            cls.ensure_owner_has_extension(obj, add_if_missing)
            return cast(ProjectionExtension[T], AssetProjectionExtension(obj))
        elif isinstance(obj, item_assets.AssetDefinition):
            cls.ensure_owner_has_extension(obj, add_if_missing)
            return cast(ProjectionExtension[T], ItemAssetsProjectionExtension(obj))
        else:
            raise pystac.ExtensionTypeError(cls._ext_error_message(obj))
