from __future__ import annotations

import json
from pathlib import Path
from typing import Union

from dbt_artifacts_parser.parser import (
    CatalogV1,
    ManifestV1,
    ManifestV2,
    ManifestV3,
    ManifestV4,
    ManifestV5,
    ManifestV6,
    ManifestV7,
    ManifestV8,
    ManifestV9,
    ManifestV10,
    ManifestV11,
    ManifestV12,
    parse_catalog,
    parse_manifest,
)

DbtCatalog = CatalogV1
DbtManifest = Union[
    ManifestV1,
    ManifestV2,
    ManifestV3,
    ManifestV4,
    ManifestV5,
    ManifestV6,
    ManifestV7,
    ManifestV8,
    ManifestV9,
    ManifestV10,
    ManifestV11,
    ManifestV12,
]


def load_catalog(path: Path | str) -> DbtCatalog:
    data = json.loads(Path(path).read_text())
    return parse_catalog(data)


def load_manifest(path: Path | str) -> DbtManifest:
    data = json.loads(Path(path).read_text())
    return parse_manifest(data)
