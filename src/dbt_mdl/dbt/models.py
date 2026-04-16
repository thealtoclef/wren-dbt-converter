"""dbt-specific parsing models: connection, profile, and profiles container."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class DbtConnection(BaseModel):
    """A single dbt target connection.

    Only ``type`` is guaranteed across all adapters; everything else is
    adapter-specific and accessible via ``getattr(conn, field, None)``.
    """

    model_config = ConfigDict(extra="allow")

    type: str


class DbtProfile(BaseModel):
    target: str
    outputs: dict[str, DbtConnection]


class DbtProfiles(BaseModel):
    profiles: dict[str, DbtProfile]
