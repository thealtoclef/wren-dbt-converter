"""Get active connection from dbt profiles."""

from __future__ import annotations

from ..dbt.models import DbtConnection, DbtProfiles


def get_active_connection(
    profiles: DbtProfiles,
    profile_name: str | None,
    target: str | None,
) -> DbtConnection:
    """Return the active DbtConnection for the target."""
    if not profiles.profiles:
        raise ValueError("profiles is empty")

    name = profile_name or next(iter(profiles.profiles))
    profile = profiles.profiles.get(name)
    if profile is None:
        raise KeyError(f"Profile {name!r} not found")

    tgt = target or profile.target
    conn = profile.outputs.get(tgt)
    if conn is None:
        raise KeyError(f"Target {tgt!r} not found in profile {name!r}")

    return conn
