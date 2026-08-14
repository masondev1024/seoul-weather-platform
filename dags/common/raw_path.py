"""Shared run-scoped raw object key layout."""

from __future__ import annotations


def safe_raw_key_segment(value: str) -> str:
    """Return a stable R2 key segment without path separators."""
    return "".join(
        character if character.isalnum() or character in "._=-" else "_"
        for character in value
    )


def build_raw_run_prefix(
    *,
    raw_prefix: str,
    domain: str,
    source_id: str,
    load_date: str,
    run_id: str,
) -> str:
    """Build ``raw/<domain>/<source>/load_date=.../run_id=...``."""
    return (
        f"{raw_prefix.rstrip('/')}/{domain}/{source_id}/"
        f"load_date={load_date}/run_id={safe_raw_key_segment(run_id)}"
    )
