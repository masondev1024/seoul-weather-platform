from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from weather_quality.models import ContractError


CANONICAL_SEOUL_GRID_SCOPE = "seoul_kma_80"
CANONICAL_SEOUL_GRID_COUNT = 80
CANONICAL_SEOUL_GRID_REVISION = (
    "seoul_kma_80:ed99fc182211cacfb61409a34b85dee7db95742a1adbb7eb47d0624698370c7d"
)


@dataclass(frozen=True, slots=True, order=True)
class GridCell:
    grid_id: str
    nx: int
    ny: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.grid_id, str)
            or isinstance(self.nx, bool)
            or not isinstance(self.nx, int)
            or isinstance(self.ny, bool)
            or not isinstance(self.ny, int)
            or self.grid_id != f"kma_{self.nx}_{self.ny}"
        ):
            raise ContractError("canonical Seoul KMA grid universe contains an invalid cell")


def _revision(cells: tuple[GridCell, ...]) -> str:
    payload = json.dumps(
        [(cell.grid_id, cell.nx, cell.ny) for cell in cells], separators=(",", ":")
    ).encode()
    return f"{CANONICAL_SEOUL_GRID_SCOPE}:{hashlib.sha256(payload).hexdigest()}"


@dataclass(frozen=True, slots=True)
class GridUniverse:
    scope: str
    cells: tuple[GridCell, ...]
    population_revision: str

    def __post_init__(self) -> None:
        canonical_cells = tuple(sorted(self.cells))
        object.__setattr__(self, "cells", canonical_cells)
        grid_ids = tuple(cell.grid_id for cell in canonical_cells)
        valid = (
            self.scope == CANONICAL_SEOUL_GRID_SCOPE
            and len(canonical_cells) == CANONICAL_SEOUL_GRID_COUNT
            and len(set(grid_ids)) == CANONICAL_SEOUL_GRID_COUNT
            and _revision(canonical_cells) == CANONICAL_SEOUL_GRID_REVISION
            and self.population_revision == CANONICAL_SEOUL_GRID_REVISION
        )
        if not valid:
            raise ContractError(
                "grid universe must equal the versioned canonical Seoul KMA grid universe"
            )

    @property
    def grid_ids(self) -> tuple[str, ...]:
        return tuple(cell.grid_id for cell in self.cells)

    @property
    def expected_count(self) -> int:
        return len(self.cells)

    def as_evidence(self) -> dict[str, object]:
        return {
            "scope": self.scope,
            "expected_grid_count": self.expected_count,
            "observed_grid_count": self.expected_count,
            "population_revision": self.population_revision,
        }


def load_canonical_grid_universe(path: Path) -> GridUniverse:
    try:
        handle = path.open("r", encoding="utf-8-sig", newline="")
    except OSError as exc:
        raise ContractError(f"cannot read canonical grid CSV: {path}") from exc
    with handle:
        rows = tuple(csv.DictReader(handle))

    cells: list[GridCell] = []
    for row in rows:
        if row.get("coverage_scope") != "seoul_bbox":
            raise ContractError(
                "canonical Seoul KMA grid universe requires only seoul_bbox cells"
            )
        try:
            cell = GridCell(
                grid_id=str(row.get("place_id") or ""),
                nx=int(str(row.get("nx"))),
                ny=int(str(row.get("ny"))),
            )
        except ValueError as exc:
            raise ContractError(
                "canonical Seoul KMA grid universe coordinates must be integers"
            ) from exc
        cells.append(cell)

    return GridUniverse(
        scope=CANONICAL_SEOUL_GRID_SCOPE,
        cells=tuple(cells),
        population_revision=CANONICAL_SEOUL_GRID_REVISION,
    )
