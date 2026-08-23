from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from math import isfinite
from typing import TypeAlias


ScalarValue: TypeAlias = float | str | bool
UTC = timezone.utc


class ContractError(ValueError):
    """Raised when a forecast-quality record violates the versioned contract."""


class TruthQuality(StrEnum):
    PROVISIONAL = "provisional"
    FINAL = "final"
    REJECTED = "rejected"


def _required_text(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{field} must be a non-empty string")
    return value.strip()


def _aware_utc(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ContractError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _validate_grid(grid_id: str, nx: int, ny: int) -> None:
    if (
        isinstance(nx, bool)
        or not isinstance(nx, int)
        or isinstance(ny, bool)
        or not isinstance(ny, int)
    ):
        raise ContractError("nx and ny must be integers")
    if grid_id != f"kma_{nx}_{ny}":
        raise ContractError("grid_id must equal kma_<nx>_<ny>")


def _validate_value(value: ScalarValue, value_kind: str, *, truth: bool) -> None:
    allowed = {"continuous", "binary", "categorical"} if truth else {
        "continuous",
        "probability",
        "categorical",
    }
    if value_kind not in allowed:
        raise ContractError(f"unsupported value_kind: {value_kind}")
    if value_kind in {"continuous", "probability"}:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(float(value)):
            raise ContractError(f"{value_kind} value must be a finite number")
    if value_kind == "probability" and not 0.0 <= float(value) <= 1.0:
        raise ContractError("probability value must be between 0 and 1")
    if value_kind == "binary" and not (
        isinstance(value, bool)
        or (isinstance(value, int) and not isinstance(value, bool) and value in (0, 1))
    ):
        raise ContractError("binary truth value must be 0 or 1")
    if value_kind == "categorical" and (not isinstance(value, str) or not value.strip()):
        raise ContractError("categorical value must be a non-empty string")


@dataclass(frozen=True, slots=True)
class ForecastVintage:
    product_family: str
    grid_id: str
    nx: int
    ny: int
    variable: str
    value_kind: str
    value: ScalarValue
    unit: str
    issued_at: datetime
    valid_at: datetime
    source_id: str
    source_revision: str

    def __post_init__(self) -> None:
        for field in (
            "product_family",
            "grid_id",
            "variable",
            "value_kind",
            "unit",
            "source_id",
            "source_revision",
        ):
            object.__setattr__(self, field, _required_text(getattr(self, field), field))
        _validate_grid(self.grid_id, self.nx, self.ny)
        _validate_value(self.value, self.value_kind, truth=False)
        object.__setattr__(self, "issued_at", _aware_utc(self.issued_at, "issued_at"))
        object.__setattr__(self, "valid_at", _aware_utc(self.valid_at, "valid_at"))
        if self.issued_at >= self.valid_at:
            raise ContractError("issued_at must be before valid_at")

    @property
    def identity(self) -> tuple[object, ...]:
        return (
            self.product_family,
            self.grid_id,
            self.variable,
            self.issued_at,
            self.valid_at,
        )


@dataclass(frozen=True, slots=True)
class ObservationTruth:
    grid_id: str
    nx: int
    ny: int
    variable: str
    value_kind: str
    value: ScalarValue
    unit: str
    observed_at: datetime
    truth_source: str
    truth_revision: str
    truth_as_of: datetime
    collected_at: datetime
    quality: TruthQuality

    def __post_init__(self) -> None:
        for field in (
            "grid_id",
            "variable",
            "value_kind",
            "unit",
            "truth_source",
            "truth_revision",
        ):
            object.__setattr__(self, field, _required_text(getattr(self, field), field))
        _validate_grid(self.grid_id, self.nx, self.ny)
        _validate_value(self.value, self.value_kind, truth=True)
        object.__setattr__(self, "observed_at", _aware_utc(self.observed_at, "observed_at"))
        object.__setattr__(self, "truth_as_of", _aware_utc(self.truth_as_of, "truth_as_of"))
        object.__setattr__(self, "collected_at", _aware_utc(self.collected_at, "collected_at"))
        if not isinstance(self.quality, TruthQuality):
            try:
                object.__setattr__(self, "quality", TruthQuality(self.quality))
            except ValueError as exc:
                raise ContractError(f"unsupported truth quality: {self.quality}") from exc
        if self.truth_as_of < self.observed_at:
            raise ContractError("truth_as_of cannot be before observed_at")
        if self.collected_at < self.observed_at:
            raise ContractError("collected_at cannot be before observed_at")
        if self.collected_at < self.truth_as_of:
            raise ContractError("collected_at cannot be before truth_as_of")

    @property
    def identity(self) -> tuple[object, ...]:
        return (
            self.truth_source,
            self.truth_revision,
            self.grid_id,
            self.variable,
            self.observed_at,
        )
