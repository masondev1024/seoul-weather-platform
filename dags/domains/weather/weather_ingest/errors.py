"""Deterministic Weather Bronze failures that Airflow must not retry."""


class WeatherBronzeDeterministicError(Exception):
    """Base class for failures that another task attempt cannot repair."""


class WeatherBronzeConfigurationError(WeatherBronzeDeterministicError, RuntimeError):
    """Invalid or missing Weather Bronze configuration."""


class WeatherSourceBusinessError(WeatherBronzeDeterministicError, RuntimeError):
    """KMA returned a non-success business result code."""


class WeatherSourceSchemaError(WeatherBronzeDeterministicError, RuntimeError):
    """KMA or landing metadata violated the expected schema."""


class WeatherRawIntegrityError(WeatherBronzeDeterministicError, RuntimeError):
    """A raw object does not match the hash recorded at landing time."""


class WeatherCompletenessError(WeatherBronzeDeterministicError, RuntimeError):
    """A Weather page set or Bronze materialization is incomplete."""


class WeatherInvalidWindowError(WeatherBronzeDeterministicError, ValueError):
    """A requested KMA issue/page window is structurally invalid."""


__all__ = [
    "WeatherBronzeConfigurationError",
    "WeatherBronzeDeterministicError",
    "WeatherCompletenessError",
    "WeatherInvalidWindowError",
    "WeatherRawIntegrityError",
    "WeatherSourceBusinessError",
    "WeatherSourceSchemaError",
]
