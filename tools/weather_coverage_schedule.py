"""Name one bounded, immutable Actions artifact per mature observation day."""
from __future__ import annotations

from datetime import datetime, timezone
import os

from tools.weather_coverage_comparison import DAY, DAYS, KST, cutoff_for


def artifact_name(rollout: datetime, now: datetime) -> str | None:
    if rollout.utcoffset() is None or now.utcoffset() is None or rollout > now:
        raise ValueError("invalid_rollout_time")
    first_day = rollout.astimezone(KST).date() + DAY
    final_cutoff = cutoff_for(first_day + (DAYS - 1) * DAY)
    if now < cutoff_for(first_day) or now >= final_cutoff + 7 * DAY:
        return None
    mature = sum(now >= cutoff_for(first_day + index * DAY) for index in range(DAYS))
    tag = rollout.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"weather-coverage-{tag}-{mature}of{DAYS}"


def main() -> None:
    name = artifact_name(datetime.fromisoformat(os.environ["ROLLOUT_AT"]), datetime.now(timezone.utc))
    if name:
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as outputs:
            print(f"name={name}", file=outputs)
            print(f"prefix={name.rsplit('-', 1)[0]}", file=outputs)


if __name__ == "__main__":
    main()
