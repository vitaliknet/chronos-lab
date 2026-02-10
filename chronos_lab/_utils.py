"""Internal utility functions for chronos-lab.

This module provides private helper functions used internally across the
library. These functions are not part of the public API and may change
without notice.
"""

from typing import Optional
import re
import pandas as pd

# Strict pattern: integer + single unit character
_PERIOD_RE = re.compile(r"^(?P<value>\d+)(?P<unit>[SMHdwm y])$".replace(" ", ""))


def _period(
        period: str,
        as_of: Optional[pd.Timestamp] = None,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Convert a period string into a start and end timestamp.

    The period must consist of a positive integer immediately followed by a
    single unit designator representing seconds, minutes, hours, days, weeks,
    months, or years. The start timestamp is calculated by subtracting the
    corresponding offset from the reference time.

    Args:
        period: Time interval string composed of an integer value and one unit
            specifier.
        as_of: Reference timestamp used as the end of the interval. If not
            provided, the current UTC time is used.

    Returns:
        A tuple of (start_timestamp, end_timestamp).

    Raises:
        ValueError: If the period format or unit is invalid.
    """
    end_dt = as_of if as_of is not None else pd.Timestamp.now(tz="UTC")

    match = _PERIOD_RE.match(period)
    if not match:
        raise ValueError(
            "Invalid period format. Expected '<int><unit>' with unit in "
            "{S, M, H, d, w, m, y}."
        )

    value = int(match.group("value"))
    unit = match.group("unit")

    offset_map = {
        "S": pd.DateOffset(seconds=value),
        "M": pd.DateOffset(minutes=value),
        "H": pd.DateOffset(hours=value),
        "d": pd.DateOffset(days=value),
        "w": pd.DateOffset(weeks=value),
        "m": pd.DateOffset(months=value),
        "y": pd.DateOffset(years=value),
    }

    start_dt = end_dt - offset_map[unit]
    return start_dt, end_dt
