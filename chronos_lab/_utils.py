"""Internal utility functions for chronos-lab.

This module provides private helper functions used internally across the library.
These functions are not part of the public API and may change without notice.
"""

from typing import Optional
import pandas as pd


def _period(period: str, as_of: Optional[pd.Timestamp] = None) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Convert period string to date range tuple.

    Args:
        period: Period string (e.g., '7d', '4w', '3mo', '1y')
        as_of: Reference timestamp (defaults to current UTC time)

    Returns:
        Tuple of (start_datetime, end_datetime)

    Raises:
        ValueError: If period unit is invalid
    """
    end_dt = as_of if as_of is not None else pd.Timestamp.now(tz='UTC')

    value = int(period[:-1]) if period[-1].isalpha() else int(period[:-2])
    unit = period[-1] if period[-1].isalpha() else period[-2:]

    offset_map = {
        'd': pd.DateOffset(days=value),
        'w': pd.DateOffset(weeks=value),
        'mo': pd.DateOffset(months=value),
        'm': pd.DateOffset(months=value),
        'y': pd.DateOffset(years=value)
    }

    if unit not in offset_map:
        raise ValueError(f"Invalid period unit: {unit}. Use 'd', 'w', 'mo'/'m', or 'y'")

    start_dt = end_dt - offset_map[unit]
    return (start_dt, end_dt)

