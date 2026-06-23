import os
import csv
import sys
import math
import textwrap
from datetime import datetime, timedelta

def safe_float(value):
    """
    Safely convert a value to float, returning None on failure.

    Parameters:
        value: Any value to convert.

    Returns:
        float or None
    """
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def load_csv_for_cluster(cluster_id, exit_on_missing=True):
    """
    Load the cached CSV for a cluster and return a list of job dicts.

    Looks for: <script_dir>/cluster_data/cluster_<cluster_id>_jobs.csv

    Parameters:
        cluster_id (str or int): The cluster ID to load.
        exit_on_missing (bool): If True, print an error and sys.exit(1) when
            the file is not found. If False, return None instead.

    Returns:
        list[dict] or None
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "cluster_data")
    filepath = os.path.join(data_dir, f"cluster_{cluster_id}_jobs.csv")

    if not os.path.exists(filepath):
        if exit_on_missing:
            print(
                f"Cluster data not found for cluster {cluster_id}. "
                "Please make sure you have the correct CSV file and cluster ID."
            )
            sys.exit(1)
        return None

    with open(filepath, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def format_seconds_human(seconds):
    """
    Format a duration in seconds as a compact human-readable string.

    Examples:
        45    -> "45s"
        130   -> "2m 10s"
        3700  -> "1h 1m 40s"
        90000 -> "1d 1h"

    Parameters:
        seconds (int or float): Duration in seconds.

    Returns:
        str
    """
    seconds = int(seconds)
    if seconds == 0:
        return "0s"
    parts = []
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds:
        parts.append(f"{seconds}s")
    return " ".join(parts)


def format_epoch_human_relative(epoch_seconds):
    """
    Format an epoch timestamp as a human-readable relative string.

    Examples:
        (30 seconds ago) -> "just now"
        (45 minutes ago) -> "45 minutes ago"
        (3 days ago)     -> "3 days ago"
        (older)          -> "2024-11-01"

    Parameters:
        epoch_seconds (int or float): Unix timestamp.

    Returns:
        str
    """
    try:
        event_time = datetime.fromtimestamp(int(epoch_seconds))
        now = datetime.now()
        delta = now - event_time

        if delta < timedelta(minutes=1):
            return "just now"
        elif delta < timedelta(hours=1):
            minutes = int(delta.total_seconds() // 60)
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        elif delta < timedelta(days=1):
            hours = int(delta.total_seconds() // 3600)
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        elif delta < timedelta(days=7):
            days = delta.days
            return f"{days} day{'s' if days != 1 else ''} ago"
        elif delta < timedelta(days=30):
            weeks = delta.days // 7
            return f"{weeks} week{'s' if weeks != 1 else ''} ago"
        else:
            return event_time.strftime("%Y-%m-%d")
    except Exception:
        return "N/A"


def readable_time(seconds, abbrev=False):
    """Takes seconds and produces a readable time, for example:
    2 seconds
    13 minutes
    5 hours
    1 day 6 hours"""

    if seconds < 60:  # seconds
        value = seconds
        unit = "second"
        if abbrev:
            unit = "s"
        elif value > 1:
            unit += "s"
        return f"{value} {unit}"

    elif seconds < 60*60:  # minutes
        value = round(seconds/60)
        unit = "minute"
        if abbrev:
            unit = "m"
        elif value > 1:
            unit += "s"
        return f"{value} {unit}"

    elif seconds < 60*60*24:  # hours
        value = round(seconds/(60*60))
        units = "hour"
        if abbrev:
            units = "h"
        elif value > 1:
            units += "s"
        return f"{value} {units}"

    else:  # days, including hours
        day_value = round(seconds/(60*60*24))
        day_unit = "day"
        hour_value = round((seconds % (60*60*24))/(60*60))
        hour_unit = "hour"
        if abbrev:
            day_unit = "d"
            hour_unit = "h"
        else:
            if day_value > 1:
                day_unit += "s"
            if hour_value > 1:
                hour_unit += "s"
        return f"{day_value} {day_unit} {hour_value} {hour_unit}"


def readable_size(bytes, decimals=3, abbrev=True):
    """Takes bytes and produces a readable size, for example:
    256 B
    1.456 MB
    34.983 GB"""

    kb = 2 ** 10
    mb = 2 ** 20
    if bytes < kb:
        value = bytes
        unit = "Bytes"
        if abbrev:
            unit = "B"

    elif bytes < mb:
        value = f"{round(bytes/kb, decimals):.3f}"
        unit = "Kilobytes"
        if abbrev:
            unit = "KB"

    elif bytes < mb * kb:
        value = f"{round(bytes/mb, decimals):.3f}"
        unit = "Megabytes"
        if abbrev:
            unit = "MB"

    elif bytes < mb * mb:
        value = f"{round(bytes/(mb * kb), decimals):.3f}"
        unit = "Gigabytes"
        if abbrev:
            unit = "GB"

    elif bytes < mb * kb * mb * kb:
        value = f"{round(bytes/(mb * mb), decimals):.3f}"
        unit = "Terabytes"
        if abbrev:
            unit = "TB"

    else:
        value = f"{round(bytes/10**15, decimals):.3f}"
        unit = "Petabytes"
        if abbrev:
            unit = "PB"

    return f"{value} {unit}"


def s(n):
    """Pluralizing function, returns "s" if n != 1, otherwise returns empty string"""
    if isinstance(n, int) and n == 1:
        return ""
    else:
        return "s"


# ── numpy replacements ────────────────────────────────────────────────────────

def np_percentile(data, p):
    """
    Compute the p-th percentile of a sorted or unsorted list.
    Equivalent to numpy.percentile(data, p) with linear interpolation.

    Parameters:
        data (list[float]): Input values.
        p (float or list[float]): Percentile(s) in [0, 100].

    Returns:
        float or list[float]
    """
    if not data:
        return 0.0 if isinstance(p, (int, float)) else [0.0] * len(p)

    sorted_data = sorted(data)
    n = len(sorted_data)

    def _single(pct):
        if pct <= 0:
            return float(sorted_data[0])
        if pct >= 100:
            return float(sorted_data[-1])
        idx = (pct / 100.0) * (n - 1)
        lo = int(idx)
        hi = lo + 1
        if hi >= n:
            return float(sorted_data[-1])
        frac = idx - lo
        return sorted_data[lo] + frac * (sorted_data[hi] - sorted_data[lo])

    if isinstance(p, (int, float)):
        return _single(p)
    return [_single(pct) for pct in p]


def np_linspace(start, stop, num):
    """
    Return `num` evenly spaced floats from `start` to `stop` inclusive.
    Equivalent to numpy.linspace(start, stop, num).

    Parameters:
        start (float): Start of interval.
        stop (float): End of interval.
        num (int): Number of points.

    Returns:
        list[float]
    """
    if num <= 0:
        return []
    if num == 1:
        return [float(start)]
    step = (stop - start) / (num - 1)
    return [start + step * i for i in range(num)]


def np_unique(data):
    """
    Return sorted unique values from data.
    Equivalent to numpy.unique(data).

    Parameters:
        data (list): Input values.

    Returns:
        list: Sorted unique values.
    """
    return sorted(set(data))


def np_histogram(data, bins):
    """
    Compute a histogram of data given explicit bin edges.
    Equivalent to numpy.histogram(data, bins=bin_edges).

    The last bin is closed on both sides [edge[-2], edge[-1]].
    All other bins are half-open [edge[i], edge[i+1]).

    Parameters:
        data (list[float]): Input values.
        bins (list[float]): Monotonically increasing bin edges (length N+1 for N bins).

    Returns:
        tuple(list[int], list[float]): (counts, bin_edges)
    """
    n_bins = len(bins) - 1
    counts = [0] * n_bins
    for val in data:
        for i in range(n_bins):
            if i < n_bins - 1:
                if bins[i] <= val < bins[i + 1]:
                    counts[i] += 1
                    break
            else:
                if bins[i] <= val <= bins[i + 1]:
                    counts[i] += 1
    return counts, list(bins)


def np_searchsorted(sorted_data, value):
    """
    Find the index where `value` would be inserted to keep `sorted_data` sorted.
    Returns the count of elements strictly less than `value` (side='right' equivalent
    for the CDF use case: counts elements <= value).

    Parameters:
        sorted_data (list[float]): Sorted list.
        value (float): Value to search for.

    Returns:
        int
    """
    lo, hi = 0, len(sorted_data)
    while lo < hi:
        mid = (lo + hi) // 2
        if sorted_data[mid] <= value:
            lo = mid + 1
        else:
            hi = mid
    return lo


# ── pandas DataFrame replacement ─────────────────────────────────────────────

class DataFrame:
    """
    Minimal pandas-compatible DataFrame backed by a list of dicts.
    Supports the operations used in Histogram and Analytics:
      - column access df["col"]
      - df.columns
      - pd.to_numeric equivalent via DataFrame.to_numeric(df, col)
      - Series.dropna(), .empty, .values, .min(), .max()
      - Series.mean(), .median(), .std()
      - Series comparisons returning boolean masks
      - df.loc[mask, cols]
      - Series.fillna(val)
      - pd.notna(val)
    """

    def __init__(self, records):
        """
        Parameters:
            records (list[dict]): List of row dicts (from load_csv_for_cluster).
        """
        self._records = list(records)
        # Collect all column names preserving insertion order
        seen = {}
        for row in self._records:
            for k in row:
                seen[k] = None
        self._columns = list(seen.keys())

    @property
    def columns(self):
        return self._columns

    def __contains__(self, col):
        return col in self._columns

    def __getitem__(self, key):
        """Return a Series for a column name, or a new DataFrame for a list of columns."""
        if isinstance(key, str):
            return Series([row.get(key) for row in self._records], name=key)
        elif isinstance(key, list):
            return DataFrame([{c: row.get(c) for c in key} for row in self._records])
        elif isinstance(key, Series):
            # Boolean mask indexing
            mask = key._data
            return DataFrame([row for row, keep in zip(self._records, mask) if keep])
        raise TypeError(f"Unsupported key type: {type(key)}")

    def __setitem__(self, col, series_or_list):
        """Assign a column from a Series or list."""
        if isinstance(series_or_list, Series):
            values = series_or_list._data
        else:
            values = list(series_or_list)
        if col not in self._columns:
            self._columns.append(col)
        for i, row in enumerate(self._records):
            row[col] = values[i] if i < len(values) else None

    def loc(self, mask, cols):
        """
        Subset rows by boolean mask and select columns.

        Parameters:
            mask (Series): Boolean Series.
            cols (list[str]): Column names to keep.

        Returns:
            DataFrame
        """
        selected = [row for row, keep in zip(self._records, mask._data) if keep]
        return DataFrame([{c: row.get(c) for c in cols} for row in selected])

    def dropna(self):
        """Return a new DataFrame with rows that have no None values."""
        return DataFrame([
            row for row in self._records
            if all(v is not None for v in row.values())
        ])

    @staticmethod
    def to_numeric(df, col):
        """
        Convert a column in-place to float, setting unparseable values to None.
        Mirrors pd.to_numeric(df[col], errors='coerce').
        """
        for row in df._records:
            val = row.get(col)
            if val is None or val == "":
                row[col] = None
            else:
                try:
                    row[col] = float(val)
                except (ValueError, TypeError):
                    row[col] = None


class Series:
    """
    Minimal pandas-compatible Series backed by a plain Python list.
    """

    def __init__(self, data, name=None):
        self._data = list(data)
        self.name = name

    # ── Basic properties ───────────────────────────────────────────────────────

    @property
    def empty(self):
        return len(self._data) == 0 or all(v is None for v in self._data)

    @property
    def values(self):
        """Return the underlying list (analogous to np.ndarray)."""
        return [v for v in self._data if v is not None]

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    # ── Filtering ─────────────────────────────────────────────────────────────

    def dropna(self):
        """Return a new Series with None values removed."""
        return Series([v for v in self._data if v is not None], name=self.name)

    def fillna(self, value):
        """Return a new Series with None replaced by `value`."""
        return Series([v if v is not None else value for v in self._data], name=self.name)

    # ── Comparison operators (return boolean Series) ───────────────────────────

    def __gt__(self, other):
        return Series([v > other if v is not None else False for v in self._data])

    def __lt__(self, other):
        return Series([v < other if v is not None else False for v in self._data])

    def __ge__(self, other):
        return Series([v >= other if v is not None else False for v in self._data])

    def __le__(self, other):
        return Series([v <= other if v is not None else False for v in self._data])

    def __and__(self, other):
        return Series([a and b for a, b in zip(self._data, other._data)])

    # ── Arithmetic ─────────────────────────────────────────────────────────────

    def __sub__(self, other):
        if isinstance(other, Series):
            return Series([
                (a - b) if a is not None and b is not None else None
                for a, b in zip(self._data, other._data)
            ])
        return Series([v - other if v is not None else None for v in self._data])

    def __mul__(self, scalar):
        return Series([v * scalar if v is not None else None for v in self._data])

    def __truediv__(self, scalar):
        return Series([v / scalar if v is not None else None for v in self._data])

    def mean(self):
        vals = [v for v in self._data if v is not None]
        return sum(vals) / len(vals) if vals else 0.0

    def median(self):
        vals = sorted(v for v in self._data if v is not None)
        n = len(vals)
        if n == 0:
            return 0.0
        mid = n // 2
        return (vals[mid - 1] + vals[mid]) / 2.0 if n % 2 == 0 else float(vals[mid])

    def std(self):
        vals = [v for v in self._data if v is not None]
        n = len(vals)
        if n < 2:
            return 0.0
        mean = sum(vals) / n
        variance = sum((x - mean) ** 2 for x in vals) / (n - 1)
        return math.sqrt(variance)

    def min(self):
        vals = [v for v in self._data if v is not None]
        return min(vals) if vals else None

    def max(self):
        vals = [v for v in self._data if v is not None]
        return max(vals) if vals else None

    def sum(self):
        vals = [v for v in self._data if v is not None]
        return sum(vals)


def notna(value):
    """
    Return True if value is not None and not NaN.
    Equivalent to pandas.notna().
    """
    if value is None:
        return False
    try:
        return not math.isnan(float(value))
    except (TypeError, ValueError):
        return True


def make_dataframe(records, numeric_cols=None):
    """
    Convenience wrapper: build a DataFrame from a list of dicts and
    coerce specified columns to numeric in one step.

    Parameters:
        records (list[dict]): Raw job dicts from load_csv_for_cluster.
        numeric_cols (list[str] | None): Columns to coerce to float.

    Returns:
        DataFrame
    """
    df = DataFrame(records)
    for col in (numeric_cols or []):
        if col in df.columns:
            DataFrame.to_numeric(df, col)
    return df


# ── tabulate replacement ──────────────────────────────────────────────────────

def tabulate(rows, headers=None, tablefmt="grid", maxcolwidths=None):
    """
    Render a list-of-lists table as a plain-text grid string and print it.

    Supports tablefmt values: "grid" and "fancy_grid".
    Optionally wraps column content to maxcolwidths (list of ints or None per col).

    Parameters:
        rows (list[list]): Table data.
        headers (list[str] | None): Column header labels.
        tablefmt (str): "grid" (ASCII) or "fancy_grid" (Unicode box-drawing).
        maxcolwidths (list[int | None] | None): Max character width per column.
    """
    if tablefmt == "fancy_grid":
        h_line  = "═"
        v_line  = "║"
        tl = "╔"; tm = "╦"; tr = "╗"
        ml = "╠"; mm = "╬"; mr = "╣"
        bl = "╚"; bm = "╩"; br = "╝"
    else:  # "grid"
        h_line  = "-"
        v_line  = "|"
        tl = "+"; tm = "+"; tr = "+"
        ml = "+"; mm = "+"; mr = "+"
        bl = "+"; bm = "+"; br = "+"

    all_rows = []
    if headers:
        all_rows.append([str(h) for h in headers])
    for row in rows:
        all_rows.append([str(cell) if cell is not None else "" for cell in row])

    if not all_rows:
        print("")
        return

    n_cols = max(len(r) for r in all_rows)

    # Pad short rows
    for row in all_rows:
        while len(row) < n_cols:
            row.append("")

    # Strip ANSI codes for width calculation only
    import re
    _ansi = re.compile(r'\x1b\[[0-9;]*m')

    def visible_len(s):
        return len(_ansi.sub('', s))

    def pad_to(s, width):
        vl = visible_len(s)
        return s + ' ' * max(0, width - vl)

    # Apply maxcolwidths wrapping
    if maxcolwidths:
        wrapped_rows = []
        for row in all_rows:
            # Wrap each cell
            cells = []
            for i, cell in enumerate(row):
                max_w = maxcolwidths[i] if i < len(maxcolwidths) else None
                if max_w and visible_len(cell) > max_w:
                    lines = textwrap.wrap(_ansi.sub('', cell), max_w)
                    cells.append(lines if lines else [""])
                else:
                    cells.append([cell])
            # Expand multi-line cells into multiple sub-rows
            height = max(len(c) for c in cells)
            for li in range(height):
                sub = []
                for c_lines in cells:
                    sub.append(c_lines[li] if li < len(c_lines) else "")
                wrapped_rows.append(sub)
        all_rows = wrapped_rows

    # Compute column widths
    col_widths = [0] * n_cols
    for row in all_rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], visible_len(cell))

    def hbar(left, mid, right):
        return left + mid.join(h_line * (w + 2) for w in col_widths) + right

    lines = []
    lines.append(hbar(tl, tm, tr))

    for idx, row in enumerate(all_rows):
        cells = " " + (" " + v_line + " ").join(
            pad_to(cell, col_widths[i]) for i, cell in enumerate(row)
        ) + " "
        lines.append(v_line + cells + v_line)

        # Separator after header row, or between every row for fancy_grid
        if headers and idx == 0:
            lines.append(hbar(ml, mm, mr))
        elif tablefmt == "fancy_grid" and idx < len(all_rows) - 1:
            lines.append(hbar(ml, mm, mr))

    lines.append(hbar(bl, bm, br))
    print("\n".join(lines))