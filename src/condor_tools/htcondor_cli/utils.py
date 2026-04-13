import os
import csv
import sys
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
