import os
import sys
from types import SimpleNamespace
import htcondor2
from htcondor_cli.noun import Noun
from htcondor_cli.verb import Verb
import math
from collections import Counter
import statistics
from datetime import timedelta
from datetime import datetime
import time as time_module
from difflib import SequenceMatcher
from .utils import *

def _ensure_cluster_data(cluster_id, logger):
    """
    Ensure cluster_data/cluster_<id>_jobs.csv exists; fetch via HTCondor if not.

    Mirrors main.ensure_cluster_data() but routes messages through `logger`
    so it fits the htcondor_cli conventions.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "cluster_data")
    filepath = os.path.join(data_dir, f"cluster_{cluster_id}_jobs.csv")

    if os.path.exists(filepath):
        return

    logger.info(
        f"No cached data found for cluster {cluster_id}. "
        f"Fetching from HTCondor now (this may take a moment)..."
    )

    try:
        from fetch_cluster_data import fetch_cluster_jobs, validate_cluster_exists
    except ImportError:
        raise RuntimeError(
            "Module to fetch cluster data not found"
        )

    if not validate_cluster_exists(cluster_id):
        raise RuntimeError(
            f"No jobs found for cluster {cluster_id}. Verify the cluster ID "
            f"is correct, that you have permission to access it, and that it "
            f"exists in HTCondor history or queue."
        )

    fetch_cluster_jobs(cluster_id, output_dir=data_dir)

# ── Verbs ─────────────────────────────────────────────────────────────────────

class Dashboard(Verb):
    """
    Displays an ASCII job-status bar chart for a cluster
    """

    options = {
        "cluster_id": {
            "args": ("cluster_id",),
            "help": "HTCondor cluster ID to display",
        },
    }

    # get data from the schedd
    def fetch_counts(clusterId, job_states):
        schedd = htcondor2.Schedd()
        counts = {state: 0 for state in job_states}

        print("Fetching job history (this may take a moment)...", file=sys.stderr)

        total_found = 0

        for i, ad in enumerate(
            schedd.history(
                constraint=f"ClusterId == {clusterId}",
                projection=["JobStatus"],
                match=-1,
            )
        ):
            total_found += 1
            counts[job_states[ad.eval("JobStatus") - 1]] += 1

            if total_found % 1000 == 0:
                print(f"  Found {total_found} matching jobs...", file=sys.stderr)

        print(f"  Completed: {total_found} matching jobs found", file=sys.stderr)

        print("Fetching current queue...", file=sys.stderr)
        for ad in schedd.query(
            constraint=f"ClusterId == {clusterId}",
            projection=["JobStatus"],
            limit=-1,
        ):
            counts[job_states[ad.eval("JobStatus") - 1]] += 1
        print("Done fetching data\n", file=sys.stderr)

        return counts


    # print the dashboard
    def draw_bars(counts, job_states, bar_width=50):
        max_label_len = max(len(s) for s in job_states)
        max_count = max(counts.values()) or 1
        count_width = len(str(max_count))
        per_width = len("100.0%")
        total_count = sum(counts.values())

        if total_count == 0:
            print("No jobs in the cluster found, please recheck clusterId")
            exit()

        header = (
            f"{'Status'.rjust(max_label_len)} | "
            f"{'Bar'.ljust(bar_width)} | "
            f"{'Count'.rjust(count_width)} | "
            f"{'%'.rjust(per_width)}"
        )
        print(header)
        print("-" * len(header))

        for state in job_states:
            cnt = counts[state]
            length = math.ceil(int(cnt / total_count * bar_width))
            bar = "█" * length
            per = cnt * 100 / total_count

            state_str = state.rjust(max_label_len)
            bar_str = bar.ljust(bar_width)
            cnt_str = str(cnt).rjust(count_width)
            per_str = f"{per:5.1f}%".rjust(per_width)

            print(f"{state_str} | {bar_str} | {cnt_str} | {per_str}")


    def get_dashboard_data(clusterId):
        """
        Return job status counts as a dictionary for use by cluster_health.py.
        Does not print anything, just returns computed metrics.
        """
        job_states = [
            "Idle", "Running", "Removing", "Completed",
            "Held", "Transferring Output", "Suspended",
        ]

        try:
            counts = Dashboard.fetch_counts(clusterId, job_states)
            total = sum(counts.values())

            if total == 0:
                return None

            return {
                "total_jobs": total,
                "status_counts": counts,
                "completed": counts.get("Completed", 0),
                "held": counts.get("Held", 0),
                "running": counts.get("Running", 0),
                "idle": counts.get("Idle", 0),
                "held_pct": (counts.get("Held", 0) / total) * 100 if total > 0 else 0,
                "completed_pct": (counts.get("Completed", 0) / total) * 100 if total > 0 else 0,
            }
        except Exception:
            return None


    def run(args):
        """Entry point called by main.py."""
        job_states = [
            "Idle", "Running", "Removing", "Completed",
            "Held", "Transferring Output", "Suspended",
        ]
        counts = Dashboard.fetch_counts(args.cluster_id, job_states)
        print(f"\nCluster {args.cluster_id} Status Dashboard\n")
        Dashboard.draw_bars(counts, job_states)


    def __init__(self, logger, cluster_id, **options):
        Dashboard.run(SimpleNamespace(cluster_id=cluster_id))

class Histogram(Verb):
    """
    Plots runtime distribution (CDF and/or percentile histogram) for a cluster
    """

    options = {
        "cluster_id": {
            "args": ("cluster_id",),
            "help": "HTCondor cluster ID to analyse",
        },
        "show": {
            "args": ("--show",),
            "choices": ["cdf", "histogram", "both"],
            "default": "both",
            "help": "Which graph to display (default: both)",
        },
        "print_list": {
            "args": ("--print-list",),
            "action": "store_true",
            "default": False,
            "help": "Print job IDs with runtime < 10 minutes",
        },
        "percentiles": {
            "args": ("--percentiles",),
            "type": int,
            "default": 10,
            "help": "Number of percentile bins for the histogram (default: 10)",
        },
    }


    """
    This program takes data from the cluster_data folder and gives an ASCII histogram
    and ASCII CDF of the runtimes for a cluster.
    """


    # ── Data loading ───────────────────────────────────────────────────────────────

    def load_data_for_cluster(cluster_id):
        """Load cluster CSV and return a cleaned DataFrame."""
        jobs = load_csv_for_cluster(cluster_id)
        return make_dataframe(jobs, numeric_cols=["RemoteWallClockTime", "QDate", "CompletionDate"])


    def get_positive_runtimes(df):
        """Return positive runtime Series, or None if missing/empty."""
        if "RemoteWallClockTime" not in df.columns:
            print("[WARN] Missing RemoteWallClockTime column.")
            return None

        rt = df["RemoteWallClockTime"].dropna()
        rt = rt[rt > 0]

        if rt.empty:
            print("[WARN] No valid runtime data found.")
            return None

        return rt

    def cumulative_distribution(cluster_id, df, height=20, width=60):
        """
        Print an ASCII CDF line plot of job runtimes.

        X-axis: runtime over full observed range.
        Y-axis: cumulative % of jobs.
        Reads as: "X% of jobs completed within Y time."
        """
        rt = Histogram.get_positive_runtimes(df)
        if rt is None:
            return

        runtimes = sorted(rt.values)
        n = len(runtimes)

        marker_pcts = [25, 50, 75, 90, 95]
        marker_times = {p: float(np_percentile(runtimes, p)) for p in marker_pcts}

        x_max = float(runtimes[-1])
        plot = [[" " for _ in range(width)] for _ in range(height)]

        prev_y = height - 1
        for x in range(width):
            rt_at_x = (x / (width - 1)) * x_max if width > 1 else x_max
            cum_pct = float(np_searchsorted(runtimes, rt_at_x)) / n
            y = int((1.0 - cum_pct) * (height - 1))
            y = max(0, min(height - 1, y))

            y_lo, y_hi = min(y, prev_y), max(y, prev_y)
            for fy in range(y_lo, y_hi + 1):
                plot[fy][x] = "·"
            plot[y][x] = "•"
            prev_y = y

        # Percentile guideline rows.
        # Multiple percentiles may map to the same row, so store a list.
        marker_rows = {}
        for p, t in marker_times.items():
            row = int((1.0 - p / 100.0) * (height - 1))
            row = max(0, min(height - 1, row))
            marker_rows.setdefault(row, []).append((p, t))

            for x in range(width):
                if plot[row][x] == " ":
                    plot[row][x] = "╌"

        CYAN = "\033[96m"
        YELLOW = "\033[93m"
        RESET = "\033[0m"

        label_w = 5

        print(f"  {'Cumulative Distribution':^{width}}")
        print(f"{'':>{label_w}}  {'-' * width}")

        for i, row in enumerate(plot):
            pct_val = round(100 * (1.0 - i / (height - 1))) if height > 1 else 100
            label = f"{pct_val}%" if pct_val in (0, 25, 50, 75, 100) else ""

            annotation = ""
            if i in marker_rows:
                parts = [
                    f"{format_seconds_human(t)} (P{p})"
                    for p, t in sorted(marker_rows[i], key=lambda x: x[0])
                ]
                annotation = "  " + " | ".join(parts)

            coloured = []
            for cell in row:
                if cell in ("•", "·"):
                    coloured.append(f"{CYAN}{cell}{RESET}")
                elif cell == "╌":
                    coloured.append(f"{YELLOW}{cell}{RESET}")
                else:
                    coloured.append(cell)

            print(f"{label:>{label_w}} |{''.join(coloured)}{annotation}")

        print(f"{'':>{label_w}} +{'-' * width}")

        col = max(1, width // 4)
        ticks = (
            f"{'0s':<{col}}"
            f"{format_seconds_human(x_max * 0.25):<{col}}"
            f"{format_seconds_human(x_max * 0.50):<{col}}"
            f"{format_seconds_human(x_max * 0.75):<{max(1, col - 1)}}"
            f"{format_seconds_human(x_max):>{col}}"
        )
        print(f"{'':>{label_w + 1}} {ticks}")
        print(f"{'':>{label_w + 1}} {'Runtime':^{width}}")

        print(
            f"\n  Key:  {CYAN}•{RESET} curve point   "
            f"{CYAN}·{RESET} connector   "
            f"{YELLOW}╌{RESET} percentile guideline"
        )
        print()


    # ── Histogram ──────────────────────────────────────────────────────────────────

    def histogram(cluster_id, df, percentiles=10, max_width=20, show_fast_jobs=False):
        """
        Print histogram where bins are percentile ranges.
        Bars are red if the bin median runtime is < 10 minutes.
        Optionally print exact job IDs whose runtime is < 10 minutes.
        """
        rt = Histogram.get_positive_runtimes(df)
        if rt is None:
            return

        runtimes = rt.values
        percentiles_list = np_linspace(0, 100, percentiles + 1)
        raw_edges = np_percentile(runtimes, percentiles_list)
        bin_edges = np_unique(raw_edges)

        if len(bin_edges) < 2:
            print("[WARN] Not enough unique runtime values to build histogram.")
            return

        counts, _ = np_histogram(runtimes, bins=bin_edges)
        max_count = max(counts) if len(counts) > 0 else 0

        print(f"\n  {'Histogram by Percentile':^80}")
        print(f"  {'-' * 80}")
        print()

        pct_width = 15
        label_width = 30
        count_width = 7
        RED = "\033[91m"
        RESET = "\033[0m"

        header = (
            f"{'Percentile':<{pct_width}}"
            f"{'Time Range':<{label_width}}"
            f"| {'Histogram':<{max_width}}"
            f" {'# Jobs':>{count_width}}"
        )
        print(header)
        print("-" * len(header))

        jobs_in_red_bins = 0

        for i in range(len(counts)):
            left = bin_edges[i]
            right = bin_edges[i + 1]

            mask = (
                (rt >= left) & (rt <= right)
                if i == len(counts) - 1
                else (rt >= left) & (rt < right)
            )

            in_bin = rt[mask]
            median_time = in_bin.median() if not in_bin.empty else 0
            is_red = median_time < 600

            if is_red:
                jobs_in_red_bins += len(in_bin)

            color = RED if is_red else ""
            time_range = (
                f"{format_seconds_human(left):>10} - {format_seconds_human(right):>10}"
            ).rjust(label_width)

            # Because duplicate percentile edges may have been collapsed by np.unique,
            # compute an approximate percentile label from the actual edge positions.
            left_pct = int(round(100 * i / len(counts)))
            right_pct = int(round(100 * (i + 1) / len(counts)))
            pct_range = f"{left_pct:02}–{right_pct:02}%".ljust(pct_width)

            bar_len = int((counts[i] / max_count) * max_width) if max_count > 0 else 0
            bar = "█" * bar_len

            print(
                f"{pct_range}{time_range} | "
                f"{color}{bar:<{max_width}}{RESET} {counts[i]:>{count_width}}"
            )

        print(f"\n{RED}Note:{RESET} Bars in red represent bins with median runtime < 10 minutes.")
        print(f"{RED}Info:{RESET} Total number of jobs in such bins: {jobs_in_red_bins}")

        if show_fast_jobs:
            if "ClusterId" in df.columns and "ProcId" in df.columns:
                fast_mask = df["RemoteWallClockTime"].fillna(-1) < 600
                fast_jobs = df.loc(fast_mask, ["ClusterId", "ProcId"]).dropna()

                fast_job_ids = [
                    f"{int(r)}.{int(p)}" if notna(r) and notna(p) else f"{r}.{p}"
                    for r, p in zip(fast_jobs["ClusterId"], fast_jobs["ProcId"])
                ]

                if fast_job_ids:
                    print(f"\nJob IDs with runtime < 10 minutes:")
                    print(", ".join(fast_job_ids))
                else:
                    print(f"\nNo jobs with runtime < 10 minutes.")
            else:
                print("\n[WARN] Cannot print fast job IDs: missing ClusterId or ProcId column.")


    # ── Data-only interface for summarize.py ──────────────────────────────────────

    def get_histogram_data(cluster_id):
        """
        Return runtime analysis metrics as a dict for use by summarize.py.
        Does not print anything.
        """
        jobs = load_csv_for_cluster(cluster_id, exit_on_missing=False)
        if not jobs:
            return None

        df = make_dataframe(jobs, numeric_cols=["RemoteWallClockTime", "QDate", "CompletionDate"])

        if "RemoteWallClockTime" not in df.columns:
            return None

        rt = df["RemoteWallClockTime"].dropna()
        rt = rt[rt > 0]
        if rt.empty:
            return None

        runtimes = rt.values
        p95 = np_percentile(runtimes, 95)

        qdate_series = df["QDate"].dropna() if "QDate" in df.columns else Series([])
        completion_series = (
            df["CompletionDate"].dropna()
            if "CompletionDate" in df.columns else Series([])
        )

        return {
            "total_runtime_jobs": len(runtimes),
            "mean_runtime": rt.mean(),
            "median_runtime": rt.median(),
            "std_runtime": rt.std(),
            "cv": rt.std() / rt.mean() if rt.mean() > 0 else 0,
            "fast_jobs": int((runtimes < 600).sum()),
            "fast_jobs_pct": float(sum(1 for v in runtimes if v < 600) / len(runtimes) * 100),
            "long_jobs": int((runtimes > p95).sum()),
            "p95_runtime": p95,
            "min_runtime": rt.min(),
            "max_runtime": rt.max(),
            "first_submitted": qdate_series.min() if not qdate_series.empty else None,
            "last_completed": completion_series.max() if not completion_series.empty else None,
        }


    # ── Entry point ────────────────────────────────────────────────────────────────

    def run(args):
        """Entry point called by main.py."""
        df = Histogram.load_data_for_cluster(args.cluster_id)

        rt = Histogram.get_positive_runtimes(df)
        n = len(rt) if rt is not None else 0

        submit_times = df["QDate"].dropna() if "QDate" in df.columns else Series([])
        completion_times = (
            df["CompletionDate"].dropna()
            if "CompletionDate" in df.columns else Series([])
        )

        first_sub = (
            format_epoch_human_relative(submit_times.min())
            if not submit_times.empty else "N/A"
        )
        last_comp = (
            format_epoch_human_relative(completion_times.max())
            if not completion_times.empty else "N/A"
        )

        BOLD = "\033[1m"
        RESET = "\033[0m"

        print(f"\n{BOLD}{'Runtime Analysis':^80}{RESET}")
        print("=" * 80)
        print(
            f"  Cluster : {args.cluster_id}   |   Jobs: {n}   |   "
            f"Submitted: {first_sub}   |   Completed: {last_comp}"
        )
        print("=" * 80)

        show = getattr(args, "show", "both")
        if show in ("cdf", "both"):
            Histogram.cumulative_distribution(args.cluster_id, df, height=15, width=60)
        if show in ("histogram", "both"):
            Histogram.histogram(
                args.cluster_id,
                df,
                percentiles=args.percentiles,
                max_width=20,
                show_fast_jobs=args.print_list,
            )

    def __init__(self, logger, cluster_id, **options):
        _ensure_cluster_data(cluster_id, logger)
        Histogram.run(SimpleNamespace(
            cluster_id=cluster_id,
            show=options.get("show", "both"),
            print_list=options.get("print_list", False),
            percentiles=options.get("percentiles", 10),
        ))

class Analytics(Verb):
    """
    Produces a resource utilisation report (CPU / memory / disk)
    """

    options = {
        "cluster_id": {
            "args": ("cluster_id",),
            "help": "HTCondor cluster ID to analyse",
        },
    }

    # to print the bar visualizations
    def bar(pct, width=50):
        filled = int(pct / 100 * width)
        return "[" + "█" * filled + " " * (width - filled) + f"] {pct:.1f}%"

    # to calculate efficiency
    def efficiency(used, expected):
        if not expected:
            return 0.0
        return (used / expected) * 100

    # to calculate waste
    def calculate_waste(requested, used):
        if not requested:
            return 0.0
        return max(0, requested - used)

    # to get percentile value
    def percentile(data, p):
        if not data:
            return 0
        sorted_data = sorted(data)
        k = (len(sorted_data) - 1) * p / 100
        f = int(k)
        c = f + 1
        if c >= len(sorted_data):
            return sorted_data[-1]
        return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])

    # to print the usage report
    def compute_usage_summary(data, label, percentage=False, unit=None):
        if not data or len(data) < 2:
            return f"{label:<25}: Not enough data"

        data_sorted = sorted(data)
        min_val = data_sorted[0]
        q1 = statistics.quantiles(data_sorted, n=4)[0]
        median = statistics.median(data_sorted)
        q3 = statistics.quantiles(data_sorted, n=4)[2]
        max_val = data_sorted[-1]
        std_dev = statistics.stdev(data_sorted)
        
        fmt = "{:.1f}%" if percentage else "{:.1f}"
        return (
            f"{label:<25}: "
            f"{fmt.format(min_val):>6}  {fmt.format(q1):>6}  {fmt.format(median):>7}  "
            f"{fmt.format(q3):>6}  {fmt.format(max_val):>6}   {fmt.format(std_dev):>6}"
        )

    # prints the resource request table
    def print_resource_table(name, values, unit=""):
        if not values:
            print(f"{name:<15}: No data")
            return

        counts = Counter(values)
        print(f"{name:<15}:")
        for val, count in sorted(counts.items()):
            print(f"{'':<15}  {val:<10} {unit:<5}  {count} job(s)")
        print()

    # prints distribution of jobs by actual resource usage as a histogram
    def print_usage_distribution(name, used_list, unit="GiB"):
        if not used_list:
            return
        
        max_val = max(used_list)
        
        # Define bins based on the data range
        if max_val <= 10:
            bins = [0, 2, 5, 10, float('inf')]
            labels = ["0-2", "2-5", "5-10", "10+"]
        elif max_val <= 50:
            bins = [0, 5, 10, 20, 50, float('inf')]
            labels = ["0-5", "5-10", "10-20", "20-50", "50+"]
        else:
            bins = [0, 10, 25, 50, 100, float('inf')]
            labels = ["0-10", "10-25", "25-50", "50-100", "100+"]
        
        # Count jobs in each bin
        bin_counts = [0] * len(labels)
        for val in used_list:
            for i in range(len(bins) - 1):
                if bins[i] <= val < bins[i+1]:
                    bin_counts[i] += 1
                    break
        
        total_jobs = len(used_list)
        
        print(f"\n{name} Distribution:")
        
        # Find max count for scaling
        max_count = max(bin_counts) if bin_counts else 1
        bar_width = 50
        
        for i, (label, count) in enumerate(zip(labels, bin_counts)):
            if count > 0:
                pct = (count / total_jobs) * 100
                # Create histogram bar
                bar_length = int((count / max_count) * bar_width)
                bar_visual = "█" * bar_length
                print(f"  {label:>10} {unit}: {bar_visual:<{bar_width}} {count:>4} ({pct:>5.1f}%)")
            else:
                # Show empty bins if they exist
                print(f"  {label:>10} {unit}: {'':<{bar_width}} {count:>4} (  0.0%)")

    # print recommendations
    def print_recommendations(mem_req, mem_used, disk_req, disk_used, cpu_req, cpu_used_pct, avg_runtime_hours):
        print(f"\n{'Resource Optimization Recommendations':^80}")
        print("=" * 80)
        
        if mem_req and mem_used:
            p95_mem = Analytics.percentile(mem_used, 95)
            recommended_mem = p95_mem * 1.1  # 10% buffer
            median_mem_req = statistics.median(mem_req)
            
            if recommended_mem < median_mem_req * 0.8:  # If we can save >20%
                savings = (median_mem_req - recommended_mem) * len(mem_used) * avg_runtime_hours
                print(f"\n📊 Memory:")
                print(f"  Current Request     : {median_mem_req:.1f} GiB")
                print(f"  Recommended         : {recommended_mem:.1f} GiB (P95 + 10% buffer)")
                waste_per_job = median_mem_req - recommended_mem
                print(f"  Potential Savings   : {savings:.1f} GiB-hours")
                print(f"                        (≈ {waste_per_job:.1f} GiB/job × {len(mem_used)} jobs × {avg_runtime_hours:.1f} hr avg runtime)")
                print(f"  Jobs Affected       : {len(mem_used)}")
        
        if disk_req and disk_used:
            p95_disk = Analytics.percentile(disk_used, 95)
            recommended_disk = p95_disk * 1.2  # 20% buffer for disk
            median_disk_req = statistics.median(disk_req)
            
            if recommended_disk < median_disk_req * 0.8:
                savings = (median_disk_req - recommended_disk) * len(disk_used) * avg_runtime_hours
                print(f"\n💾 Disk:")
                print(f"  Current Request     : {median_disk_req:.1f} GiB")
                print(f"  Recommended         : {recommended_disk:.1f} GiB (P95 + 20% buffer)")
                waste_per_job = median_disk_req - recommended_disk
                print(f"  Potential Savings   : {savings:.1f} GiB-hours")
                print(f"                        (≈ {waste_per_job:.1f} GiB/job × {len(disk_used)} jobs × {avg_runtime_hours:.1f} hr avg runtime)")
                print(f"  Jobs Affected       : {len(disk_used)}")
        
        if cpu_req and cpu_used_pct:
            median_cpu_pct = statistics.median(cpu_used_pct)
            median_cpu_req = statistics.median(cpu_req)
            
            if median_cpu_pct < 50:
                # Calculate recommended CPUs based on actual usage
                # Use the median efficiency to scale down the request
                recommended_cpus = max(1, int(median_cpu_req * (median_cpu_pct / 100) * 1.2))  # 20% buffer
                
                print(f"\n⚙️  CPU:")
                print(f"  Current Request     : {median_cpu_req:.1f} CPUs")
                print(f"  Current Efficiency  : {median_cpu_pct:.1f}%")
                print(f"  Recommended         : {recommended_cpus} CPUs")
                print(f"  Jobs Affected       : {len(cpu_used_pct)}")

    # prints the total report
    def summarize(cluster_id):
        jobs = load_csv_for_cluster(cluster_id)

        mem_requested, mem_used = [], []
        disk_requested, disk_used = [], []
        run_time, cpu_used_time = [], []
        runtimes = []
        cpu_requests = []
        gpu_requests = []

        for job in jobs:
            mem_req = safe_float(job.get("RequestMemory"))
            mem_use = safe_float(job.get("ResidentSetSize_RAW"))
            if mem_req:
                mem_requested.append(round(mem_req / 1024, 2))  # Convert MiB to GiB
            if mem_use:
                mem_used.append(mem_use / 1024 / 1024)  # Convert KiB to GiB

            disk_req = safe_float(job.get("RequestDisk"))
            disk_use = safe_float(job.get("DiskUsage_RAW"))
            if disk_req:
                disk_requested.append(round(disk_req / (1024 * 1024), 2))  # Convert KiB to GiB
            if disk_use:
                disk_used.append(disk_use / (1024 * 1024))  # Convert KiB to GiB

            cpus = safe_float(job.get("RequestCpus"))
            if cpus:
                cpu_requests.append(int(cpus))

            gpus = safe_float(job.get("RequestGpus"))
            if gpus:
                gpu_requests.append(int(gpus))

            user_cpu = safe_float(job.get("RemoteUserCpu")) or 0
            sys_cpu = safe_float(job.get("RemoteSysCpu")) or 0
            wall_time = safe_float(job.get("RemoteWallClockTime"))

            if wall_time and cpus and (user_cpu or sys_cpu):
                total_cpu_used = sys_cpu / cpus
                cpu_used_time.append(total_cpu_used)
                run_time.append(wall_time)

            if wall_time:
                runtimes.append(wall_time)

        

        # Compute per-job efficiency lists
        per_job_cpu_eff = [
            Analytics.efficiency(cpu_used_time[i], run_time[i])
            for i in range(len(cpu_used_time))
            if run_time[i]
        ]

        per_job_mem_eff = [
            Analytics.efficiency(mem_used[i], mem_requested[i])
            for i in range(min(len(mem_used), len(mem_requested)))
            if mem_requested[i]
        ]

        per_job_disk_eff = [
            Analytics.efficiency(disk_used[i], disk_requested[i])
            for i in range(min(len(disk_used), len(disk_requested)))
            if disk_requested[i]
        ]

        # Take medians
        avg_cpu_eff = statistics.median(per_job_cpu_eff) if per_job_cpu_eff else 0
        avg_mem_eff = statistics.median(per_job_mem_eff) if per_job_mem_eff else 0
        avg_disk_eff = statistics.median(per_job_disk_eff) if per_job_disk_eff else 0

        
        total_jobs = len(jobs)
        avg_runtime = statistics.mean(runtimes) if runtimes else 0
        avg_runtime_str = str(timedelta(seconds=int(avg_runtime))) if avg_runtime else "N/A"
        avg_runtime_hours = avg_runtime / 3600 if avg_runtime else 1.0

        print("=" * 80)
        print(f"{'HTCondor Cluster Resource Summary':^80}")
        print("=" * 80)
        print(f"{'Cluster ID':>20}: {cluster_id}")
        print(f"{'Job Count':>20}: {total_jobs}")
        print(f"{'Avg Runtime':>20}: {avg_runtime_str}")
        print()

        print(f"{'Requested Resources':^80}")
        print("=" * 80)
        Analytics.print_resource_table("Memory (GiB)", mem_requested, "GiB")
        Analytics.print_resource_table("Disk (GiB)", disk_requested, "GiB")
        Analytics.print_resource_table("CPUs", cpu_requests, "")
        Analytics.print_resource_table("GPUs", gpu_requests, "")

        print(f"{'Number Summary Table':^80}")
        print("=" * 80)
        print(f"{'Resource (units)':<25}: {'Min':>6}  {'Q1':>6}  {'Median':>7}  {'Q3':>6}  {'Max':>6}   {'StdDev':>6}")
        print("-" * 80)

        cpu_usages, mem_values, disk_values = [], [], []

        for i in range(len(jobs)):
            if i < len(cpu_used_time) and i < len(run_time) and run_time[i]:
                cpu_usages.append(Analytics.efficiency(cpu_used_time[i], run_time[i]))
            if i < len(mem_used):
                mem_values.append(mem_used[i])
            if i < len(disk_used):
                disk_values.append(disk_used[i])


        print(Analytics.compute_usage_summary(mem_values, "Memory Used (GiB)"))
        print(Analytics.compute_usage_summary(disk_values, "Disk Used (GiB)"))
        print(Analytics.compute_usage_summary(cpu_usages, "CPU Usage (%)", percentage=True))
        

        print()

        print(f"{'Overall Utilization':^80}")
        print("=" * 80)
        print(f"  Memory usage      {Analytics.bar(avg_mem_eff)}")
        print(f"  Disk usage        {Analytics.bar(avg_disk_eff)}")
        print(f"  CPU usage         {Analytics.bar(avg_cpu_eff)}")
        print()

        # Usage distribution
        print(f"{'Resource Usage Distribution':^80}")
        print("=" * 80)
        Analytics.print_usage_distribution("Memory", mem_used, "GiB")
        Analytics.print_usage_distribution("Disk", disk_used, "GiB")
        
        # Recommendations
        Analytics.print_recommendations(mem_requested, mem_used, disk_requested, disk_used, 
                            cpu_requests, cpu_usages, avg_runtime_hours)

        # Gives human readable notes on the efficiency and also warnings
        print()
        print(f"{'Efficiency Summary':^80}")
        print("=" * 80)

        def warn(resource, efficiency):
            if efficiency < 15:
                print(f"  ⚠️  {resource} usage is {efficiency:.1f}% - significant over-provisioning")
            elif efficiency < 50:
                print(f"  ⚠️  {resource} usage is {efficiency:.1f}% - consider reducing requests")
            elif efficiency > 80:
                print(f"  ✅ {resource} usage is {efficiency:.1f}% - well optimized")
            else:
                print(f"  ✅ {resource} usage is {efficiency:.1f}%")

        warn("Memory", avg_mem_eff)
        warn("Disk", avg_disk_eff)
        warn("CPU", avg_cpu_eff)


        print()
        print(f"{'End of Summary':^80}")
        print("=" * 80)


    def get_analytics_data(cluster_id):
        """
        Return analytics data as a dictionary for use by cluster_health.py
        Does not print anything, just returns computed metrics.
        
        Returns:
            dict: Dictionary containing all analytics metrics
        """
        jobs = load_csv_for_cluster(cluster_id, exit_on_missing=False)
        if jobs is None:
            return None

        mem_requested, mem_used = [], []
        disk_requested, disk_used = [], []
        run_time, cpu_used_time = [], []
        runtimes = []
        cpu_requests = []
        gpu_requests = []

        for job in jobs:
            mem_req = safe_float(job.get("RequestMemory"))
            mem_use = safe_float(job.get("ResidentSetSize_RAW"))
            if mem_req:
                mem_requested.append(round(mem_req / 1024, 2))
            if mem_use:
                mem_used.append(mem_use / 1024 / 1024)

            disk_req = safe_float(job.get("RequestDisk"))
            disk_use = safe_float(job.get("DiskUsage_RAW"))
            if disk_req:
                disk_requested.append(round(disk_req / (1024 * 1024), 2))
            if disk_use:
                disk_used.append(disk_use / (1024 * 1024))

            cpus = safe_float(job.get("RequestCpus"))
            if cpus:
                cpu_requests.append(int(cpus))

            gpus = safe_float(job.get("RequestGpus"))
            if gpus:
                gpu_requests.append(int(gpus))

            user_cpu = safe_float(job.get("RemoteUserCpu")) or 0
            sys_cpu = safe_float(job.get("RemoteSysCpu")) or 0
            wall_time = safe_float(job.get("RemoteWallClockTime"))

            if wall_time and cpus and (user_cpu or sys_cpu):
                total_cpu_used = sys_cpu / cpus
                cpu_used_time.append(total_cpu_used)
                run_time.append(wall_time)

            if wall_time:
                runtimes.append(wall_time)

        # Compute per-job efficiency lists
        per_job_cpu_eff = [
            Analytics.efficiency(cpu_used_time[i], run_time[i])
            for i in range(len(cpu_used_time))
            if run_time[i]
        ]

        per_job_mem_eff = [
            Analytics.efficiency(mem_used[i], mem_requested[i])
            for i in range(min(len(mem_used), len(mem_requested)))
            if mem_requested[i]
        ]

        per_job_disk_eff = [
            Analytics.efficiency(disk_used[i], disk_requested[i])
            for i in range(min(len(disk_used), len(disk_requested)))
            if disk_requested[i]
        ]

        # Take medians
        avg_cpu_eff = statistics.median(per_job_cpu_eff) if per_job_cpu_eff else 0
        avg_mem_eff = statistics.median(per_job_mem_eff) if per_job_mem_eff else 0
        avg_disk_eff = statistics.median(per_job_disk_eff) if per_job_disk_eff else 0

        avg_runtime = statistics.mean(runtimes) if runtimes else 0
        avg_runtime_hours = avg_runtime / 3600 if avg_runtime else 1.0

        # Calculate savings
        savings = {}
        
        if mem_requested and mem_used:
            p95_mem = Analytics.percentile(mem_used, 95)
            recommended_mem = p95_mem * 1.1
            median_mem_req = statistics.median(mem_requested)
            
            if recommended_mem < median_mem_req * 0.8:
                mem_savings = (median_mem_req - recommended_mem) * len(mem_used) * avg_runtime_hours
                savings["memory"] = {
                    "current": median_mem_req,
                    "recommended": recommended_mem,
                    "savings_gib_hours": mem_savings,
                    "reduction_pct": ((median_mem_req - recommended_mem) / median_mem_req) * 100
                }
        
        if disk_requested and disk_used:
            p95_disk = Analytics.percentile(disk_used, 95)
            recommended_disk = p95_disk * 1.2
            median_disk_req = statistics.median(disk_requested)
            
            if recommended_disk < median_disk_req * 0.8:
                disk_savings = (median_disk_req - recommended_disk) * len(disk_used) * avg_runtime_hours
                savings["disk"] = {
                    "current": median_disk_req,
                    "recommended": recommended_disk,
                    "savings_gib_hours": disk_savings,
                    "reduction_pct": ((median_disk_req - recommended_disk) / median_disk_req) * 100
                }
        
        if cpu_requests and per_job_cpu_eff:
            median_cpu_pct = statistics.median(per_job_cpu_eff)
            median_cpu_req = statistics.median(cpu_requests)
            
            if median_cpu_pct < 50:
                recommended_cpus = max(1, int(median_cpu_req * (median_cpu_pct / 100) * 1.2))
                savings["cpu"] = {
                    "current": median_cpu_req,
                    "recommended": recommended_cpus,
                    "current_efficiency": median_cpu_pct,
                }

        return {
            "total_jobs": len(jobs),
            "avg_runtime": avg_runtime,
            "avg_runtime_hours": avg_runtime_hours,
            "memory_efficiency": avg_mem_eff,
            "disk_efficiency": avg_disk_eff,
            "cpu_efficiency": avg_cpu_eff,
            "memory_jobs": len(per_job_mem_eff),
            "disk_jobs": len(per_job_disk_eff),
            "cpu_jobs": len(per_job_cpu_eff),
            "mem_requested": mem_requested,
            "mem_used": mem_used,
            "disk_requested": disk_requested,
            "disk_used": disk_used,
            "cpu_requests": cpu_requests,
            "cpu_efficiency_list": per_job_cpu_eff,
            "savings": savings,
        }


    def __init__(self, logger, cluster_id, **options):
        _ensure_cluster_data(cluster_id, logger)
        Analytics.summarize(cluster_id)

class Hold(Verb):

    """
    Classifies and buckets held jobs by hold reason
    """

    options = {
        "cluster_id": {
            "args": ("cluster_id",),
            "help": "HTCondor cluster ID to analyse",
        },
        "min_count": {
            "args": ("--min-count",),
            "type": int,
            "default": 1,
            "help": "Only show buckets with at least N jobs (default: 1)",
        },
        "top": {
            "args": ("--top",),
            "type": int,
            "default": None,
            "help": "Show only the top N most common buckets",
        },
        "code": {
            "args": ("--code",),
            "type": int,
            "default": None,
            "help": "Filter to jobs with a specific HoldReasonCode",
        },
        "sort_by": {
            "args": ("--sort-by",),
            "choices": ["count", "code", "percent", "time"],
            "default": "count",
            "help": "Sort results by count, code, percent, or time (default: count)",
        },
        "threshold": {
            "args": ("--threshold",),
            "type": float,
            "default": 0.7,
            "help": "Similarity threshold (0.0-1.0) for grouping error messages (default: 0.7)",
        },
        "show_job_ids": {
            "args": ("--show-job-ids",),
            "action": "store_true",
            "default": False,
            "help": "Display ProcIds in the output table",
        },
        "export_jobs": {
            "args": ("--export-jobs",),
            "default": None,
            "help": "Export held job IDs to a CSV file for bulk operations",
        },
    }

    # Mapping of HoldReasonCodes to their explanations
    HOLD_REASON_CODES = {
        1: {"label": "UserRequest", "reason": "The user put the job on hold with condor_hold."},
        3: {"label": "JobPolicy", "reason": "The PERIODIC_HOLD expression evaluated to True. Or, ON_EXIT_HOLD was true."},
        4: {"label": "CorruptedCredential", "reason": "The credentials for the job are invalid."},
        5: {"label": "JobPolicyUndefined", "reason": "A job policy expression evaluated to Undefined."},
        6: {"label": "FailedToCreateProcess", "reason": "The condor_starter failed to start the executable."},
        7: {"label": "UnableToOpenOutput", "reason": "The standard output file for the job could not be opened."},
        8: {"label": "UnableToOpenInput", "reason": "The standard input file for the job could not be opened."},
        9: {"label": "UnableToOpenOutputStream", "reason": "The standard output stream for the job could not be opened."},
        10: {"label": "UnableToOpenInputStream", "reason": "The standard input stream for the job could not be opened."},
        11: {"label": "InvalidTransferAck", "reason": "An internal HTCondor protocol error was encountered when transferring files."},
        12: {"label": "TransferOutputError", "reason": "An error occurred while transferring job output files or self-checkpoint files."},
        13: {"label": "TransferInputError", "reason": "An error occurred while transferring job input files."},
        14: {"label": "IwdError", "reason": "The initial working directory of the job cannot be accessed."},
        15: {"label": "SubmittedOnHold", "reason": "The user requested the job be submitted on hold."},
        16: {"label": "SpoolingInput", "reason": "Input files are being spooled."},
        17: {"label": "JobShadowMismatch", "reason": "A standard universe job is not compatible with the condor_shadow version available on the submitting machine."},
        18: {"label": "InvalidTransferGoAhead", "reason": "An internal HTCondor protocol error was encountered when transferring files."},
        19: {"label": "HookPrepareJobFailure", "reason": "<Keyword>_HOOK_PREPARE_JOB was defined but could not be executed or returned failure."},
        20: {"label": "MissedDeferredExecutionTime", "reason": "The job missed its deferred execution time and therefore failed to run."},
        21: {"label": "StartdHeldJob", "reason": "The job was put on hold because WANT_HOLD in the machine policy was true."},
        22: {"label": "UnableToInitUserLog", "reason": "Unable to initialize job event log."},
        23: {"label": "FailedToAccessUserAccount", "reason": "Failed to access user account."},
        24: {"label": "NoCompatibleShadow", "reason": "No compatible shadow."},
        25: {"label": "InvalidCronSettings", "reason": "Invalid cron settings."},
        26: {"label": "SystemPolicy", "reason": "SYSTEM_PERIODIC_HOLD evaluated to true."},
        27: {"label": "SystemPolicyUndefined", "reason": "The system periodic job policy evaluated to undefined."},
        32: {"label": "MaxTransferInputSizeExceeded", "reason": "The maximum total input file transfer size was exceeded."},
        33: {"label": "MaxTransferOutputSizeExceeded", "reason": "The maximum total output file transfer size was exceeded."},
        34: {"label": "JobOutOfResources", "reason": "Memory usage exceeds a memory limit."},
        35: {"label": "InvalidDockerImage", "reason": "Specified Docker image was invalid."},
        36: {"label": "FailedToCheckpoint", "reason": "Job failed when sent the checkpoint signal it requested."},
        43: {"label": "PreScriptFailed", "reason": "Pre script failed."},
        44: {"label": "PostScriptFailed", "reason": "Post script failed."},
        45: {"label": "SingularityTestFailed", "reason": "Test of singularity runtime failed before launching a job"},
        46: {"label": "JobDurationExceeded", "reason": "The job's allowed duration was exceeded."},
        47: {"label": "JobExecuteExceeded", "reason": "The job's allowed execution time was exceeded."},
        48: {"label": "HookShadowPrepareJobFailure", "reason": "Prepare job shadow hook failed when it was executed; status code indicated job should be held."}
    }

    """
    Groups similar hold reason messages using fuzzy string matching (difflib.SequenceMatcher).

        Parameters:
            reason_list (List[Tuple[str, int, int]]): List of (reason, subcode, proc_id) tuples.
            threshold (float): Similarity ratio (between 0 and 1) above which reasons are considered similar.

        Returns:
            List[List[Tuple[str, int, int, int]]]: Buckets of (reason, subcode, proc_id, hold_time) tuples.
    """
    def bucket_reasons_with_data(reason_data, threshold=0.7):
        buckets = []
        for reason, subcode, proc_id, hold_time in reason_data:
            placed = False
            for bucket in buckets:
                ratio = SequenceMatcher(None, reason, bucket[0][0]).ratio()
                if ratio >= threshold:
                    bucket.append((reason, subcode, proc_id, hold_time))
                    placed = True
                    break
            if not placed:
                buckets.append([(reason, subcode, proc_id, hold_time)])
        return buckets



    def calculate_avg_hold_time(bucket):
        """Calculate average time jobs have been held in a bucket"""
        current_time = time_module.time()
        hold_durations = []
        
        for _, _, _, hold_time in bucket:
            if hold_time > 0:
                duration = current_time - hold_time
                hold_durations.append(duration)
        
        if not hold_durations:
            return None, "N/A"
        
        avg_seconds = sum(hold_durations) / len(hold_durations)
        return avg_seconds, format_seconds_human(avg_seconds)


    """ 
    Queries the HTCondor schedd for held jobs in the specified cluster and groups them by their HoldReasonCode.
    Now also collects ProcId and EnteredCurrentStatus (hold time).

        Parameters:
            cluster_id (str or int): The ID of the cluster to analyze.

        Returns:
            Dict[int, List[Tuple[str, int, int, int]]]: Maps HoldReasonCode to list of 
                                                        (HoldReason, HoldReasonSubCode, ProcId, HoldTime) tuples.
    """
    def group_by_code(cluster_id):
        schedd = htcondor2.Schedd()
        reasons_by_code = {}

        print("Fetching held jobs from cluster...", file=sys.stderr)
        
        for ad in schedd.query(
            constraint=f"ClusterId == {cluster_id} && JobStatus == 5",
            projection=["ProcId", "HoldReasonCode", "HoldReason", "HoldReasonSubCode", "EnteredCurrentStatus"],
            limit=-1
        ):
            code = ad.eval("HoldReasonCode")
            subcode = ad.eval("HoldReasonSubCode")
            proc_id = ad.eval("ProcId")
            hold_time = ad.get("EnteredCurrentStatus", 0)

            # Displaying only the first line of HoldReason, to bucket more efficiently
            reason = ad.eval("HoldReason").split('. ')[0]
            if "Error from" in reason and ": " in reason:
                parts = reason.split(": ", 1)
                if len(parts) == 2:
                    reason = parts[1]

            reasons_by_code.setdefault(code, []).append((reason, subcode, proc_id, hold_time))

        print(f"Found {sum(len(v) for v in reasons_by_code.values())} held jobs\n", file=sys.stderr)
        return reasons_by_code


    """
    Analyzes and prints time-based statistics for held jobs.

        Parameters:
            reasons_by_code (Dict): Dictionary grouping hold reasons by HoldReasonCode.
    """
    def print_time_analysis(reasons_by_code):
        all_times = []
        for pairs in reasons_by_code.values():
            all_times.extend([hold_time for _, _, _, hold_time in pairs if hold_time > 0])
        
        if not all_times:
            print("⏱️  Time Analysis: No timestamp data available\n")
            return
        
        earliest = min(all_times)
        latest = max(all_times)
        current_time = time_module.time()
        
        print("⏱️  Time Analysis:")
        print(f"  First held: {datetime.fromtimestamp(earliest).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Last held:  {datetime.fromtimestamp(latest).strftime('%Y-%m-%d %H:%M:%S')}")
        duration_hours = (latest - earliest) / 3600
        print(f"  Duration:   {duration_hours:.1f} hours")
        
        # Calculate overall average hold time
        avg_hold_duration = (current_time - sum(all_times) / len(all_times))
        print(f"  Avg hold:   {format_seconds_human(avg_hold_duration)}")
        
        print()


    """
    Export job IDs with hold reason codes to a CSV file for bulk operations.

        Parameters:
            all_buckets (List): All buckets with job data.
            reasons_by_code (Dict): Dictionary mapping codes to job data.
            cluster_id (str): The cluster ID.
            filename (str): Output filename.
    """
    def export_job_ids(all_buckets, reasons_by_code, cluster_id, filename):
        import csv
        
        # Build a mapping of proc_id to hold reason code
        proc_to_code = {}
        for code, pairs in reasons_by_code.items():
            for _, _, proc_id, _ in pairs:
                proc_to_code[proc_id] = code
        
        # Collect job IDs with their codes
        job_data = []
        seen_jobs = set()
        for bucket in all_buckets:
            for _, _, proc_id, _ in bucket:
                job_id = f"{cluster_id}.{proc_id}"
                if job_id not in seen_jobs:
                    seen_jobs.add(job_id)
                    hold_code = proc_to_code.get(proc_id, "Unknown")
                    hold_label = Hold.HOLD_REASON_CODES.get(hold_code, {}).get("label", f"Code {hold_code}")
                    job_data.append((job_id, hold_code, hold_label))
        
        # Sort by job ID for consistency
        job_data.sort(key=lambda x: (int(x[0].split('.')[0]), int(x[0].split('.')[1])))
        
        # Write to CSV
        with open(filename, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["JobID", "HoldReasonCode", "HoldReasonLabel"])
            writer.writerows(job_data)
        
        print(f"✓ Exported {len(job_data)} unique job IDs to {filename}\n")

    """ 
    Processes grouped hold reasons and prints a detailed table with filtering and sorting options.

        Parameters:
            reasons_by_code (Dict): Dictionary grouping hold reasons by HoldReasonCode.
            cluster_id (str or int): The cluster ID being analyzed.
            args: Parsed command line arguments.
    """
    def bucket_and_print_table(reasons_by_code, cluster_id, args):
        print(f"Cluster ID: {cluster_id}")

        held_jobs = sum(len(pairs) for pairs in reasons_by_code.values())
        print(f"Held Jobs in Cluster: {held_jobs}\n")
        
        # Time analysis
        Hold.print_time_analysis(reasons_by_code)

        example_rows = []
        all_buckets = []
        seen_codes = set()

        # Filter by specific code if requested
        if args.code:
            if args.code not in reasons_by_code:
                print(f"No held jobs found with HoldReasonCode {args.code}")
                return
            reasons_by_code = {args.code: reasons_by_code[args.code]}

        for code, pairs in reasons_by_code.items():
            label = Hold.HOLD_REASON_CODES.get(code, {}).get("label", f"Code {code}")
            seen_codes.add(code)
            buckets = Hold.bucket_reasons_with_data(pairs, threshold=args.threshold)
            
            for bucket in buckets:
                # Apply min-count filter
                if len(bucket) < args.min_count:
                    continue
                    
                all_buckets.append(bucket)
                example_reason, subcode, proc_id, hold_time = bucket[0]
                percent = (len(bucket) / held_jobs) * 100 if held_jobs > 0 else 0
                
                # Calculate average hold time for this bucket
                avg_hold_seconds, avg_hold_str = Hold.calculate_avg_hold_time(bucket)
                
                # Prepare job IDs string if requested
                job_ids_str = ""
                if args.show_job_ids:
                    if len(bucket) <= 5:
                        ids = [str(p) for _, _, p, _ in bucket]
                        job_ids_str = ", ".join(ids)
                    else:
                        ids = [str(p) for _, _, p, _ in bucket[:3]]
                        job_ids_str = f"{', '.join(ids)}... (+{len(bucket)-3} more)"
                
                row = [
                    label, 
                    subcode, 
                    f"{percent:.1f}% ({len(bucket)})", 
                    avg_hold_str,
                    example_reason
                ]
                if args.show_job_ids:
                    row.append(job_ids_str)
                
                # Store avg_hold_seconds for sorting
                row.append(avg_hold_seconds if avg_hold_seconds else 0)
                
                example_rows.append(row)

        # Sort results
        if args.sort_by == 'count':
            example_rows.sort(key=lambda x: int(x[2].split('(')[1].split(')')[0]), reverse=True)
        elif args.sort_by == 'code':
            example_rows.sort(key=lambda x: x[0])
        elif args.sort_by == 'percent':
            example_rows.sort(key=lambda x: float(x[2].split('%')[0]), reverse=True)
        elif args.sort_by == 'time':
            example_rows.sort(key=lambda x: x[-1], reverse=True)  # Sort by avg_hold_seconds
        
        # Remove the avg_hold_seconds column (used only for sorting)
        example_rows = [row[:-1] for row in example_rows]
        
        # Apply top N filter
        if args.top:
            example_rows = example_rows[:args.top]

        headers = ["Hold Reason Label", "SubCode", "% of Held Jobs (Count)", "Avg Hold Time", "Example Reason"]
        if args.show_job_ids:
            headers.append("Job IDs (ProcId)")
        
        print(tabulate(example_rows, headers=headers, tablefmt="grid"))

        print("\nLegend:")
        legend = []
        for code in sorted(seen_codes):
            entry = Hold.HOLD_REASON_CODES.get(code, {})
            legend.append([code, entry.get("label", "Unknown"), entry.get("reason", "No description available.")])
        print(tabulate(legend, headers=["Code", "Label", "Reason"], tablefmt="fancy_grid"))
        

        # Export job IDs if requested
        if args.export_jobs:
            Hold.export_job_ids(all_buckets, reasons_by_code, cluster_id, args.export_jobs)

    def get_hold_bucket_data(cluster_id, threshold=0.7):
        """
        Return held jobs analysis data as a dictionary for use by cluster_health.py
        Does not print anything, just returns computed metrics.
        
        Returns:
            dict: Dictionary containing held jobs analysis
        """
        try:
            reasons_by_code = Hold.group_by_code(cluster_id)
            
            if not reasons_by_code:
                return {
                    "held_count": 0,
                    "held_codes": {},
                    "held_reasons": {},
                    "unique_reasons": 0,
                    "buckets": [],
                }
            
            held_count = sum(len(pairs) for pairs in reasons_by_code.values())
            held_codes = {}
            all_buckets = []
            
            for code, pairs in reasons_by_code.items():
                held_codes[code] = len(pairs)
                buckets = Hold.bucket_reasons_with_data(pairs, threshold=threshold)
                all_buckets.extend(buckets)
            
            # Get top reasons
            held_reasons = {}
            for bucket in all_buckets:
                reason = bucket[0][0]  # Get example reason from first job
                held_reasons[reason] = len(bucket)
            
            # Time analysis
            all_times = []
            for pairs in reasons_by_code.values():
                all_times.extend([hold_time for _, _, _, hold_time in pairs if hold_time > 0])
            
            time_stats = {}
            if all_times:
                current_time = time_module.time()
                earliest = min(all_times)
                latest = max(all_times)
                avg_hold_duration = (current_time - sum(all_times) / len(all_times))
                
                time_stats = {
                    "first_held": earliest,
                    "last_held": latest,
                    "duration_hours": (latest - earliest) / 3600,
                    "avg_hold_duration": avg_hold_duration,
                }
            
            return {
                "held_count": held_count,
                "held_codes": held_codes,
                "held_reasons": held_reasons,
                "unique_reasons": len(held_reasons),
                "buckets": all_buckets,
                "time_stats": time_stats,
            }
        except Exception as e:
            return {
                "held_count": 0,
                "error": str(e)
            }

    def __init__(self, logger, cluster_id, **options):
        reasons_by_code = Hold.group_by_code(cluster_id)
        if not reasons_by_code:
            logger.info(f"No held jobs found in cluster {cluster_id}")
            return
        args = SimpleNamespace(
            cluster_id=cluster_id,
            min_count=options.get("min_count", 1),
            top=options.get("top"),
            code=options.get("code"),
            sort_by=options.get("sort_by", "count"),
            threshold=options.get("threshold", 0.7),
            show_job_ids=options.get("show_job_ids", False),
            export_jobs=options.get("export_jobs"),
        )
        Hold.bucket_and_print_table(reasons_by_code, cluster_id, args)

class Summarize(Verb):
    """
    This program provides a concise cluster health summary by aggregating
    data from all analysis tools and providing tool recommendations.
    """
    
    options = {
        "cluster_id": {
            "args": ("cluster_id",),
            "help": "HTCondor cluster ID to summarize",
        },
    }

    THRESHOLDS = {
        "memory_efficiency": {"critical": 15, "warning": 50, "good": 80},
        "disk_efficiency": {"critical": 15, "warning": 50, "good": 80},
        "cpu_efficiency": {"critical": 15, "warning": 50, "good": 80},
        "held_jobs_pct": {"critical": 20, "warning": 10},
        "fast_jobs_pct": {"critical": 30, "warning": 15},
        "runtime_variance": {"critical": 3.0, "warning": 2.0},
    }

    class Colors:
        RED = "\033[91m"
        YELLOW = "\033[93m"
        GREEN = "\033[92m"
        CYAN = "\033[96m"
        BOLD = "\033[1m"
        RESET = "\033[0m"


    def color_text(text, color):
        return f"{color}{text}{Summarize.Colors.RESET}"


    def get_health_status(value, threshold_dict, higher_is_better=True):
        if higher_is_better:
            if value >= threshold_dict["good"]:
                return "HEALTHY"
            elif value >= threshold_dict["warning"]:
                return "WARNING"
            else:
                return "CRITICAL"
        else:
            if value >= threshold_dict["critical"]:
                return "CRITICAL"
            elif value >= threshold_dict["warning"]:
                return "WARNING"
            else:
                return "HEALTHY"


    def get_status_symbol(status):
        if status == "CRITICAL":
            return Summarize.color_text("🔴 CRITICAL", Summarize.Colors.RED)
        elif status == "WARNING":
            return Summarize.color_text("🟡 WARNING", Summarize.Colors.YELLOW)
        else:
            return Summarize.color_text("🟢 HEALTHY", Summarize.Colors.GREEN)


    def generate_health_report(cluster_id, efficiency_data, status_data, runtime_data, held_data):
        findings = []

        mem_eff = efficiency_data.get("memory_efficiency", 0)
        mem_status = Summarize.get_health_status(mem_eff, Summarize.THRESHOLDS["memory_efficiency"])
        mem_jobs = efficiency_data.get("memory_jobs", 0)
        if mem_status == "CRITICAL":
            reason = f"Severe over-provisioning ({mem_eff:.1f}% efficiency)"
        elif mem_status == "WARNING":
            reason = f"Low efficiency ({mem_eff:.1f}%)"
        else:
            reason = f"Well optimized ({mem_eff:.1f}% efficiency)"
        findings.append({
            "aspect": "Memory Efficiency", "status": mem_status,
            "value": f"{mem_eff:.1f}%", "jobs": mem_jobs, "reason": reason,
            "tool": f"python main.py analytics {cluster_id}",
        })

        disk_eff = efficiency_data.get("disk_efficiency", 0)
        disk_status = Summarize.get_health_status(disk_eff, Summarize.THRESHOLDS["disk_efficiency"])
        disk_jobs = efficiency_data.get("disk_jobs", 0)
        if disk_status == "CRITICAL":
            reason = f"Severe over-provisioning ({disk_eff:.1f}% efficiency)"
        elif disk_status == "WARNING":
            reason = f"Low efficiency ({disk_eff:.1f}%)"
        else:
            reason = f"Well optimized ({disk_eff:.1f}% efficiency)"
        findings.append({
            "aspect": "Disk Efficiency", "status": disk_status,
            "value": f"{disk_eff:.1f}%", "jobs": disk_jobs, "reason": reason,
            "tool": f"python main.py analytics {cluster_id}",
        })

        cpu_eff = efficiency_data.get("cpu_efficiency", 0)
        cpu_status = Summarize.get_health_status(cpu_eff, Summarize.THRESHOLDS["cpu_efficiency"])
        cpu_jobs = efficiency_data.get("cpu_jobs", 0)
        if cpu_status == "CRITICAL":
            reason = f"Severe over-provisioning ({cpu_eff:.1f}% efficiency)"
        elif cpu_status == "WARNING":
            reason = f"Low efficiency ({cpu_eff:.1f}%)"
        else:
            reason = f"Well optimized ({cpu_eff:.1f}% efficiency)"
        findings.append({
            "aspect": "CPU Efficiency", "status": cpu_status,
            "value": f"{cpu_eff:.1f}%", "jobs": cpu_jobs, "reason": reason,
            "tool": f"python main.py analytics {cluster_id}",
        })

        held_count = held_data.get("held_count", 0)
        held_pct = status_data.get("held_pct", 0)
        if held_count > 0:
            held_status = Summarize.get_health_status(held_pct, Summarize.THRESHOLDS["held_jobs_pct"], higher_is_better=False)
            unique_reasons = held_data.get("unique_reasons", 0)
            reason = (
                f"{held_count} jobs held ({held_pct:.1f}%), {unique_reasons} unique reasons"
                if held_status in ("CRITICAL", "WARNING")
                else f"{held_count} jobs held ({held_pct:.1f}%)"
            )
            findings.append({
                "aspect": "Held Jobs", "status": held_status,
                "value": str(held_count), "jobs": held_count, "reason": reason,
                "tool": f"python main.py hold {cluster_id}",
            })
        else:
            findings.append({
                "aspect": "Held Jobs", "status": "HEALTHY",
                "value": "0", "jobs": 0, "reason": "No held jobs", "tool": "N/A",
            })

        fast_jobs = runtime_data.get("fast_jobs", 0)
        fast_pct = runtime_data.get("fast_jobs_pct", 0)
        total_runtime_jobs = runtime_data.get("total_runtime_jobs", 0)
        if fast_jobs > 0:
            fast_status = Summarize.get_health_status(fast_pct, Summarize.THRESHOLDS["fast_jobs_pct"], higher_is_better=False)
            if fast_status == "CRITICAL":
                reason = f"{fast_jobs} jobs < 10min ({fast_pct:.1f}%), consider job bundling"
            elif fast_status == "WARNING":
                reason = f"{fast_jobs} jobs < 10min ({fast_pct:.1f}%), review job granularity"
            else:
                reason = f"{fast_jobs} jobs < 10min ({fast_pct:.1f}%)"
            findings.append({
                "aspect": "Fast Jobs", "status": fast_status,
                "value": f"{fast_pct:.1f}%", "jobs": fast_jobs, "reason": reason,
                "tool": f"python main.py histogram {cluster_id}",
            })
        else:
            findings.append({
                "aspect": "Fast Jobs", "status": "HEALTHY",
                "value": "0%", "jobs": 0, "reason": "No fast jobs detected", "tool": "N/A",
            })

        cv = runtime_data.get("cv", 0)
        correlation = runtime_data.get("correlation", 0)
        if cv > 0:
            cv_status = Summarize.get_health_status(cv, Summarize.THRESHOLDS["runtime_variance"], higher_is_better=False)
            if cv_status == "CRITICAL":
                reason = f"High variance (CV={cv:.2f}), inconsistent runtimes"
            elif cv_status == "WARNING":
                reason = f"Moderate variance (CV={cv:.2f})"
            else:
                reason = f"Consistent runtimes (CV={cv:.2f})"
            if correlation > 0.4:
                reason += ", later jobs slower"
            elif correlation < -0.4:
                reason += ", later jobs faster"
            findings.append({
                "aspect": "Runtime Consistency", "status": cv_status,
                "value": f"CV={cv:.2f}", "jobs": total_runtime_jobs, "reason": reason,
                "tool": f"python main.py histogram {cluster_id}",
            })
        else:
            findings.append({
                "aspect": "Runtime Consistency", "status": "HEALTHY",
                "value": "N/A", "jobs": 0, "reason": "No runtime data available", "tool": "N/A",
            })

        total_jobs = status_data.get("total_jobs", 0)
        completed = status_data.get("completed", 0)
        running = status_data.get("running", 0)
        idle = status_data.get("idle", 0)
        if total_jobs > 0:
            findings.append({
                "aspect": "Job Status", "status": "INFO",
                "value": str(total_jobs), "jobs": total_jobs,
                "reason": f"{completed} completed, {running} running, {idle} idle",
                "tool": f"python main.py dashboard {cluster_id}",
            })

        return findings


    def print_health_summary(cluster_id, findings):
        print("\n" + "=" * 120)
        print(f"{Summarize.color_text('HTCondor Cluster Health Report', Summarize.Colors.BOLD + Summarize.Colors.CYAN):^130}")
        print("=" * 120)
        print(f"Cluster ID: {cluster_id}")
        print(f"Report Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 120)

        critical_count = sum(1 for f in findings if f["status"] == "CRITICAL")
        warning_count = sum(1 for f in findings if f["status"] == "WARNING")

        if critical_count > 0:
            overall = Summarize.color_text("🔴 CRITICAL", Summarize.Colors.RED)
        elif warning_count > 0:
            overall = Summarize.color_text("🟡 WARNING", Summarize.Colors.YELLOW)
        else:
            overall = Summarize.color_text("🟢 HEALTHY", Summarize.Colors.GREEN)

        print(f"\nOverall Status: {overall}")
        print(f"Critical Issues: {critical_count} | Warnings: {warning_count}")
        print()

        table_data = []
        for finding in findings:
            if finding["status"] == "INFO":
                status_display = Summarize.color_text("ℹ️  INFO", Summarize.Colors.CYAN)
            else:
                status_display = Summarize.get_status_symbol(finding["status"])
            table_data.append([
                finding["aspect"], status_display, finding["value"],
                finding["reason"], finding["tool"],
            ])

        headers = ["Aspect", "Status", "Value", "Reason / Details", "Tool for Details"]
        print(tabulate(table_data, headers=headers, tablefmt="grid", maxcolwidths=[20, 15, 12, 50, 35]))

        print("\n" + "=" * 120)
        print(f"{Summarize.color_text('Recommended Next Steps', Summarize.Colors.BOLD)}")
        print("=" * 120)

        if critical_count > 0:
            print(f"\n{Summarize.color_text('🔴 HIGH PRIORITY:', Summarize.Colors.RED)}")
            for finding in findings:
                if finding["status"] == "CRITICAL" and finding["tool"] != "N/A":
                    print(f"  • {finding['aspect']}: {finding['tool']}")

        if warning_count > 0:
            print(f"\n{Summarize.color_text('🟡 REVIEW:', Summarize.Colors.YELLOW)}")
            for finding in findings:
                if finding["status"] == "WARNING" and finding["tool"] != "N/A":
                    print(f"  • {finding['aspect']}: {finding['tool']}")

        if critical_count == 0 and warning_count == 0:
            print(f"\n{Summarize.color_text('✓ Cluster is healthy - no immediate action required', Summarize.Colors.GREEN)}")
            print("  • Continue regular monitoring")
            print("  • Run weekly health checks")

        print("\n" + "=" * 120)
        print()


    def run(args):
        """Entry point called by main.py."""
        cluster_id = args.cluster_id

        efficiency_data = Analytics.get_analytics_data(cluster_id)
        if not efficiency_data:
            print(f"ERROR: Could not load analytics data.")
            print(f"Please run: python main.py fetch {cluster_id}")
            sys.exit(1)

        status_data = Dashboard.get_dashboard_data(cluster_id)
        if not status_data:
            status_data = {
                "total_jobs": efficiency_data.get("total_jobs", 0),
                "status_counts": {}, "held": 0, "held_pct": 0,
                "completed": 0, "running": 0, "idle": 0,
            }

        runtime_data = Histogram.get_histogram_data(cluster_id)
        if not runtime_data:
            runtime_data = {
                "cv": 0, "fast_jobs": 0, "fast_jobs_pct": 0,
                "total_runtime_jobs": 0, "correlation": 0,
            }

        held_data = Hold.get_hold_bucket_data(cluster_id)
        if not held_data:
            held_data = {"held_count": 0, "unique_reasons": 0}

        findings = Summarize.generate_health_report(
            cluster_id, efficiency_data, status_data, runtime_data, held_data
        )
        Summarize.print_health_summary(cluster_id, findings)


    def __init__(self, logger, cluster_id, **options):
        _ensure_cluster_data(cluster_id, logger)
        Summarize.run(SimpleNamespace(cluster_id=cluster_id))

class Fetch(Verb):
    """
    Fetches raw job data for a cluster from HTCondor and caches it as CSV
    """

    options = {
        "cluster_id": {
            "args": ("cluster_id",),
            "help": "HTCondor cluster ID to fetch",
        },
        "output_dir": {
            "args": ("--output-dir",),
            "default": None,
            "help": "Directory to save the CSV file (default: ./cluster_data)",
        },
    }

    def __init__(self, logger, cluster_id, **options):
        from fetch_cluster_data import fetch_cluster_jobs, validate_cluster_exists

        if not validate_cluster_exists(cluster_id):
            raise RuntimeError(
                f"No jobs found for cluster {cluster_id}. Verify the cluster "
                f"ID is correct and that you have permission to access it."
            )

        output_dir = options.get("output_dir") or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "cluster_data"
        )
        filepath, job_count = fetch_cluster_jobs(cluster_id, output_dir=output_dir)
        logger.info(f"Fetched {job_count} jobs for cluster {cluster_id} -> {filepath}")

# ── Noun ──────────────────────────────────────────────────────────────────────

class Cluster(Noun):
    """
    Run profiling and health analyses on an HTCondor job cluster
    """

    class dashboard(Dashboard):
        pass

    class histogram(Histogram):
        pass

    class analytics(Analytics):
        pass

    class hold(Hold):
        pass

    class summarize(Summarize):
        pass

    class fetch(Fetch):
        pass

    @classmethod
    def verbs(cls):
        return [
            cls.summarize,
            cls.dashboard,
            cls.histogram,
            cls.analytics,
            cls.hold,
            cls.fetch,
        ]