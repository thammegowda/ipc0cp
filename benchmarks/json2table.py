#!/usr/bin/env python3
"""Render ipc0cp benchmark JSON results as a Markdown table.

Usage:
  python benchmarks/json_to_md_table.py benchmarks/benchmark_results_*.json

This script prints a Markdown snippet you can paste into the root README.
It prefers the precomputed mean/stddev fields, but can fall back to per-run data.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class VariantSpec:
    key: str
    label: str


VARIANTS: list[VariantSpec] = [
    VariantSpec("stdio_raw", "Python STDIO (raw framing)"),
    VariantSpec("stdio_api", "Python STDIO (API framing)"),
    VariantSpec("shm", "Python Shared Memory (SHM)"),
    VariantSpec("cpp_stdio", "C++ STDIO (API framing)"),
    VariantSpec("cpp_shm", "C++ Shared Memory (SHM)"),
    VariantSpec("py_cpp_stdio", "Python → C++ STDIO (API)"),
    VariantSpec("py_cpp_shm", "Python → C++ Shared Memory (SHM)"),
]

# Speedups of interest (consumer throughput): baseline -> variant
SPEEDUP_BASELINES: dict[str, str] = {
    # Python framed STDIO vs Python SHM
    "shm": "stdio_api",
    # C++ STDIO vs C++ SHM
    "cpp_shm": "cpp_stdio",
    # Python -> C++ STDIO vs Python -> C++ SHM
    "py_cpp_shm": "py_cpp_stdio",
}


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _mean(values: Iterable[float]) -> float | None:
    values_list = [v for v in values if v is not None]
    if not values_list:
        return None
    return sum(values_list) / len(values_list)


def _variant_means(entry: dict[str, Any]) -> tuple[float | None, float | None]:
    """Return (producer_mean, consumer_mean) in MB/s."""

    # Prefer computing means from per-run data, so we can filter out
    # obvious outliers (e.g., 0.0 MB/s due to an aborted run).
    runs = entry.get("runs")
    if isinstance(runs, list) and runs:
        producer_values: list[float] = []
        consumer_values: list[float] = []

        for run in runs:
            if not isinstance(run, dict):
                continue
            p = _as_float(run.get("producer_throughput_mbps"))
            c = _as_float(run.get("consumer_throughput_mbps"))
            if p is not None and p > 0.0:
                producer_values.append(p)
            if c is not None and c > 0.0:
                consumer_values.append(c)

        producer_mean = _mean(producer_values)
        consumer_mean = _mean(consumer_values)
        if producer_mean is not None or consumer_mean is not None:
            return producer_mean, consumer_mean

    # Fall back to precomputed fields if we couldn't derive any valid values.
    producer_mean = _as_float(entry.get("producer_mean"))
    consumer_mean = _as_float(entry.get("consumer_mean"))
    return producer_mean, consumer_mean


def _format_num(value: float | None, *, digits: int) -> str:
    if value is None:
        return "—"
    return f"{value:.{digits}f}"


def _warn_if_suspicious(key: str, entry: dict[str, Any]) -> None:
    runs = entry.get("runs")
    if not isinstance(runs, list):
        return

    for idx, run in enumerate(runs, start=1):
        if not isinstance(run, dict):
            continue
        cons_tp = _as_float(run.get("consumer_throughput_mbps"))
        cons_bytes = run.get("consumer_bytes")
        if cons_tp == 0.0 or cons_bytes == 0:
            print(
                f"Warning: {key} run {idx} looks incomplete "
                f"(consumer_throughput_mbps={cons_tp}, consumer_bytes={cons_bytes}).",
                file=sys.stderr,
            )


def render_markdown(data: dict[str, Any], *, digits: int = 2) -> str:
    config = data.get("config") if isinstance(data.get("config"), dict) else {}

    lines: list[str] = []

    # Config line
    runs = config.get("runs")
    duration = config.get("duration")
    min_size = config.get("min_size")
    max_size = config.get("max_size")

    config_bits: list[str] = []
    if runs is not None:
        config_bits.append(f"{runs} runs")
    if duration is not None:
        config_bits.append(f"{duration}s")
    if min_size is not None and max_size is not None:
        config_bits.append(f"payloads {int(min_size) // 1024}KB–{int(max_size) // (1024 * 1024)}MB")

    if config_bits:
        lines.append(f"Benchmark results ({'; '.join(config_bits)}; payload-throughput only):")
    else:
        lines.append("Benchmark results (payload-throughput only):")

    # First pass: compute means so we can compute speedups.
    means_by_key: dict[str, tuple[float | None, float | None]] = {}
    for spec in VARIANTS:
        entry = data.get(spec.key)
        if not isinstance(entry, dict) or not entry.get("available", False):
            continue
        means_by_key[spec.key] = _variant_means(entry)

    # Table
    lines.append("")
    lines.append("| Variant | Producer mean (MB/s) | Consumer mean (MB/s) | Speedup |")
    lines.append("|---|---:|---:|---:|")

    for spec in VARIANTS:
        entry = data.get(spec.key)
        if not isinstance(entry, dict) or not entry.get("available", False):
            continue

        _warn_if_suspicious(spec.key, entry)
        prod_mean, cons_mean = means_by_key.get(spec.key, (None, None))

        speedup_cell = "—"
        baseline_key = SPEEDUP_BASELINES.get(spec.key)
        if baseline_key is not None:
            _baseline_prod, baseline_cons = means_by_key.get(baseline_key, (None, None))
            if baseline_cons is not None and baseline_cons > 0 and cons_mean is not None:
                speedup_cell = f"{cons_mean / baseline_cons:.2f}×"

        lines.append(
            f"| {spec.label} | {_format_num(prod_mean, digits=digits)} | {_format_num(cons_mean, digits=digits)} | {speedup_cell} |"
        )

    # Optional: print legacy global speedups if present (kept for compatibility)
    speed_raw = _as_float(data.get("speedup_vs_stdio_raw"))
    speed_api = _as_float(data.get("speedup_vs_stdio_api"))
    if speed_raw is not None or speed_api is not None:
        lines.append("")
        raw_s = _format_num(speed_raw, digits=2)
        api_s = _format_num(speed_api, digits=2)
        lines.append(
            f"Global speedups (consumer throughput): SHM vs STDIO (raw) **{raw_s}×**, SHM vs STDIO (API) **{api_s}×**."
        )

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Render ipc0cp benchmark JSON results as a Markdown table"
    )
    parser.add_argument(
        "json_file",
        type=Path,
        help="Path to benchmark_results_*.json",
    )
    parser.add_argument(
        "--digits",
        type=int,
        default=2,
        help="Decimal digits for MB/s values (default: 2)",
    )

    args = parser.parse_args()

    data = json.loads(args.json_file.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit("Invalid JSON: expected a top-level object")

    sys.stdout.write(render_markdown(data, digits=args.digits))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
