"""Download Tardis free-tier days for phase 0 (see phase-0-edge-spike, section 2).

Download and normalization only: no fit, no residuals, no analysis.

Without an API key Tardis serves only the 1st of each month, so one "month"
in this script means exactly one day. Two passes run separately: options
(options_chain / OPTIONS) and perp (derivative_ticker + trades /
BTC-PERPETUAL). A single day that cannot be fetched is recorded in the
manifest and the pass continues.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import tardis_dev
from tardis_dev import download_datasets

from research.report.config import load_config

SCHEMA_VERSION = 1
EXCHANGE = "deribit"
DATASETS_ENDPOINT = "https://datasets.tardis.dev/v1"

OPTIONS_COLUMNS = [
    "timestamp",
    "symbol",
    "type",
    "strike_price",
    "expiration",
    "bid_price",
    "bid_amount",
    "ask_price",
    "ask_amount",
    "bid_iv",
    "ask_iv",
    "mark_iv",
    "mark_price",
    "underlying_price",
    "open_interest",
]

# Deribit option prices are quoted in BTC (inverse). The USD columns are
# added alongside these; the originals are never overwritten.
BTC_DENOMINATED_PRICE_COLUMNS = ["bid_price", "ask_price", "mark_price"]

REQUIRED_CONFIG_KEYS = ["months", "paths", "rate_limit", "normalize", "limits"]


class ConfigKeyError(ValueError):
    """A required config parameter is absent. No default is substituted (P-15)."""

    def __init__(self, dotted_key: str):
        super().__init__(
            f"config is missing required parameter {dotted_key!r}. "
            "No default value will be substituted; add it explicitly to the config."
        )
        self.dotted_key = dotted_key


class SchemaMismatchError(RuntimeError):
    """The downloaded CSV does not carry the columns phase 0 expects."""


class TransferQuotaError(RuntimeError):
    """The free tier's data-transfer quota is spent until the window resets."""

    def __init__(self, retry_after_seconds: int):
        hours = retry_after_seconds / 3600
        super().__init__(
            f"Tardis reports the data transfer limit is reached; the window resets in "
            f"{retry_after_seconds}s ({hours:.1f}h). Nothing can be downloaded until then."
        )
        self.retry_after_seconds = retry_after_seconds


def check_transfer_quota() -> None:
    """Fail fast when the transfer quota is spent.

    tardis-dev swallows this 429 in its own retry loop, sleeping up to ~80
    minutes per day before surfacing anything, so ask once up front. The
    request must carry tardis-dev's headers and no Range header -- this
    endpoint answers 403 to any ranged request, whatever the User-Agent.
    """
    url = f"{DATASETS_ENDPOINT}/{EXCHANGE}/derivative_ticker/2025/09/01/BTC-PERPETUAL.csv.gz"
    request = urllib.request.Request(
        url,
        headers={
            "Accept-Encoding": "gzip",
            "User-Agent": f"tardis-dev/{tardis_dev.__version__}",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read(64)
    except urllib.error.HTTPError as error:
        if error.code != 429:
            raise
        body = error.read().decode("utf-8", errors="replace")
        try:
            retry_after = int(json.loads(body).get("retryAfterSeconds", 0))
        except (ValueError, AttributeError):
            retry_after = 0
        raise TransferQuotaError(retry_after) from error


def require(values: dict, *keys: str) -> Any:
    node: Any = values
    for depth, key in enumerate(keys):
        if not isinstance(node, dict) or key not in node:
            raise ConfigKeyError(".".join(keys[: depth + 1]))
        node = node[key]
    return node


def surface_tardis_retries() -> None:
    """Make tardis-dev's internal retry loop visible.

    `reliable_download` retries up to 30 times on its own, sleeping
    `min(2**attempt, 120) + 60` seconds after a 429, and reports each attempt
    only at debug level. Left silent, the process shows no socket, no output
    and no CPU for over an hour and reads as a hang.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s.%(msecs)03d  DEBUG  %(name)s  %(message)s", "%H:%M:%S")
    )
    tardis_logger = logging.getLogger("tardis_dev")
    tardis_logger.setLevel(logging.DEBUG)
    tardis_logger.addHandler(handler)


def log(level: str, **fields: Any) -> None:
    stamp = datetime.now(UTC).strftime("%H:%M:%S.%f")[:-3]
    payload = " ".join(f"{key}={value}" for key, value in fields.items())
    stream = sys.stderr if level == "ERROR" else sys.stdout
    print(f"{stamp}  {level:<5}  tardis  {payload}", file=stream, flush=True)


@dataclass(frozen=True, slots=True)
class Pass:
    name: str
    data_types: tuple[str, ...]
    symbols: tuple[str, ...]


PASSES = (
    Pass("options", ("options_chain",), ("OPTIONS",)),
    Pass("perp", ("derivative_ticker", "trades"), ("BTC-PERPETUAL",)),
)


def free_tier_days(start: str, end: str) -> list[date]:
    """First-of-month days from `start` to `end`, inclusive.

    `end` may be the literal "current" to track today's month.
    """
    start_year, start_month = (int(part) for part in start.split("-"))
    if end == "current":
        today = datetime.now(UTC).date()
        end_year, end_month = today.year, today.month
    else:
        end_year, end_month = (int(part) for part in end.split("-"))

    days: list[date] = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        days.append(date(year, month, 1))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return days


def free_gib(path: Path) -> float:
    return shutil.disk_usage(path).free / (1024**3)


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_manifest(path: Path) -> dict:
    if not path.exists():
        return {"schema_version": SCHEMA_VERSION, "entries": {}}
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest.setdefault("entries", {})
    return manifest


def save_manifest(manifest: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def raw_path_for(raw_root: Path, data_type: str, day: date) -> Path:
    return raw_root / data_type / f"{day.isoformat()}.csv.gz"


def entry_key(data_type: str, day: date) -> str:
    return f"{data_type}/{day.isoformat()}"


def already_done(manifest: dict, raw_root: Path, data_type: str, day: date) -> bool:
    """A day counts as done only if its recorded sha256 still matches the file."""
    entry = manifest["entries"].get(entry_key(data_type, day))
    if entry is None or entry.get("status") != "ok":
        return False
    if entry.get("schema_version") != SCHEMA_VERSION:
        return False
    raw_path = raw_path_for(raw_root, data_type, day)
    if not raw_path.exists():
        return False
    return sha256_of(raw_path) == entry.get("raw_sha256")


def is_rate_limited(error: BaseException) -> bool:
    # Tardis throttles with 429. A 403 from this endpoint means the request
    # shape was rejected (a Range header draws one), not that we are throttled,
    # so retrying a 403 is pointless and it is deliberately not listed here.
    text = str(error).lower()
    return any(marker in text for marker in ("429", "too many requests", "rate limit"))


def fetch_day(
    tardis_pass: Pass,
    day: date,
    raw_root: Path,
    max_attempts: int,
    backoff_seconds: float,
    timeout_seconds: int,
) -> None:
    """Fetch one free-tier day. `to_date` is exclusive, so it is day + 1.

    `timeout_seconds` is aiohttp's *total* timeout, covering the body transfer,
    so it has to clear the largest day (9.7 GiB measured) while still capping a
    stalled connection well under tardis-dev's own 1800 s default.
    """
    next_day = date.fromordinal(day.toordinal() + 1)

    def filename(exchange: str, data_type: str, _day: Any, _symbol: str, _format: str) -> str:
        return f"{data_type}/{day.isoformat()}.csv.gz"

    last_error: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            download_datasets(
                exchange=EXCHANGE,
                data_types=list(tardis_pass.data_types),
                symbols=list(tardis_pass.symbols),
                from_date=day.isoformat(),
                to_date=next_day.isoformat(),
                download_dir=str(raw_root),
                get_filename=filename,
                skip_if_exists=False,
                timeout=timeout_seconds,
            )
            return
        except BaseException as error:  # noqa: BLE001 - one bad day must not end the pass
            last_error = error
            if attempt == max_attempts:
                break
            delay = backoff_seconds * (2 ** (attempt - 1))
            log(
                "WARN",
                event="retry",
                pass_name=tardis_pass.name,
                day=day.isoformat(),
                attempt=attempt,
                rate_limited=is_rate_limited(error),
                sleep_s=round(delay, 1),
                error=type(last_error).__name__,
            )
            time.sleep(delay)

    assert last_error is not None
    raise last_error


def normalize_options_chain(
    raw_path: Path,
    out_path: Path,
    chunk_rows: int,
) -> tuple[int, int]:
    """Filter to BTC options while reading, resample to 1 min, write parquet.

    Returns (rows_read, rows_written). Rows are reduced to the last observation
    per (symbol, minute) inside every chunk, so the accumulated frame stays far
    smaller than the raw file.
    """
    rows_read = 0
    reduced: list[pd.DataFrame] = []

    for chunk in pd.read_csv(raw_path, chunksize=chunk_rows):
        rows_read += len(chunk)
        missing = [column for column in OPTIONS_COLUMNS if column not in chunk.columns]
        if missing:
            raise SchemaMismatchError(
                f"{raw_path.name} is missing expected column(s): {', '.join(missing)}"
            )
        chunk = chunk[chunk["symbol"].str.startswith("BTC-", na=False)]
        if chunk.empty:
            continue
        chunk = chunk[OPTIONS_COLUMNS].copy()
        chunk["minute"] = pd.to_datetime(chunk["timestamp"], unit="us", utc=True).dt.floor("min")
        reduced.append(chunk.drop_duplicates(subset=["symbol", "minute"], keep="last"))

    if not reduced:
        raise SchemaMismatchError(f"{raw_path.name} contained no BTC- option rows")

    frame = pd.concat(reduced, ignore_index=True)
    frame = frame.drop_duplicates(subset=["symbol", "minute"], keep="last")
    frame = frame.sort_values(["symbol", "minute"], ignore_index=True)

    underlying = frame["underlying_price"]
    for column in BTC_DENOMINATED_PRICE_COLUMNS:
        frame[f"{column}_usd"] = frame[column] * underlying

    out_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out_path, index=False)
    return rows_read, len(frame)


def run_pass(
    tardis_pass: Pass,
    days: list[date],
    manifest: dict,
    manifest_path: Path,
    raw_root: Path,
    normalized_root: Path,
    sleep_between_days: float,
    max_attempts: int,
    backoff_seconds: float,
    timeout_seconds: int,
    chunk_rows: int,
    min_free_gb: float,
    force: bool,
) -> None:
    log("INFO", event="pass_start", pass_name=tardis_pass.name, days=len(days))

    for position, day in enumerate(days):
        pending = [
            data_type
            for data_type in tardis_pass.data_types
            if force or not already_done(manifest, raw_root, data_type, day)
        ]
        if not pending:
            log("INFO", event="skip", pass_name=tardis_pass.name, day=day.isoformat())
            continue

        # A free-tier options_chain day runs to several GB, so an exhausted
        # disk is a real outcome here. Stopping the pass beats filling the
        # volume and writing truncated files for every day that follows.
        available = free_gib(raw_root)
        if available < min_free_gb:
            for data_type in pending:
                manifest["entries"][entry_key(data_type, day)] = {
                    "dataset": data_type,
                    "date": day.isoformat(),
                    "status": "skipped",
                    "reason": f"free disk {available:.1f} GiB below floor {min_free_gb} GiB",
                    "schema_version": SCHEMA_VERSION,
                    "recorded_at": datetime.now(UTC).isoformat(),
                }
            save_manifest(manifest, manifest_path)
            log(
                "ERROR",
                event="low_disk_stop",
                pass_name=tardis_pass.name,
                day=day.isoformat(),
                free_gib=round(available, 1),
                floor_gib=min_free_gb,
            )
            break

        if position > 0:
            time.sleep(sleep_between_days)

        started = time.monotonic()
        try:
            fetch_day(
                tardis_pass, day, raw_root, max_attempts, backoff_seconds, timeout_seconds
            )
        except BaseException as error:  # noqa: BLE001 - record and continue
            for data_type in pending:
                manifest["entries"][entry_key(data_type, day)] = {
                    "dataset": data_type,
                    "date": day.isoformat(),
                    "status": "failed",
                    "reason": f"{type(error).__name__}: {error}",
                    "rate_limited": is_rate_limited(error),
                    "attempts": max_attempts,
                    "schema_version": SCHEMA_VERSION,
                    "recorded_at": datetime.now(UTC).isoformat(),
                }
            save_manifest(manifest, manifest_path)
            log(
                "ERROR",
                event="download_failed",
                pass_name=tardis_pass.name,
                day=day.isoformat(),
                rate_limited=is_rate_limited(error),
                error=type(error).__name__,
            )
            continue

        download_seconds = time.monotonic() - started

        for data_type in pending:
            key = entry_key(data_type, day)
            raw_path = raw_path_for(raw_root, data_type, day)
            if not raw_path.exists():
                manifest["entries"][key] = {
                    "dataset": data_type,
                    "date": day.isoformat(),
                    "status": "failed",
                    "reason": "download reported success but no file was written",
                    "rate_limited": False,
                    "schema_version": SCHEMA_VERSION,
                    "recorded_at": datetime.now(UTC).isoformat(),
                }
                save_manifest(manifest, manifest_path)
                log("ERROR", event="missing_file", dataset=data_type, day=day.isoformat())
                continue

            entry: dict[str, Any] = {
                "dataset": data_type,
                "date": day.isoformat(),
                "status": "ok",
                "raw_bytes": raw_path.stat().st_size,
                "raw_sha256": sha256_of(raw_path),
                "download_seconds": round(download_seconds, 2),
                "downloaded_at": datetime.now(UTC).isoformat(),
                "schema_version": SCHEMA_VERSION,
            }

            if data_type == "options_chain":
                out_path = normalized_root / data_type / f"date={day.isoformat()}" / "part.parquet"
                normalize_started = time.monotonic()
                try:
                    rows_read, rows_written = normalize_options_chain(
                        raw_path, out_path, chunk_rows
                    )
                except (SchemaMismatchError, ValueError) as error:
                    entry.update(
                        status="failed",
                        reason=f"{type(error).__name__}: {error}",
                        rate_limited=False,
                    )
                    manifest["entries"][key] = entry
                    save_manifest(manifest, manifest_path)
                    log("ERROR", event="normalize_failed", day=day.isoformat(), error=str(error))
                    continue
                entry.update(
                    rows_raw=rows_read,
                    rows_normalized=rows_written,
                    normalize_seconds=round(time.monotonic() - normalize_started, 2),
                    normalized_path=str(out_path),
                )

            manifest["entries"][key] = entry
            save_manifest(manifest, manifest_path)
            log(
                "INFO",
                event="day_ok",
                dataset=data_type,
                day=day.isoformat(),
                mib=round(entry["raw_bytes"] / (1024 * 1024), 1),
                rows=entry.get("rows_normalized", "-"),
                seconds=entry["download_seconds"],
            )

    log("INFO", event="pass_done", pass_name=tardis_pass.name)


def summarize(manifest: dict) -> None:
    per_dataset: dict[str, dict[str, Any]] = {}
    failures: list[tuple[str, str, str]] = []

    for entry in manifest["entries"].values():
        stats = per_dataset.setdefault(
            entry["dataset"], {"ok": 0, "failed": 0, "bytes": 0}
        )
        if entry["status"] == "ok":
            stats["ok"] += 1
            stats["bytes"] += entry.get("raw_bytes", 0)
        else:
            stats["failed"] += 1
            failures.append((entry["dataset"], entry["date"], entry.get("reason", "unknown")))

    total_bytes = sum(stats["bytes"] for stats in per_dataset.values())
    for dataset, stats in sorted(per_dataset.items()):
        log(
            "INFO",
            event="summary",
            dataset=dataset,
            ok=stats["ok"],
            failed=stats["failed"],
            gib=round(stats["bytes"] / (1024**3), 2),
        )
    log("INFO", event="summary_total", gib=round(total_bytes / (1024**3), 2))

    for dataset, day, reason in sorted(failures):
        log("ERROR", event="summary_failure", dataset=dataset, day=day, reason=reason)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-download days already recorded as ok in the manifest",
    )
    args = parser.parse_args()
    surface_tardis_retries()

    try:
        run_config = load_config(args.config, required_keys=REQUIRED_CONFIG_KEYS)
        values = run_config.values

        days = free_tier_days(require(values, "months", "start"), require(values, "months", "end"))
        raw_root = Path(require(values, "paths", "raw_root"))
        normalized_root = Path(require(values, "paths", "normalized_root"))
        manifest_path = Path(require(values, "paths", "manifest"))
        sleep_between_days = float(require(values, "rate_limit", "sleep_seconds_between_days"))
        max_attempts = int(require(values, "rate_limit", "max_attempts"))
        backoff_seconds = float(require(values, "rate_limit", "backoff_seconds"))
        timeout_seconds = int(require(values, "rate_limit", "download_timeout_seconds"))
        chunk_rows = int(require(values, "normalize", "chunk_rows"))
        min_free_gb = float(require(values, "limits", "min_free_gb"))
        raw_root.mkdir(parents=True, exist_ok=True)
    except (ConfigKeyError, ValueError, OSError) as error:
        print(f"tardis_download failed to start: {error}", file=sys.stderr)
        return 1

    log(
        "INFO",
        event="start",
        config_sha256=run_config.content_hash[:12],
        days=len(days),
        first=days[0].isoformat(),
        last=days[-1].isoformat(),
        force=args.force,
    )

    try:
        check_transfer_quota()
    except TransferQuotaError as error:
        log("ERROR", event="transfer_quota_spent", retry_after_s=error.retry_after_seconds)
        print(f"tardis_download stopped: {error}", file=sys.stderr)
        return 1

    manifest = load_manifest(manifest_path)
    for tardis_pass in PASSES:
        run_pass(
            tardis_pass,
            days,
            manifest,
            manifest_path,
            raw_root,
            normalized_root,
            sleep_between_days,
            max_attempts,
            backoff_seconds,
            timeout_seconds,
            chunk_rows,
            min_free_gb,
            args.force,
        )

    summarize(manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
