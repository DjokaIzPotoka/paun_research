"""Continuous Deribit BTC L1 collector for phase 0 (phase-0-edge-spike, section 2).

Collection and writing only: no fit, no residuals, no unit conversion. Prices
are stored exactly as Deribit sends them (BTC for inverse options).

Every BTC option, BTC-PERPETUAL, every BTC expiry future and the BTC index are
subscribed on Deribit's public WebSocket. The last message per instrument is
kept per minute window; at each full minute one row is emitted for every
instrument that had at least one message in that minute, labelled with the
window END (as-of time). An instrument with no message gets no row -- a stale
value is never carried forward.

strike / expiration / option_type / kind come from public/get_instruments, not
from the ticker. The symbol is parsed only as a cross-check (meta_mismatch). A
symbol missing from the metadata cache triggers a refresh; its row is held one
minute, and if metadata is still missing then, the lost minutes are recorded
in collector_gaps as missing_metadata instead of being written with guesses.

    uv run python scripts/deribit_l1_collector.py --config scripts/deribit_l1_collector.yaml
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import re
import signal
import sys
import time
import uuid
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol

import aiohttp
import pyarrow as pa
import pyarrow.parquet as pq

from research.report.config import ConfigKeyError, load_config, require

SCHEMA_VERSION = 1
MINUTE_MS = 60_000
HOUR_MS = 3_600_000
INDEX_KIND = "index"

_ts_ms = pa.timestamp("ms", tz="UTC")
_ts_us = pa.timestamp("us", tz="UTC")


def _f(name: str) -> pa.Field:
    return pa.field(name, pa.float64())


L1_SCHEMA = pa.schema(
    [
        pa.field("snapshot_ts", _ts_ms, nullable=False),
        pa.field("recv_ts", _ts_us, nullable=False),
        pa.field("venue_ts", _ts_ms, nullable=False),
        pa.field("quote_age_ms", pa.int64(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        _f("strike"),
        pa.field("expiration", _ts_ms),
        pa.field("option_type", pa.string()),
        *(
            _f(name)
            for name in (
                "bid_price",
                "bid_amount",
                "ask_price",
                "ask_amount",
                "bid_iv",
                "ask_iv",
                "mark_iv",
                "mark_price",
                "underlying_price",
                "index_price",
                "open_interest",
                "delta",
                "gamma",
                "vega",
                "theta",
                "interest_rate",
                "funding_8h",
                "current_funding",
            )
        ),
        pa.field("meta_mismatch", pa.bool_(), nullable=False),
        pa.field("instruments_fetched_at", _ts_ms),
        pa.field("schema_version", pa.uint16(), nullable=False),
    ]
)

GAP_SCHEMA = pa.schema(
    [
        pa.field("gap_start", _ts_ms, nullable=False),
        pa.field("gap_end", _ts_ms, nullable=False),
        pa.field("reason", pa.string(), nullable=False),
        pa.field("affected_instruments", pa.uint32(), nullable=False),
        pa.field("detail", pa.string(), nullable=False),
        pa.field("recorded_at", _ts_ms, nullable=False),
        pa.field("schema_version", pa.uint16(), nullable=False),
    ]
)

INSTRUMENT_SCHEMA = pa.schema(
    [
        pa.field("fetched_at", _ts_ms, nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("instrument_id", pa.uint64(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("option_type", pa.string()),
        _f("strike"),
        pa.field("expiration_timestamp", pa.int64(), nullable=False),
        pa.field("creation_timestamp", pa.int64(), nullable=False),
        pa.field("is_active", pa.bool_(), nullable=False),
        pa.field("settlement_currency", pa.string()),
        pa.field("settlement_period", pa.string(), nullable=False),
        pa.field("contract_size", pa.float64(), nullable=False),
        pa.field("tick_size", pa.float64(), nullable=False),
        pa.field("min_trade_amount", pa.float64(), nullable=False),
        pa.field("schema_version", pa.uint16(), nullable=False),
    ]
)

# table -> (schema, column that decides which hourly file a row lands in, dedup key)
TABLES: dict[str, tuple[pa.Schema, str, tuple[str, ...]]] = {
    "l1_snapshots": (L1_SCHEMA, "snapshot_ts", ("symbol", "snapshot_ts")),
    "collector_gaps": (GAP_SCHEMA, "recorded_at", ("gap_start", "reason", "gap_end")),
    "instruments": (INSTRUMENT_SCHEMA, "fetched_at", ("symbol", "fetched_at")),
}

REQUIRED_CONFIG_KEYS = ["deribit", "snapshot", "reconnect", "sink", "state_dir"]


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def now_us() -> int:
    return time.time_ns() // 1_000


def log(level: str, **fields: Any) -> None:
    stamp = datetime.now(UTC).strftime("%H:%M:%S.%f")[:-3]
    payload = " ".join(f"{key}={value}" for key, value in fields.items())
    stream = sys.stderr if level == "ERROR" else sys.stdout
    print(f"{stamp}  {level:<5}  l1  {payload}", file=stream, flush=True)


# --------------------------------------------------------------------------- metadata

_MONTHS = {
    m: i
    for i, m in enumerate(
        ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"),
        start=1,
    )
}
_EXPIRY_TOKEN = re.compile(r"(\d{1,2})([A-Z]{3})(\d{2})")


def _expiry_date(token: str) -> date | None:
    match = _EXPIRY_TOKEN.fullmatch(token)
    if not match or match.group(2) not in _MONTHS:
        return None
    day, month, year = int(match.group(1)), _MONTHS[match.group(2)], 2000 + int(match.group(3))
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _utc_date(ms: int) -> date:
    return datetime.fromtimestamp(ms / 1000, UTC).date()


def symbol_agrees(
    symbol: str, kind: str, strike: float | None, option_type: str | None, expiration_ms: int | None
) -> bool:
    """Cross-check venue metadata against what the symbol itself claims."""
    parts = symbol.split("-")
    if kind == "future":
        if len(parts) != 2:
            return False
        if parts[1] == "PERPETUAL":
            return expiration_ms is None
        expiry = _expiry_date(parts[1])
        return (
            expiry is not None and expiration_ms is not None and _utc_date(expiration_ms) == expiry
        )
    if kind == "option":
        if len(parts) != 4 or expiration_ms is None or strike is None:
            return False
        expiry = _expiry_date(parts[1])
        try:
            parsed_strike = float(parts[2].replace("d", "."))
        except ValueError:
            return False
        parsed_type = {"C": "call", "P": "put"}.get(parts[3])
        return (
            expiry == _utc_date(expiration_ms)
            and parsed_strike == float(strike)
            and parsed_type == option_type
        )
    return False


@dataclass(frozen=True, slots=True)
class InstrumentMeta:
    symbol: str
    kind: str
    strike: float | None
    expiration_ms: int | None
    option_type: str | None
    fetched_at_ms: int
    meta_mismatch: bool


def instrument_from_rest(item: dict, fetched_at_ms: int) -> tuple[InstrumentMeta, dict]:
    """Venue metadata for one instrument, plus its row for the `instruments` table.

    expiration comes from expiration_timestamp (ms, UTC), never from the name.
    A perpetual has no expiration (Deribit reports year 3000), so it is None.
    """
    symbol = item["instrument_name"]
    kind = item["kind"]
    option_type = item.get("option_type")
    strike = item.get("strike")
    perpetual = item["settlement_period"] == "perpetual"
    expiration_ms = None if perpetual else int(item["expiration_timestamp"])
    meta = InstrumentMeta(
        symbol=symbol,
        kind=kind,
        strike=None if strike is None else float(strike),
        expiration_ms=expiration_ms,
        option_type=option_type,
        fetched_at_ms=fetched_at_ms,
        meta_mismatch=not symbol_agrees(symbol, kind, strike, option_type, expiration_ms),
    )
    row = {
        "fetched_at": fetched_at_ms,
        "symbol": symbol,
        "instrument_id": item["instrument_id"],
        "kind": kind,
        "option_type": option_type,
        "strike": strike,
        "expiration_timestamp": item["expiration_timestamp"],
        "creation_timestamp": item["creation_timestamp"],
        "is_active": item["is_active"],
        "settlement_currency": item.get("settlement_currency"),
        "settlement_period": item["settlement_period"],
        "contract_size": item["contract_size"],
        "tick_size": item["tick_size"],
        "min_trade_amount": item["min_trade_amount"],
        "schema_version": SCHEMA_VERSION,
    }
    return meta, row


# --------------------------------------------------------------------------- rows


def ticker_row(snapshot_ms: int, recv_us: int, data: dict, meta: InstrumentMeta) -> dict:
    """One l1_snapshots row from a ticker message. Values are copied, never converted."""
    greeks = data.get("greeks") or {}
    return {
        "snapshot_ts": snapshot_ms,
        "recv_ts": recv_us,
        "venue_ts": data["timestamp"],
        "quote_age_ms": snapshot_ms - recv_us // 1000,
        "symbol": data["instrument_name"],
        "kind": meta.kind,
        "strike": meta.strike,
        "expiration": meta.expiration_ms,
        "option_type": meta.option_type,
        "bid_price": data.get("best_bid_price"),
        "bid_amount": data.get("best_bid_amount"),
        "ask_price": data.get("best_ask_price"),
        "ask_amount": data.get("best_ask_amount"),
        "bid_iv": data.get("bid_iv"),
        "ask_iv": data.get("ask_iv"),
        "mark_iv": data.get("mark_iv"),
        "mark_price": data.get("mark_price"),
        "underlying_price": data.get("underlying_price"),
        "index_price": data.get("index_price"),
        "open_interest": data.get("open_interest"),
        "delta": greeks.get("delta"),
        "gamma": greeks.get("gamma"),
        "vega": greeks.get("vega"),
        "theta": greeks.get("theta"),
        "interest_rate": data.get("interest_rate"),
        "funding_8h": data.get("funding_8h"),
        "current_funding": data.get("current_funding"),
        "meta_mismatch": meta.meta_mismatch,
        "instruments_fetched_at": meta.fetched_at_ms,
        "schema_version": SCHEMA_VERSION,
    }


def index_row(snapshot_ms: int, recv_us: int, data: dict) -> dict:
    row = dict.fromkeys(L1_SCHEMA.names)
    row.update(
        snapshot_ts=snapshot_ms,
        recv_ts=recv_us,
        venue_ts=data["timestamp"],
        quote_age_ms=snapshot_ms - recv_us // 1000,
        symbol=data["index_name"],
        kind=INDEX_KIND,
        index_price=data["price"],
        meta_mismatch=False,
        schema_version=SCHEMA_VERSION,
    )
    return row


def gap_row(start_ms: int, end_ms: int, reason: str, affected: int, detail: str = "") -> dict:
    return {
        "gap_start": start_ms,
        "gap_end": end_ms,
        "reason": reason,
        "affected_instruments": affected,
        "detail": detail,
        "recorded_at": now_ms(),
        "schema_version": SCHEMA_VERSION,
    }


# --------------------------------------------------------------------------- minute book


@dataclass(slots=True)
class Quote:
    data: dict
    recv_us: int
    is_index: bool


class SnapshotBook:
    """Last message per instrument, bucketed by the minute it was received in.

    Bucketing on receipt time (not on when the flush happens to run) keeps a
    message that lands a few ms after the boundary out of the window it does
    not belong to.
    """

    def __init__(self) -> None:
        self._windows: dict[int, dict[str, Quote]] = defaultdict(dict)

    def on_message(self, symbol: str, data: dict, recv_us: int, is_index: bool = False) -> None:
        window_end = (recv_us // 1000 // MINUTE_MS + 1) * MINUTE_MS
        self._windows[window_end][symbol] = Quote(data, recv_us, is_index)

    def take(self, window_end_ms: int) -> dict[str, Quote]:
        """Messages for the window ending at `window_end_ms`; older unflushed ones are dropped."""
        for stale in [end for end in self._windows if end < window_end_ms]:
            del self._windows[stale]
        return self._windows.pop(window_end_ms, {})


@dataclass(slots=True)
class _Pending:
    snapshot_ms: int
    quote: Quote


class MetadataGate:
    """Turns a minute's quotes into rows, holding rows whose metadata is not cached yet.

    A symbol without metadata is held for one minute. If metadata arrives by
    the next flush, the held row is written with its original snapshot_ts.
    If not, that minute -- and every following minute until metadata appears --
    goes to collector_gaps as missing_metadata. Nothing is written with empty or
    guessed metadata.
    """

    def __init__(self) -> None:
        self.meta: dict[str, InstrumentMeta] = {}
        self._pending: dict[str, _Pending] = {}
        self._streak: set[str] = set()

    def update(self, metas: dict[str, InstrumentMeta]) -> None:
        self.meta = metas

    def knows(self, symbol: str) -> bool:
        return symbol in self.meta

    @property
    def waiting(self) -> int:
        return len(self._pending) + len(self._streak)

    def resolve(self, snapshot_ms: int, quotes: dict[str, Quote]) -> tuple[list[dict], list[dict]]:
        rows: list[dict] = []
        lost: dict[int, set[str]] = defaultdict(set)

        for symbol, pending in self._pending.items():
            meta = self.meta.get(symbol)
            if meta is not None:
                rows.append(
                    ticker_row(pending.snapshot_ms, pending.quote.recv_us, pending.quote.data, meta)
                )
            else:
                lost[pending.snapshot_ms].add(symbol)
                self._streak.add(symbol)
        self._pending = {}

        for symbol, quote in quotes.items():
            if quote.is_index:
                rows.append(index_row(snapshot_ms, quote.recv_us, quote.data))
                continue
            meta = self.meta.get(symbol)
            if meta is not None:
                rows.append(ticker_row(snapshot_ms, quote.recv_us, quote.data, meta))
            elif symbol in self._streak:
                lost[snapshot_ms].add(symbol)
            else:
                self._pending[symbol] = _Pending(snapshot_ms, quote)

        self._streak = {symbol for symbol in self._streak if symbol not in self.meta}
        gaps = [
            gap_row(
                end - MINUTE_MS,
                end,
                "missing_metadata",
                len(symbols),
                ",".join(sorted(symbols)[:50]),
            )
            for end, symbols in sorted(lost.items())
        ]
        return rows, gaps


class ConnectionGaps:
    """Opens a gap when we stop listening and closes it once resubscribed."""

    def __init__(self) -> None:
        self._open: tuple[int, str] | None = None
        self._attempts = 0

    @property
    def is_open(self) -> bool:
        return self._open is not None

    def disconnected(self, last_heard_ms: int, reason: str) -> None:
        self._attempts += 1
        if self._open is None:
            self._open = (last_heard_ms, reason)

    def resubscribed(self, at_ms: int, affected: int) -> dict | None:
        if self._open is None:
            return None
        start_ms, reason = self._open
        row = gap_row(start_ms, at_ms, reason, affected, f"reconnect_attempts={self._attempts}")
        self._open = None
        self._attempts = 0
        return row


# --------------------------------------------------------------------------- sinks


def _table(name: str, rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=TABLES[name][0])


def _hour_start(value: int | datetime) -> int:
    ms = int(value.timestamp() * 1000) if isinstance(value, datetime) else int(value)
    return ms - ms % HOUR_MS


def _atomic_write(table: pa.Table, final: Path) -> None:
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.with_name(final.name + ".tmp")
    pq.write_table(table, tmp, compression="zstd")
    with tmp.open("rb+") as handle:
        os.fsync(handle.fileno())
    os.replace(tmp, final)


class Sink(Protocol):
    def write(self, table: str, rows: list[dict]) -> None: ...
    def seal_before(self, hour_start_ms: int) -> list[Path]: ...


class FileSink:
    """Hourly parquet files: <root>/<table>/date=YYYY-MM-DD/HH.parquet.

    Rows first land as minute parts in <root>/<table>/.staging/YYYY-MM-DD/HH/,
    so a crash loses nothing already flushed. Once an hour is over its parts
    are merged into HH.parquet with an atomic rename, and that file is never
    touched again. A row that arrives for an already sealed hour goes into a
    separate HH.late-*.parquet instead, so a sync tool can copy a sealed
    hour at any time without racing the collector.
    """

    def __init__(self, root: Path):
        self.root = Path(root)

    def _paths(self, table: str, hour_ms: int) -> tuple[Path, Path]:
        moment = datetime.fromtimestamp(hour_ms / 1000, UTC)
        day, hour = moment.strftime("%Y-%m-%d"), moment.strftime("%H")
        final = self.root / table / f"date={day}" / f"{hour}.parquet"
        staging = self.root / table / ".staging" / day / hour
        return final, staging

    def write(self, table: str, rows: list[dict]) -> None:
        if not rows:
            return
        bucket_column = TABLES[table][1]
        by_hour: dict[int, list[dict]] = defaultdict(list)
        for row in rows:
            by_hour[_hour_start(row[bucket_column])].append(row)

        for hour_ms, hour_rows in by_hour.items():
            final, staging = self._paths(table, hour_ms)
            if final.exists():
                late = final.with_name(
                    f"{final.stem}.late-{now_ms()}-{uuid.uuid4().hex[:8]}.parquet"
                )
                _atomic_write(_table(table, hour_rows), late)
                log("WARN", event="late_rows", table=table, file=late.name, rows=len(hour_rows))
            else:
                _atomic_write(
                    _table(table, hour_rows), staging / f"{now_ms()}-{uuid.uuid4().hex[:8]}.parquet"
                )

    def seal_before(self, hour_start_ms: int) -> list[Path]:
        sealed: list[Path] = []
        for table, (_schema, _bucket, key) in TABLES.items():
            staging_root = self.root / table / ".staging"
            if not staging_root.is_dir():
                continue
            for hour_dir in sorted(staging_root.glob("*/*")):
                day, hour = hour_dir.parent.name, hour_dir.name
                hour_ms = _hour_start(
                    datetime.strptime(f"{day} {hour}", "%Y-%m-%d %H").replace(tzinfo=UTC)
                )
                if hour_ms >= hour_start_ms:
                    continue
                final, _ = self._paths(table, hour_ms)
                parts = sorted(hour_dir.glob("*.parquet"))
                if not final.exists() and parts:
                    merged = pa.concat_tables(
                        [pq.read_table(p, schema=TABLES[table][0]) for p in parts]
                    )
                    _atomic_write(_dedupe_sorted(merged, key), final)
                    sealed.append(final)
                for leftover in hour_dir.iterdir():
                    leftover.unlink()
                hour_dir.rmdir()
                with contextlib.suppress(OSError):
                    hour_dir.parent.rmdir()
        return sealed


def _dedupe_sorted(table: pa.Table, key: tuple[str, ...]) -> pa.Table:
    """Sort by `key` and keep the last row written for each key."""
    indexed = table.append_column("__order", pa.array(range(table.num_rows), pa.int64()))
    ordered = indexed.sort_by([(k, "ascending") for k in key] + [("__order", "descending")])
    keep, previous = [], None
    columns = [ordered.column(k).to_pylist() for k in key]
    for i, current in enumerate(zip(*columns, strict=True)):
        if current != previous:
            keep.append(i)
            previous = current
    return ordered.take(keep).drop_columns(["__order"])


class ClickHouseSink:
    def __init__(self, section: dict):
        from scripts.load_l1 import ClickHouse, migrate

        self.client = ClickHouse.from_config(section)
        applied = migrate(self.client)
        log(
            "INFO",
            event="clickhouse_ready",
            database=self.client.database,
            migrated=",".join(applied) or "-",
        )

    def write(self, table: str, rows: list[dict]) -> None:
        if not rows:
            return
        sink = pa.BufferOutputStream()
        pq.write_table(_table(table, rows), sink, compression="zstd")
        self.client.insert_parquet(table, sink.getvalue().to_pybytes())

    def seal_before(self, hour_start_ms: int) -> list[Path]:
        return []


def make_sink(section: dict) -> Sink:
    kind = require(section, "kind")
    if kind == "file":
        return FileSink(Path(require(section, "file", "root")))
    if kind == "clickhouse":
        return ClickHouseSink(require(section, "clickhouse"))
    raise ValueError(f"sink.kind must be 'file' or 'clickhouse', got {kind!r}")


# --------------------------------------------------------------------------- collector


def batched(items: list[str], size: int) -> Iterator[list[str]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


@dataclass
class Settings:
    ws_url: str
    rest_url: str
    currency: str
    ticker_interval: str
    index_channel: str
    subscribe_batch_size: int
    heartbeat_interval_s: int
    stale_after_s: float
    request_timeout_s: float
    refresh_every_s: float
    post_expiry_refresh_utc: tuple[int, int]
    metadata_refresh_min_interval_s: float
    flush_grace_ms: int
    backoff_initial_s: float
    backoff_max_s: float
    retry_buffer_max_rows: int
    state_dir: Path

    @classmethod
    def from_config(cls, values: dict) -> Settings:
        hh, mm = (
            int(p) for p in str(require(values, "deribit", "post_expiry_refresh_utc")).split(":")
        )
        return cls(
            ws_url=require(values, "deribit", "ws_url"),
            rest_url=require(values, "deribit", "rest_url").rstrip("/"),
            currency=require(values, "deribit", "currency"),
            ticker_interval=require(values, "deribit", "ticker_interval"),
            index_channel=require(values, "deribit", "index_channel"),
            subscribe_batch_size=int(require(values, "deribit", "subscribe_batch_size")),
            heartbeat_interval_s=int(require(values, "deribit", "heartbeat_interval_seconds")),
            stale_after_s=float(require(values, "deribit", "stale_after_seconds")),
            request_timeout_s=float(require(values, "deribit", "request_timeout_seconds")),
            refresh_every_s=float(require(values, "deribit", "instruments_refresh_seconds")),
            post_expiry_refresh_utc=(hh, mm),
            metadata_refresh_min_interval_s=float(
                require(values, "deribit", "metadata_refresh_min_interval_seconds")
            ),
            flush_grace_ms=int(require(values, "snapshot", "flush_grace_ms")),
            backoff_initial_s=float(require(values, "reconnect", "initial_backoff_seconds")),
            backoff_max_s=float(require(values, "reconnect", "max_backoff_seconds")),
            retry_buffer_max_rows=int(require(values, "sink", "retry_buffer_max_rows")),
            state_dir=Path(require(values, "state_dir")),
        )

    def ticker_channel(self, symbol: str) -> str:
        return f"ticker.{symbol}.{self.ticker_interval}"


class ConnectionLost(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass
class Snapshotter:
    """All per-minute state, independent of the network. Collector feeds it."""

    book: SnapshotBook = field(default_factory=SnapshotBook)
    gate: MetadataGate = field(default_factory=MetadataGate)
    connection: ConnectionGaps = field(default_factory=ConnectionGaps)
    pending_gaps: list[dict] = field(default_factory=list)

    def flush(self, snapshot_ms: int) -> tuple[list[dict], list[dict]]:
        rows, gaps = self.gate.resolve(snapshot_ms, self.book.take(snapshot_ms))
        gaps = self.pending_gaps + gaps
        self.pending_gaps = []
        return rows, gaps


class Collector:
    def __init__(self, settings: Settings, sink: Sink):
        self.s = settings
        self.sink = sink
        self.snap = Snapshotter()
        self.subscribed: set[str] = set()
        self.desired: set[str] = set()
        self.last_heard_ms = now_ms()
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._next_id = 0
        self._replies: dict[int, asyncio.Future] = {}
        self._refresh_requested = asyncio.Event()
        self._subscription_lock = asyncio.Lock()
        self._last_refresh_attempt = 0.0
        self._retry: dict[str, list[dict]] = defaultdict(list)
        self._state_file = self.s.state_dir / "last_flushed_ms"

    # ---- lifecycle

    async def run(self) -> None:
        self.s.state_dir.mkdir(parents=True, exist_ok=True)
        if self._state_file.exists():
            last = int(self._state_file.read_text(encoding="utf-8").strip())
            self.snap.connection.disconnected(last, "process_restart")
        self.sink.seal_before(_hour_start(now_ms()))

        timeout = aiohttp.ClientTimeout(total=self.s.request_timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as http:
            self.http = http
            await self._refresh_instruments()
            tasks = [
                asyncio.create_task(self._connection_loop(), name="connection"),
                asyncio.create_task(self._flush_loop(), name="flush"),
                asyncio.create_task(self._refresh_loop(), name="refresh"),
            ]
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
                for task in done:
                    task.result()
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    # ---- instruments

    async def _fetch(self, kind: str) -> list[dict]:
        url = f"{self.s.rest_url}/public/get_instruments"
        params = {"currency": self.s.currency, "kind": kind, "expired": "false"}
        async with self.http.get(url, params=params) as response:
            response.raise_for_status()
            return (await response.json())["result"]

    async def _refresh_instruments(self) -> bool:
        self._last_refresh_attempt = time.monotonic()
        fetched_ms = now_ms()
        try:
            items = await self._fetch("option") + await self._fetch("future")
        except (aiohttp.ClientError, TimeoutError, KeyError, ValueError) as error:
            log(
                "ERROR",
                event="instruments_refresh_failed",
                error=f"{type(error).__name__}: {error}",
            )
            return False

        metas, rows, mismatched = {}, [], []
        for item in items:
            meta, row = instrument_from_rest(item, fetched_ms)
            metas[meta.symbol] = meta
            rows.append(row)
            if meta.meta_mismatch:
                mismatched.append(meta.symbol)
        for symbol in mismatched[:20]:
            log("WARN", event="meta_mismatch", symbol=symbol)

        self.snap.gate.update(metas)
        self._write("instruments", rows)
        self.desired = {self.s.ticker_channel(s) for s in metas} | {self.s.index_channel}
        log(
            "INFO",
            event="instruments",
            options=sum(m.kind == "option" for m in metas.values()),
            futures=sum(m.kind == "future" for m in metas.values()),
            meta_mismatch=len(mismatched),
        )
        if self._ws is not None and not self._ws.closed:
            try:
                await self._sync_subscriptions()
            except (ConnectionLost, TimeoutError, aiohttp.ClientError) as error:
                # The connection loop owns reconnecting; it resubscribes to `desired`.
                log(
                    "WARN",
                    event="subscription_sync_deferred",
                    error=f"{type(error).__name__}: {error}",
                )
        return True

    def _next_refresh_at(self, last: float) -> float:
        by_interval = last + self.s.refresh_every_s
        hh, mm = self.s.post_expiry_refresh_utc
        base = datetime.fromtimestamp(last, UTC)
        daily = base.replace(hour=hh, minute=mm, second=0, microsecond=0).timestamp()
        if daily <= last:
            daily += 86_400
        return min(by_interval, daily)

    async def _refresh_loop(self) -> None:
        last = time.time()
        while True:
            wait = max(0.0, self._next_refresh_at(last) - time.time())
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._refresh_requested.wait(), timeout=wait)
            self._refresh_requested.clear()
            if await self._refresh_instruments():
                last = time.time()
            else:
                last = time.time() - self.s.refresh_every_s + self.s.metadata_refresh_min_interval_s

    def _request_refresh(self) -> None:
        if time.monotonic() - self._last_refresh_attempt >= self.s.metadata_refresh_min_interval_s:
            self._refresh_requested.set()

    # ---- websocket

    async def _call(self, method: str, params: dict) -> Any:
        if self._ws is None or self._ws.closed:
            raise ConnectionLost("connection_lost:not_connected")
        self._next_id += 1
        request_id = self._next_id
        future = asyncio.get_running_loop().create_future()
        self._replies[request_id] = future
        await self._ws.send_str(
            json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        )
        try:
            reply = await asyncio.wait_for(future, timeout=self.s.request_timeout_s)
        finally:
            self._replies.pop(request_id, None)
        if "error" in reply:
            raise ConnectionLost(f"{method}_error:{reply['error'].get('message')}")
        return reply["result"]

    async def _sync_subscriptions(self) -> None:
        async with self._subscription_lock:
            await self._sync_subscriptions_locked()

    async def _sync_subscriptions_locked(self) -> None:
        wanted = sorted(self.desired - self.subscribed)
        rejected: list[str] = []
        for batch in batched(wanted, self.s.subscribe_batch_size):
            accepted = set(await self._call("public/subscribe", {"channels": batch}))
            self.subscribed |= accepted
            rejected.extend(c for c in batch if c not in accepted)
        stale = sorted(self.subscribed - self.desired)
        for batch in batched(stale, self.s.subscribe_batch_size):
            await self._call("public/unsubscribe", {"channels": batch})
            self.subscribed -= set(batch)
        if rejected:
            at = now_ms()
            self.snap.pending_gaps.append(
                gap_row(at, at, "subscribe_rejected", len(rejected), ",".join(rejected[:50]))
            )
            log("ERROR", event="subscribe_rejected", channels=len(rejected))
        if wanted or stale:
            log(
                "INFO",
                event="subscriptions",
                added=len(wanted) - len(rejected),
                removed=len(stale),
                total=len(self.subscribed),
            )

    async def _connection_loop(self) -> None:
        backoff = self.s.backoff_initial_s
        while True:
            reason = "connection_lost:closed"
            try:
                async with self.http.ws_connect(
                    self.s.ws_url, max_msg_size=2**24, heartbeat=None
                ) as ws:
                    self._ws = ws
                    self.subscribed = set()
                    reader = asyncio.create_task(self._read(ws))
                    try:
                        await self._call(
                            "public/set_heartbeat", {"interval": self.s.heartbeat_interval_s}
                        )
                        await self._sync_subscriptions()
                        gap = self.snap.connection.resubscribed(now_ms(), len(self.subscribed))
                        if gap is not None:
                            self.snap.pending_gaps.append(gap)
                            log(
                                "WARN",
                                event="gap_closed",
                                reason=gap["reason"],
                                seconds=round((gap["gap_end"] - gap["gap_start"]) / 1000, 1),
                            )
                        backoff = self.s.backoff_initial_s
                        await reader
                    finally:
                        reader.cancel()
                        with contextlib.suppress(BaseException):
                            await reader
            except ConnectionLost as lost:
                reason = lost.reason
            except (aiohttp.ClientError, TimeoutError, OSError) as error:
                reason = f"connection_lost:{type(error).__name__}"
            except Exception as error:  # noqa: BLE001 - an odd message must cost a reconnect, not the process
                reason = f"connection_lost:unexpected_{type(error).__name__}"
                log("ERROR", event="unexpected_error", error=f"{type(error).__name__}: {error}")
            finally:
                self._ws = None
                # set_exception, not cancel: a cancelled future would raise CancelledError
                # inside the refresh task and take the whole collector down with it.
                for future in self._replies.values():
                    if not future.done():
                        future.set_exception(ConnectionLost("connection_lost:closed"))
                self._replies.clear()

            self.snap.connection.disconnected(self.last_heard_ms, reason)
            log("WARN", event="disconnected", reason=reason, retry_in_s=round(backoff, 1))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self.s.backoff_max_s)

    async def _read(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while True:
            try:
                message = await ws.receive(timeout=self.s.stale_after_s)
            except TimeoutError:
                raise ConnectionLost("stale_connection") from None
            if message.type in (
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.CLOSING,
            ):
                raise ConnectionLost(f"connection_lost:closed_{ws.close_code}")
            if message.type == aiohttp.WSMsgType.ERROR:
                raise ConnectionLost("connection_lost:ws_error")
            if message.type != aiohttp.WSMsgType.TEXT:
                continue

            recv = now_us()
            self.last_heard_ms = recv // 1000
            payload = json.loads(message.data)
            if "id" in payload:
                future = self._replies.get(payload["id"])
                if future is not None and not future.done():
                    future.set_result(payload)
                continue

            method = payload.get("method")
            if method == "heartbeat":
                if payload["params"]["type"] == "test_request":
                    await ws.send_str(
                        json.dumps({"jsonrpc": "2.0", "method": "public/test", "params": {}})
                    )
                continue
            if method != "subscription":
                continue

            channel = payload["params"]["channel"]
            data = payload["params"]["data"]
            if channel == self.s.index_channel:
                self.snap.book.on_message(data["index_name"], data, recv, is_index=True)
                continue
            symbol = data["instrument_name"]
            self.snap.book.on_message(symbol, data, recv)
            if not self.snap.gate.knows(symbol):
                self._request_refresh()

    # ---- flushing

    def _write(self, table: str, rows: list[dict]) -> None:
        batch = self._retry.pop(table, []) + rows
        if not batch:
            return
        try:
            self.sink.write(table, batch)
        except Exception as error:  # noqa: BLE001 - a failed write must not stop collection
            overflow = len(batch) - self.s.retry_buffer_max_rows
            if overflow > 0:
                batch = batch[overflow:]
                at = now_ms()
                self.snap.pending_gaps.append(gap_row(at, at, "sink_write_failed", overflow, table))
            self._retry[table] = batch
            log(
                "ERROR",
                event="sink_write_failed",
                table=table,
                buffered=len(batch),
                error=f"{type(error).__name__}: {error}",
            )

    def flush(self, snapshot_ms: int) -> None:
        started = time.monotonic()
        rows, gaps = self.snap.flush(snapshot_ms)
        self._write("l1_snapshots", rows)
        self._write("collector_gaps", gaps)
        sealed = self.sink.seal_before(_hour_start(snapshot_ms))
        self._state_file.write_text(str(snapshot_ms), encoding="utf-8")
        if self.snap.gate.waiting:
            self._request_refresh()
        log(
            "INFO",
            event="flush",
            snapshot=datetime.fromtimestamp(snapshot_ms / 1000, UTC).strftime("%H:%M"),
            rows=len(rows),
            gaps=len(gaps),
            waiting_meta=self.snap.gate.waiting,
            subscribed=len(self.subscribed),
            sealed=len(sealed),
            ms=round((time.monotonic() - started) * 1000),
        )

    async def _flush_loop(self) -> None:
        while True:
            current = now_ms()
            boundary = (current // MINUTE_MS + 1) * MINUTE_MS
            await asyncio.sleep((boundary - current + self.s.flush_grace_ms) / 1000)
            self.flush(boundary)


def _install_signal_handlers(task: asyncio.Task) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, task.cancel)


async def _main_async(settings: Settings, sink: Sink) -> None:
    task = asyncio.current_task()
    assert task is not None
    _install_signal_handlers(task)
    await Collector(settings, sink).run()


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--seal-and-exit",
        action="store_true",
        help="seal every staged hour, including the current partial one, then exit. "
        "Only for when the service is stopped for good.",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        run_config = load_config(args.config, required_keys=REQUIRED_CONFIG_KEYS)
        settings = Settings.from_config(run_config.values)
        sink = make_sink(require(run_config.values, "sink"))
    except (ConfigKeyError, ValueError, OSError) as error:
        print(f"deribit_l1_collector failed to start: {error}", file=sys.stderr)
        return 1

    if args.seal_and_exit:
        sealed = sink.seal_before(_hour_start(now_ms()) + HOUR_MS)
        log("INFO", event="sealed_and_exit", files=len(sealed))
        return 0

    log(
        "INFO",
        event="start",
        config_sha256=run_config.content_hash[:12],
        sink=require(run_config.values, "sink", "kind"),
        interval=settings.ticker_interval,
    )
    with contextlib.suppress(asyncio.CancelledError, KeyboardInterrupt):
        asyncio.run(_main_async(settings, sink))
    log("INFO", event="stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
