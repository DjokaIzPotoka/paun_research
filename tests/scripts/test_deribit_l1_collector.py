import copy
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.deribit_l1_collector import (
    L1_SCHEMA,
    MINUTE_MS,
    ConnectionGaps,
    FileSink,
    Snapshotter,
    batched,
    instrument_from_rest,
    main,
    symbol_agrees,
)

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "deribit_real.json").read_text())
MESSAGES = {
    ch.split(".")[1] if ch.startswith("ticker.") else "btc_usd": m["params"]["data"]
    for ch, m in FIXTURE["ws_messages"].items()
}
FETCHED_MS = 1_790_265_600_000
METAS = {
    item["instrument_name"]: instrument_from_rest(item, FETCHED_MS)[0]
    for item in FIXTURE["instruments"]
}
OPTION, FUTURE, PERP = "BTC-25SEP26-30000-C", "BTC-25SEP26", "BTC-PERPETUAL"

T0 = 1_790_265_660_000  # a full minute, UTC


def snapshotter(metas=METAS) -> Snapshotter:
    snap = Snapshotter()
    snap.gate.update(dict(metas))
    return snap


def feed(snap: Snapshotter, symbol: str, at_ms: int) -> None:
    data = MESSAGES[symbol]
    snap.book.on_message(
        data.get("instrument_name", data.get("index_name")),
        data,
        at_ms * 1000,
        is_index=symbol == "btc_usd",
    )


def by_symbol(rows):
    return {row["symbol"]: row for row in rows}


# ---------------------------------------------------------------- parsing a real ticker message


def test_option_ticker_row_copies_venue_values_without_conversion():
    snap = snapshotter()
    feed(snap, OPTION, T0 - 5_000)
    rows, gaps = snap.flush(T0)

    row = by_symbol(rows)[OPTION]
    data = MESSAGES[OPTION]
    item = next(i for i in FIXTURE["instruments"] if i["instrument_name"] == OPTION)
    assert gaps == []
    assert row["snapshot_ts"] == T0
    assert row["quote_age_ms"] == 5_000
    assert row["venue_ts"] == data["timestamp"]
    assert row["bid_price"] == data["best_bid_price"]
    assert row["ask_price"] == data["best_ask_price"]
    assert row["bid_amount"] == data["best_bid_amount"]
    assert row["ask_amount"] == data["best_ask_amount"]
    assert (row["bid_iv"], row["ask_iv"], row["mark_iv"]) == (
        data["bid_iv"],
        data["ask_iv"],
        data["mark_iv"],
    )
    assert row["mark_price"] == data["mark_price"]
    assert row["underlying_price"] == data["underlying_price"]
    assert row["index_price"] == data["index_price"]
    assert row["open_interest"] == data["open_interest"]
    assert row["delta"] == data["greeks"]["delta"]
    assert row["theta"] == data["greeks"]["theta"]
    assert row["interest_rate"] == data["interest_rate"]
    assert row["kind"] == "option"
    assert row["option_type"] == item["option_type"]
    assert row["strike"] == item["strike"]
    assert row["expiration"] == item["expiration_timestamp"]
    assert row["meta_mismatch"] is False
    assert row["instruments_fetched_at"] == FETCHED_MS


def test_perpetual_row_carries_funding_and_no_expiration():
    snap = snapshotter()
    feed(snap, PERP, T0 - 1_000)
    row = by_symbol(snap.flush(T0)[0])[PERP]

    assert row["funding_8h"] == MESSAGES[PERP]["funding_8h"]
    assert row["current_funding"] == MESSAGES[PERP]["current_funding"]
    assert row["expiration"] is None
    assert row["bid_iv"] is None and row["delta"] is None
    assert row["meta_mismatch"] is False


def test_index_row():
    snap = snapshotter()
    feed(snap, "btc_usd", T0 - 2_000)
    row = by_symbol(snap.flush(T0)[0])["btc_usd"]

    assert row["kind"] == "index"
    assert row["index_price"] == MESSAGES["btc_usd"]["price"]
    assert row["bid_price"] is None


def test_rows_match_the_parquet_schema():
    snap = snapshotter()
    for symbol in (OPTION, FUTURE, PERP, "btc_usd"):
        feed(snap, symbol, T0 - 1_000)
    rows, _ = snap.flush(T0)
    table = pa.Table.from_pylist(rows, schema=L1_SCHEMA)
    assert table.num_rows == 4


# ---------------------------------------------------------------- snapshot semantics


def test_instrument_without_message_in_minute_is_not_written():
    snap = snapshotter()
    feed(snap, OPTION, T0 - 30_000)
    feed(snap, FUTURE, T0 - 20_000)
    first, _ = snap.flush(T0)

    feed(snap, OPTION, T0 + 10_000)
    second, _ = snap.flush(T0 + MINUTE_MS)

    assert set(by_symbol(first)) == {OPTION, FUTURE}
    assert set(by_symbol(second)) == {OPTION}


def test_last_message_in_minute_wins_and_boundary_message_goes_to_next_minute():
    snap = snapshotter()
    early, late = copy.deepcopy(MESSAGES[OPTION]), copy.deepcopy(MESSAGES[OPTION])
    early["best_bid_price"], late["best_bid_price"] = 0.1, 0.2
    snap.book.on_message(OPTION, early, (T0 - 40_000) * 1000)
    snap.book.on_message(OPTION, late, (T0 - 1) * 1000)
    after = copy.deepcopy(MESSAGES[OPTION])
    after["best_bid_price"] = 0.3
    snap.book.on_message(OPTION, after, T0 * 1000)

    first = by_symbol(snap.flush(T0)[0])[OPTION]
    second = by_symbol(snap.flush(T0 + MINUTE_MS)[0])[OPTION]
    assert first["bid_price"] == 0.2
    assert second["bid_price"] == 0.3


# ---------------------------------------------------------------- metadata rules


def test_unknown_symbol_is_held_one_minute_and_written_once_metadata_arrives():
    snap = snapshotter({k: v for k, v in METAS.items() if k != OPTION})
    feed(snap, OPTION, T0 - 5_000)
    rows, gaps = snap.flush(T0)
    assert OPTION not in by_symbol(rows) and gaps == []

    snap.gate.update(dict(METAS))
    rows, gaps = snap.flush(T0 + MINUTE_MS)
    held = by_symbol(rows)[OPTION]
    assert held["snapshot_ts"] == T0
    assert held["strike"] is not None
    assert gaps == []


def test_metadata_still_missing_next_minute_records_missing_metadata_gaps():
    snap = snapshotter({k: v for k, v in METAS.items() if k != OPTION})
    feed(snap, OPTION, T0 - 5_000)
    snap.flush(T0)
    feed(snap, OPTION, T0 + 5_000)
    rows, gaps = snap.flush(T0 + MINUTE_MS)

    assert OPTION not in by_symbol(rows)
    assert [(g["gap_start"], g["gap_end"], g["reason"]) for g in gaps] == [
        (T0 - MINUTE_MS, T0, "missing_metadata"),
        (T0, T0 + MINUTE_MS, "missing_metadata"),
    ]
    assert all(g["detail"] == OPTION and g["affected_instruments"] == 1 for g in gaps)


def test_symbol_that_disagrees_with_metadata_is_flagged():
    item = copy.deepcopy(next(i for i in FIXTURE["instruments"] if i["instrument_name"] == OPTION))
    item["strike"] = item["strike"] + 1000
    meta, _ = instrument_from_rest(item, FETCHED_MS)
    assert meta.meta_mismatch is True
    assert meta.strike == item["strike"]  # the venue's value is used, not the parsed one


@pytest.mark.parametrize("symbol", [OPTION, FUTURE, PERP])
def test_real_symbols_agree_with_their_metadata(symbol):
    assert METAS[symbol].meta_mismatch is False


def test_symbol_agrees_handles_decimal_strikes():
    assert symbol_agrees("BTC-25SEP26-62500d5-P", "option", 62500.5, "put", 1_790_323_200_000)


# ---------------------------------------------------------------- gaps


def test_reconnect_writes_a_collector_gaps_row():
    snap = snapshotter()
    snap.connection.disconnected(T0 + 1_000, "connection_lost:ClientConnectionError")
    snap.connection.disconnected(T0 + 3_000, "connection_lost:ClientConnectionError")
    gap = snap.connection.resubscribed(T0 + 9_000, affected=1093)
    snap.pending_gaps.append(gap)

    _, gaps = snap.flush(T0 + MINUTE_MS)
    assert len(gaps) == 1
    assert gaps[0]["gap_start"] == T0 + 1_000  # last message heard, not the second failure
    assert gaps[0]["gap_end"] == T0 + 9_000
    assert gaps[0]["reason"] == "connection_lost:ClientConnectionError"
    assert gaps[0]["affected_instruments"] == 1093
    assert gaps[0]["detail"] == "reconnect_attempts=2"


def test_resubscribe_without_disconnect_records_nothing():
    assert ConnectionGaps().resubscribed(T0, affected=10) is None


# ---------------------------------------------------------------- file sink


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_sealed_hour_file_is_never_modified(tmp_path):
    sink = FileSink(tmp_path)
    hour = T0 - T0 % 3_600_000
    snap = snapshotter()
    feed(snap, OPTION, hour + 30_000)
    first_rows, _ = snap.flush(hour + MINUTE_MS)
    feed(snap, FUTURE, hour + 90_000)
    second_rows, _ = snap.flush(hour + 2 * MINUTE_MS)
    sink.write("l1_snapshots", first_rows)
    sink.write("l1_snapshots", second_rows)

    assert sink.seal_before(hour) == []  # hour not over yet: nothing sealed
    sealed = sink.seal_before(hour + 3_600_000)
    assert len(sealed) == 1
    final = sealed[0]
    assert pq.read_table(final).num_rows == 2
    before = (_sha(final), final.stat().st_mtime_ns)

    feed(snap, PERP, hour + 150_000)
    late_rows, _ = snap.flush(hour + 3 * MINUTE_MS)
    sink.write("l1_snapshots", late_rows)
    assert sink.seal_before(hour + 3_600_000) == []

    assert (_sha(final), final.stat().st_mtime_ns) == before
    late = [p for p in final.parent.glob("*.late-*.parquet")]
    assert len(late) == 1 and pq.read_table(late[0]).num_rows == 1
    assert not (tmp_path / "l1_snapshots" / ".staging").exists() or not any(
        (tmp_path / "l1_snapshots" / ".staging").rglob("*.parquet")
    )


def test_seal_dedupes_a_minute_written_twice(tmp_path):
    sink = FileSink(tmp_path)
    hour = T0 - T0 % 3_600_000
    snap = snapshotter()
    feed(snap, OPTION, hour + 30_000)
    rows, _ = snap.flush(hour + MINUTE_MS)
    sink.write("l1_snapshots", rows)
    sink.write("l1_snapshots", rows)  # e.g. a retry after a failed write that actually landed

    final = sink.seal_before(hour + 3_600_000)[0]
    assert pq.read_table(final).num_rows == 1


# ---------------------------------------------------------------- misc


def test_subscription_batches_respect_the_cap():
    channels = [f"ticker.X-{i}.agg2" for i in range(1093)]
    assert [len(b) for b in batched(channels, 500)] == [500, 500, 93]


def test_missing_config_parameter_refuses_to_start(tmp_path, capsys):
    source = Path(__file__).resolve().parents[2] / "scripts" / "deribit_l1_collector.yaml"
    text = source.read_text().replace("  flush_grace_ms: 250\n", "")
    config = tmp_path / "config.yaml"
    config.write_text(text)

    assert main(["--config", str(config)]) == 1
    assert "snapshot.flush_grace_ms" in capsys.readouterr().err
