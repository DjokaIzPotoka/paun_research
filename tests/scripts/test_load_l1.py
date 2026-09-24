"""Loader idempotency against a real ClickHouse.

Needs the local instance from scripts/load_l1.yaml; skipped when it is not
reachable (e.g. in CI). Uses database optrv_test and drops it afterwards.
"""

import json
from pathlib import Path

import pytest
import yaml

from scripts.deribit_l1_collector import MINUTE_MS, FileSink, Snapshotter, instrument_from_rest
from scripts.load_l1 import ClickHouse, ClickHouseError, load_directory, migrate

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "deribit_real.json").read_text())
T0 = 1_790_265_660_000


@pytest.fixture
def client():
    section = yaml.safe_load((ROOT / "scripts" / "load_l1.yaml").read_text())["clickhouse"]
    try:
        ch = ClickHouse.from_config(section, database="optrv_test")
        ch.execute("DROP DATABASE IF EXISTS optrv_test", use_database=False)
        migrate(ch)
    except (ValueError, OSError, ClickHouseError) as error:
        pytest.skip(f"local ClickHouse not available: {error}")
    yield ch
    ch.execute("DROP DATABASE IF EXISTS optrv_test", use_database=False)


def sealed_input(tmp_path: Path) -> Path:
    snap = Snapshotter()
    snap.gate.update(
        {
            item["instrument_name"]: instrument_from_rest(item, T0)[0]
            for item in FIXTURE["instruments"]
        }
    )
    sink = FileSink(tmp_path)
    hour = T0 - T0 % 3_600_000
    for minute in range(3):
        for message in FIXTURE["ws_messages"].values():
            data = message["params"]["data"]
            symbol = data.get("instrument_name", data.get("index_name"))
            snap.book.on_message(
                symbol,
                data,
                (hour + minute * MINUTE_MS + 5_000) * 1000,
                is_index="index_name" in data,
            )
        rows, _ = snap.flush(hour + (minute + 1) * MINUTE_MS)
        sink.write("l1_snapshots", rows)
    sink.seal_before(hour + 3_600_000)
    return tmp_path


def counts(ch: ClickHouse) -> tuple[int, int]:
    raw = int(ch.execute("SELECT count() FROM l1_snapshots").strip())
    final = int(ch.execute("SELECT count() FROM l1_snapshots FINAL").strip())
    return raw, final


def test_reloading_the_same_file_does_not_duplicate(client, tmp_path):
    root = sealed_input(tmp_path)

    first = load_directory(client, root, force=False)
    assert first["loaded"] == 1 and first["rows"] == 12
    assert counts(client) == (12, 12)

    second = load_directory(client, root, force=False)
    assert second == {"loaded": 0, "skipped": 1, "rows": 0}

    # --force bypasses the manifest; the dedup token and ReplacingMergeTree still hold.
    forced = load_directory(client, root, force=True)
    assert forced["loaded"] == 1
    assert counts(client) == (12, 12)


def test_migrations_are_idempotent(client):
    assert migrate(client) == []
