"""Load collector parquet files into the local ClickHouse (phase-0-edge-spike, section 2).

Input is a local copy of the collector's file sink (rsync'd from the VM). Only
sealed files are loaded: hourly `HH.parquet` and late `HH.late-*.parquet`,
never `.staging/` parts or `*.tmp`. Loading is idempotent three ways: files are
skipped by sha256 via the `load_manifest` table, each insert carries a
deduplication token derived from that hash, and the tables are
ReplacingMergeTree on their natural key.

Talks to ClickHouse over its HTTP interface with urllib; a parquet file is sent
as-is in an `INSERT ... FORMAT Parquet` body.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from research.report.config import ConfigKeyError, load_config, require

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations" / "optrv"
LOADABLE_TABLES = ("l1_snapshots", "collector_gaps", "instruments")
REQUIRED_CONFIG_KEYS = ["input_root", "clickhouse"]


class ClickHouseError(RuntimeError):
    pass


def log(level: str, **fields: object) -> None:
    stamp = datetime.now(UTC).strftime("%H:%M:%S.%f")[:-3]
    payload = " ".join(f"{key}={value}" for key, value in fields.items())
    stream = sys.stderr if level == "ERROR" else sys.stdout
    print(f"{stamp}  {level:<5}  load  {payload}", file=stream, flush=True)


class ClickHouse:
    """Minimal ClickHouse HTTP client. The password never appears in errors or logs."""

    def __init__(self, url: str, database: str, user: str, password: str, timeout: float):
        self.url = url.rstrip("/")
        self.database = database
        self._user = user
        self._password = password
        self.timeout = timeout

    @classmethod
    def from_config(cls, section: dict, database: str | None = None) -> ClickHouse:
        password_file = Path(require(section, "password_file"))
        if not password_file.exists():
            raise ValueError(f"clickhouse.password_file {password_file} does not exist")
        return cls(
            url=require(section, "url"),
            database=database or require(section, "database"),
            user=require(section, "user"),
            password=password_file.read_text(encoding="utf-8").strip(),
            timeout=float(require(section, "timeout_seconds")),
        )

    def _post(self, params: dict[str, str], body: bytes) -> str:
        request = urllib.request.Request(
            f"{self.url}/?{urllib.parse.urlencode(params)}",
            data=body,
            method="POST",
            headers={"X-ClickHouse-User": self._user, "X-ClickHouse-Key": self._password},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.read().decode("utf-8")
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace").strip()
            raise ClickHouseError(f"HTTP {error.code}: {detail[:500]}") from None

    def execute(self, sql: str, *, use_database: bool = True) -> str:
        params = {"database": self.database} if use_database else {}
        return self._post(params, sql.encode("utf-8"))

    def query_json(self, sql: str) -> list[dict]:
        text = self.execute(f"{sql} FORMAT JSONEachRow")
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    def insert_parquet(self, table: str, body: bytes, dedup_token: str | None = None) -> None:
        params = {"database": self.database, "query": f"INSERT INTO {table} FORMAT Parquet"}
        if dedup_token:
            params["insert_deduplication_token"] = dedup_token
        self._post(params, body)

    def insert_json_rows(self, table: str, rows: list[dict]) -> None:
        params = {"database": self.database, "query": f"INSERT INTO {table} FORMAT JSONEachRow"}
        body = "\n".join(json.dumps(row) for row in rows).encode("utf-8")
        self._post(params, body)


def utc_text(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def migrate(client: ClickHouse) -> list[str]:
    """Create the database and apply numbered migrations not yet recorded. Returns applied names."""
    client.execute(f"CREATE DATABASE IF NOT EXISTS {client.database}", use_database=False)
    files = sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.sql"))
    bootstrap, rest = files[0], files[1:]
    client.execute(bootstrap.read_text(encoding="utf-8"))
    done = {
        int(row["version"])
        for row in client.query_json("SELECT version FROM schema_migrations FINAL")
    }

    applied = []
    for path in rest:
        version = int(path.name[:3])
        if version in done:
            continue
        client.execute(path.read_text(encoding="utf-8"))
        client.insert_json_rows(
            "schema_migrations",
            [
                {
                    "version": version,
                    "name": path.stem,
                    "applied_at": utc_text(int(time.time() * 1000)),
                }
            ],
        )
        applied.append(path.stem)
    return applied


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sealed_files(root: Path) -> list[tuple[str, Path]]:
    """(table, path) for every sealed parquet file, in a stable order."""
    found = []
    for table in LOADABLE_TABLES:
        table_dir = root / table
        if not table_dir.is_dir():
            continue
        for path in sorted(table_dir.glob("date=*/*.parquet")):
            found.append((table, path))
    return found


def load_directory(client: ClickHouse, root: Path, force: bool) -> dict[str, int]:
    loaded = {
        (row["sha256"], row["table_name"])
        for row in client.query_json("SELECT sha256, table_name FROM load_manifest FINAL")
    }
    counts = {"loaded": 0, "skipped": 0, "rows": 0}

    for table, path in sealed_files(root):
        digest = sha256_of(path)
        relative = path.relative_to(root).as_posix()
        if not force and (digest, table) in loaded:
            counts["skipped"] += 1
            continue

        rows = pq.ParquetFile(path).metadata.num_rows
        started = time.monotonic()
        client.insert_parquet(table, path.read_bytes(), dedup_token=f"{table}:{digest}")
        client.insert_json_rows(
            "load_manifest",
            [
                {
                    "sha256": digest,
                    "table_name": table,
                    "source_path": relative,
                    "rows": rows,
                    "loaded_at": utc_text(int(time.time() * 1000)),
                }
            ],
        )
        counts["loaded"] += 1
        counts["rows"] += rows
        log(
            "INFO",
            event="file_loaded",
            table=table,
            file=relative,
            rows=rows,
            seconds=round(time.monotonic() - started, 2),
        )
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-send files already in load_manifest (table dedup still prevents duplicates)",
    )
    args = parser.parse_args()

    try:
        values = load_config(args.config, required_keys=REQUIRED_CONFIG_KEYS).values
        root = Path(require(values, "input_root"))
        client = ClickHouse.from_config(require(values, "clickhouse"))
        if not root.is_dir():
            raise ValueError(f"input_root {root} is not a directory")
    except (ConfigKeyError, ValueError, OSError) as error:
        print(f"load_l1 failed to start: {error}", file=sys.stderr)
        return 1

    try:
        applied = migrate(client)
        log("INFO", event="migrated", database=client.database, applied=",".join(applied) or "-")
        counts = load_directory(client, root, args.force)
    except (ClickHouseError, OSError) as error:
        log("ERROR", event="load_failed", error=str(error))
        return 1

    log("INFO", event="done", **counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
