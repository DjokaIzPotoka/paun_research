from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


class ConfigKeyError(ValueError):
    """A required nested config parameter is absent. No default is substituted (P-15)."""

    def __init__(self, dotted_key: str):
        super().__init__(
            f"config is missing required parameter {dotted_key!r}. "
            "No default value will be substituted; add it explicitly to the config."
        )
        self.dotted_key = dotted_key


def require(values: dict, *keys: str) -> Any:
    """Walk `keys` into nested config, failing with the dotted path of the first gap."""
    node: Any = values
    for depth, key in enumerate(keys):
        if not isinstance(node, dict) or key not in node:
            raise ConfigKeyError(".".join(keys[: depth + 1]))
        node = node[key]
    return node


class MissingConfigError(ValueError):
    """Raised when a run config is missing a required parameter.

    There is no fallback value: the component that needs the parameter
    must refuse to start rather than silently substitute a default.
    """

    def __init__(self, missing_keys: list[str], path: Path):
        keys = ", ".join(sorted(missing_keys))
        super().__init__(
            f"config {path} is missing required parameter(s): {keys}. "
            "No default value will be substituted; add them explicitly to the config."
        )
        self.missing_keys = missing_keys
        self.path = path


@dataclass(frozen=True, slots=True)
class RunConfig:
    values: dict
    content_hash: str
    path: Path


def load_config(path: Path, required_keys: Iterable[str]) -> RunConfig:
    """Load a run's config.yaml and hash its exact on-disk bytes.

    Fails loudly if any of `required_keys` is absent -- callers must pass
    the full set of parameters their component needs; there is no implicit
    default for a missing one.
    """
    path = Path(path)
    raw = path.read_bytes()
    content_hash = hashlib.sha256(raw).hexdigest()
    values = yaml.safe_load(raw) or {}
    if not isinstance(values, dict):
        raise ValueError(f"config {path} must contain a YAML mapping at the top level")

    missing = [key for key in required_keys if key not in values]
    if missing:
        raise MissingConfigError(missing, path)

    return RunConfig(values=values, content_hash=content_hash, path=path)


_REPORT_TEMPLATE = """# Run report: {run_id}

- config: `{config_path}`
- config sha256: `{content_hash}`

No domain logic has run yet (M0: skeleton, tools, and boundaries only).
"""


def write_report(run_config: RunConfig, runs_dir: Path) -> Path:
    run_id = run_config.values["run_id"]
    output_dir = Path(runs_dir) / str(run_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "report.md"
    report_path.write_text(
        _REPORT_TEMPLATE.format(
            run_id=run_id,
            config_path=run_config.path.as_posix(),
            content_hash=run_config.content_hash,
        ),
        encoding="utf-8",
    )
    return report_path


def _main() -> int:
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Load, hash, and report on a run config.yaml")
    parser.add_argument("config_path", type=Path)
    parser.add_argument(
        "--require",
        action="append",
        default=[],
        dest="required_keys",
        help="a required top-level config key (repeatable)",
    )
    parser.add_argument("--runs-dir", type=Path, default=Path("runs"))
    args = parser.parse_args()

    try:
        run_config = load_config(args.config_path, args.required_keys)
        report_path = write_report(run_config, args.runs_dir)
    except (MissingConfigError, ValueError, KeyError) as exc:
        print(f"config check failed: {exc}", file=sys.stderr)
        return 1

    print(f"config OK: {run_config.path} sha256={run_config.content_hash}")
    print(f"report written: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
