# zora-research

Python research engine for ZORA hypotheses.

**M0 status**: skeleton, tooling, and import boundaries only. No domain
logic, no pricing, no data-ingestion scripts — those land in later
milestones.

## Layout

```
research/
├── data/             # ingest, one schema
├── state/            # forward, iv, greeks, surface_fit -- clean, deterministic
├── eval/             # costs, markout, attribution, permutation, FDR, ceiling
├── report/           # one template -> run report (+ config loading/hashing)
└── hypothesis/
    ├── base.py       # Protocol every hypothesis implements
    └── options_rv/
scripts/              # research/ consumers; empty until a first script is needed
runs/                 # one directory per run; only config.yaml and report.md are committed
tests/
docs/
```

## Setup

```bash
make setup
```

Requires [uv](https://docs.astral.sh/uv/). Python version is pinned in
`pyproject.toml`.

## Commands

```bash
make setup       # uv sync
make test        # pytest
make lint        # ruff check
make boundaries  # import-linter, enforces .importlinter
make run         # load + hash runs/m0-smoke/config.yaml, write its report.md
```

Every target here is invoked by CI (`.github/workflows/ci.yml`). A target is
not added to this Makefile unless CI calls it.

## Import boundaries

Enforced by `.importlinter` / `make boundaries`:

1. `research/hypothesis/*` depends only on `research.state` and
   `research.hypothesis.base`.
2. `research/eval` never imports `research/hypothesis`.
3. `research/state` never imports `research/data` -- fitting and forward
   models operate on data passed to them; they don't know where it came from.
4. `scripts/` imports `research/`, never the reverse.

## Configuration

Each run has exactly one `config.yaml`, hashed on load
(`research/report/config.py`). There is no `.env` fallback and no default
value baked into code for a missing parameter: if a required key is absent,
the component refuses to start and names the missing key. The config's
sha256 is embedded in the run's `report.md` so a report can always be traced
back to the exact config that produced it -- which is also why `config.yaml`
is the one file under `runs/` that stays committed.

## Units

Prices and quantities are never bare floats. `research/state/money.py`
wraps an amount with its currency (`Money`); combining two `Money` values in
different currencies raises `CurrencyMismatchError` instead of silently
coercing. Converting currencies requires an explicit `FxRate` (rate, base,
quote, source, status) -- there is no implicit conversion path.

## Cleanliness rules

- No `utils/`, `common/`, or `helpers/`. A function lives in the module
  that first needs it, and only moves once a third call site from a
  different module asks for it.
- A Makefile target exists only if CI calls it.
- If you don't know where something belongs, stop and ask -- don't create
  a new folder to avoid the decision.
