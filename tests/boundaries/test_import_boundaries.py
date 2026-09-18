"""Regression check that the four import-linter contracts in .importlinter
still exist under their expected names. The actual enforcement runs via
`make boundaries` (import-linter statically scanning research/ and scripts/);
this test only guards against the contract silently disappearing from config.

Each contract below was manually proven to fail by temporarily inserting the
noted import into the real module it constrains, running `make boundaries`,
observing a BROKEN contract, and then reverting the import before commit.
"""

import configparser
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parents[2] / ".importlinter"


def _contract_names() -> set[str]:
    parser = configparser.ConfigParser()
    parser.read(CONFIG_PATH)
    return {
        section.removeprefix("importlinter:contract:")
        for section in parser.sections()
        if section.startswith("importlinter:contract:")
    }


def test_hypothesis_boundary_contract_exists():
    # Rule 1: research/hypothesis/* imports only research.state and
    # research.hypothesis.base.
    #
    # Verified by temporarily adding `import research.eval` to
    # research/hypothesis/base.py and running `make boundaries`:
    #   Broken contracts:
    #   research.hypothesis only depends on research.state and
    #   research.hypothesis.base
    # Reverted after confirming the failure.
    assert "hypothesis-boundary" in _contract_names()


def test_eval_boundary_contract_exists():
    # Rule 2: research/eval must not import research/hypothesis.
    #
    # Verified by temporarily adding `import research.hypothesis` to
    # research/eval/__init__.py and running `make boundaries`:
    #   Broken contracts:
    #   research.eval must not import research.hypothesis
    # Reverted after confirming the failure.
    assert "eval-no-hypothesis" in _contract_names()


def test_state_boundary_contract_exists():
    # Rule 3: research/state must not import research/data.
    #
    # Verified by temporarily adding `import research.data` to
    # research/state/__init__.py and running `make boundaries`:
    #   Broken contracts:
    #   research.state must not import research.data
    # Reverted after confirming the failure.
    assert "state-no-data" in _contract_names()


def test_scripts_boundary_contract_exists():
    # Rule 4: scripts/ imports research/, never the reverse.
    #
    # Verified by temporarily adding `import scripts` to
    # research/__init__.py and running `make boundaries`:
    #   Broken contracts:
    #   research must not import scripts (scripts depends on research,
    #   never the reverse)
    # Reverted after confirming the failure.
    assert "scripts-one-way" in _contract_names()
