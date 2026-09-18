.PHONY: setup test lint boundaries run

setup:
	uv sync --all-groups

test:
	uv run pytest

lint:
	uv run ruff check .

boundaries:
	uv run lint-imports

run:
	uv run python -m research.report.config runs/m0-smoke/config.yaml --require run_id --require seed
