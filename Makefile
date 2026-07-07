# Local dev commands — these mirror .github/workflows/ci.yml exactly, so
# `make check` reproduces CI locally (fix/prevent failures before pushing).
.PHONY: install test lint fmt fmt-check check

install:
	python -m pip install -e ".[dev]"

test:
	python -m pytest -q

lint:
	python -m ruff check .

fmt:
	python -m ruff format .

fmt-check:
	python -m ruff format --check .

# Everything CI runs, in the same order.
check: lint fmt-check test
