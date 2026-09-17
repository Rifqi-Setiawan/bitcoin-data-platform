.PHONY: help install lint format typecheck test check build audit clean-dist clean

VENV ?= .venv
PYTHON = $(VENV)/bin/python
RUFF = $(VENV)/bin/ruff
MYPY = $(VENV)/bin/mypy
PYTEST = $(VENV)/bin/pytest
PIP_AUDIT ?= pip-audit

help:
	@echo "Available targets:"
	@echo "  install    - Install package in editable mode with dev dependencies"
	@echo "  lint       - Check code formatting and linting with ruff"
	@echo "  format     - Auto-format code with ruff"
	@echo "  typecheck  - Run static type checks with mypy"
	@echo "  test       - Run test suite with pytest"
	@echo "  check      - Run all quality gates (lint, typecheck, test)"
	@echo "  build      - Build distribution packages (wheel and sdist)"
	@echo "  audit      - Audit dependencies for security vulnerabilities"
	@echo "  clean-dist - Remove build and distribution packaging artifacts"
	@echo "  clean      - Remove temporary, cache, and build files"

install:
	$(PYTHON) -m pip install -r requirements-dev.txt
	$(PYTHON) -m pip install -e .

lint:
	$(RUFF) check .
	$(RUFF) format --check .

format:
	$(RUFF) format .
	$(RUFF) check --fix .

typecheck:
	$(MYPY)

test:
	$(PYTEST)

check: lint typecheck test

build:
	$(PYTHON) -m build

audit:
	$(PIP_AUDIT)

clean-dist:
	rm -rf dist/ build/ *.egg-info src/*.egg-info

clean: clean-dist
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
