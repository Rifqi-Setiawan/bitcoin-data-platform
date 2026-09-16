.PHONY: help install lint format typecheck test check clean

VENV ?= .venv
PYTHON = $(VENV)/bin/python
RUFF = $(VENV)/bin/ruff
MYPY = $(VENV)/bin/mypy
PYTEST = $(VENV)/bin/pytest

help:
	@echo "Available targets:"
	@echo "  install    - Install package in editable mode with dev dependencies"
	@echo "  lint       - Check code formatting and linting with ruff"
	@echo "  format     - Auto-format code with ruff"
	@echo "  typecheck  - Run static type checks with mypy"
	@echo "  test       - Run test suite with pytest"
	@echo "  check      - Run all quality gates (lint, typecheck, test)"
	@echo "  clean      - Remove temporary and cache files"

install:
	uv pip install --python $(PYTHON) -r requirements-dev.txt
	uv pip install --python $(PYTHON) -e .

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

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
