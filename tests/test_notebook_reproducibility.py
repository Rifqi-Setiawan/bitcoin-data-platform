"""Tests validating clean-room research notebook reproducibility and hygiene."""

import json
from pathlib import Path
from typing import Any

NOTEBOOKS_DIR = Path(__file__).resolve().parent.parent / "notebooks"
NOTEBOOK_PATH = NOTEBOOKS_DIR / "bitcoin_research_baseline.ipynb"
README_PATH = NOTEBOOKS_DIR / "README.md"


def test_notebook_directory_and_readme() -> None:
    """1. Verify notebooks directory and README.md exist with setup instructions."""
    assert NOTEBOOKS_DIR.is_dir()
    assert README_PATH.is_file()

    content = README_PATH.read_text(encoding="utf-8")
    assert "source .venv/bin/activate" in content
    assert "mart_btc_market_and_network_daily" in content
    assert "bitcoin_research_baseline.ipynb" in content


def test_notebook_valid_json_structure() -> None:
    """2. Verify notebook parses as valid JSON with standard Jupyter v4 schema."""
    assert NOTEBOOK_PATH.is_file()
    raw_content = NOTEBOOK_PATH.read_text(encoding="utf-8")
    nb = json.loads(raw_content)

    assert isinstance(nb, dict)
    assert nb.get("nbformat") == 4
    assert "cells" in nb
    assert isinstance(nb["cells"], list)
    assert len(nb["cells"]) >= 5


def test_notebook_cells_structure_and_types() -> None:
    """3. Verify all cells have valid structure, types, and required fields."""
    nb = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))

    has_markdown = False
    has_code = False

    for cell in nb["cells"]:
        assert isinstance(cell, dict)
        cell_type = cell.get("cell_type")
        assert cell_type in ("markdown", "code")

        source = cell.get("source")
        assert isinstance(source, list | str)

        if cell_type == "markdown":
            has_markdown = True
        elif cell_type == "code":
            has_code = True
            assert "outputs" in cell
            assert isinstance(cell["outputs"], list)

    assert has_markdown, "Notebook must contain markdown documentation cells"
    assert has_code, "Notebook must contain executable code cells"


def test_notebook_no_hardcoded_secrets() -> None:
    """4. Verify notebook contains no secrets, API keys, or credentials."""
    raw_content = NOTEBOOK_PATH.read_text(encoding="utf-8").lower()

    forbidden_patterns = [
        "api_key =",
        "apikey =",
        "secret_key =",
        "password =",
        "passwd =",
        "bearer ",
        "authorization:",
        "private_key",
    ]

    for pattern in forbidden_patterns:
        msg = f"Potential secret pattern '{pattern}' detected in notebook"
        assert pattern not in raw_content, msg


def test_notebook_zero_embedded_etl_and_clean_serving() -> None:
    """5. Verify notebook strictly uses serving layer and contains no raw pipeline ETL code."""
    nb = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    code_cells: list[dict[str, Any]] = [c for c in nb["cells"] if c.get("cell_type") == "code"]

    all_code = "\n".join(
        "".join(c.get("source", [])) if isinstance(c.get("source"), list) else str(c.get("source"))
        for c in code_cells
    )

    # Must consume serving layer or DuckDB conformed mart
    assert "mart_btc_market_and_network_daily" in all_code
    assert "QueryService" in all_code or "DuckDBManager" in all_code

    # Must NOT contain raw ingestion, clients, or direct HTTP network requests
    forbidden_tokens = [
        "CoinbaseClient",
        "CoinMetricsClient",
        "requests.get",
        "httpx.get",
        "urllib.request",
        "write_raw_envelope",
        "write_network_raw_envelope",
        "plan_backfill",
    ]

    for token in forbidden_tokens:
        msg = f"Embedded pipeline ETL token '{token}' forbidden in research notebook"
        assert token not in all_code, msg


def test_notebook_clean_outputs() -> None:
    """6. Verify notebook cells are clean without bloated cached binaries."""
    nb = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))

    for i, cell in enumerate(nb["cells"]):
        if cell.get("cell_type") == "code":
            outputs = cell.get("outputs", [])
            for out in outputs:
                # Output text or data should not exceed 50KB per cell
                out_str = json.dumps(out)
                msg = f"Cell {i} has oversized cached output ({len(out_str)} bytes)"
                assert len(out_str) < 50_000, msg
