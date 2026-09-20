"""Global pytest configuration and autouse fixtures for Bitcoin Data Platform test suite."""

from collections.abc import Generator
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolate_ambient_kill_switch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[None, None, None]:
    """Ensure tests never write to or read from the repository root's PAPER_KILL_SWITCH."""
    temp_kill_switch = tmp_path / "PAPER_KILL_SWITCH"
    monkeypatch.setenv("PAPER_KILL_SWITCH_PATH", str(temp_kill_switch))
    yield
