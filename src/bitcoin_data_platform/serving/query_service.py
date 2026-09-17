"""Parameterized query service for analytical serving over DuckDB."""

import re
from pathlib import Path
from typing import Any, cast

import pyarrow as pa

from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager, DuckDBManagerError

_PARAM_REGEX = re.compile(r"(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)")
_VALID_PARAM_KEY = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def load_query_file(file_path: Path | str) -> str:
    """Load SQL query text from file system."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Query file not found at: {path}")
    return path.read_text(encoding="utf-8")


def bind_named_parameters(
    sql: str, params: dict[str, Any] | None = None
) -> tuple[str, dict[str, Any]]:
    """Convert named parameter bindings (:param) to DuckDB native syntax ($param).

    Validates parameter names to prevent injection and preserves PostgreSQL/DuckDB
    type casting syntax (::TYPE).
    """
    cleaned_params: dict[str, Any] = {}
    if params:
        for k, v in params.items():
            norm_key = k.lstrip(":$")
            if not _VALID_PARAM_KEY.match(norm_key):
                raise ValueError(
                    f"Invalid parameter key '{k}'. Must be an alphanumeric identifier."
                )
            cleaned_params[norm_key] = v

    prepared_sql = _PARAM_REGEX.sub(r"$\1", sql)
    return prepared_sql, cleaned_params


def execute_parameterized_query(
    db_manager: DuckDBManager,
    sql: str,
    params: dict[str, Any] | None = None,
) -> pa.Table:
    """Execute a parameterized query against DuckDB and return the result as a PyArrow Table."""
    con = db_manager.get_connection()
    prepared_sql, cleaned_params = bind_named_parameters(sql, params)

    try:
        if cleaned_params:
            cursor = con.execute(prepared_sql, cleaned_params)
        else:
            cursor = con.execute(prepared_sql)

        if cursor.description is None:
            return pa.Table.from_pylist([])

        try:
            if hasattr(cursor, "to_arrow_table"):
                return cast(pa.Table, cursor.to_arrow_table())
            reader = cursor.arrow()
            if hasattr(reader, "read_all"):
                return cast(pa.Table, reader.read_all())
            return cast(pa.Table, reader)
        except Exception as exc:
            raise DuckDBManagerError(f"Failed to fetch Arrow table from query: {exc}") from exc
    except DuckDBManagerError:
        raise
    except Exception as exc:
        raise DuckDBManagerError(f"SQL execution error: {exc}") from exc


class QueryService:
    """Service for managing, loading, and executing analytical queries."""

    def __init__(self, db_manager: DuckDBManager) -> None:
        self.db_manager = db_manager

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> pa.Table:
        """Execute a parameterized SQL string and return an Arrow Table."""
        return execute_parameterized_query(self.db_manager, sql, params)

    def execute_file(self, file_path: Path | str, params: dict[str, Any] | None = None) -> pa.Table:
        """Load SQL from file, bind parameters, and execute returning an Arrow Table."""
        sql = load_query_file(file_path)
        return self.execute(sql, params)
