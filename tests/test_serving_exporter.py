"""Tests for Research Serving Layer exporter and query service."""

import io
import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.ipc as paipc
import pyarrow.parquet as pq
import pytest

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.serving.exporter import export_arrow_table, export_data
from bitcoin_data_platform.serving.query_service import (
    QueryService,
    bind_named_parameters,
    execute_parameterized_query,
    load_query_file,
)
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


@pytest.fixture
def sample_arrow_table() -> pa.Table:
    """Create a sample PyArrow table with diverse types."""
    return pa.Table.from_pydict(
        {
            "id": [1, 2, 3],
            "asset": ["BTC", "BTC", "BTC"],
            "price": [Decimal("65000.50"), Decimal("66000.75"), Decimal("67000.00")],
            "volume": [Decimal("123.456"), Decimal("789.012"), None],
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("asset", pa.string()),
                pa.field("price", pa.decimal128(18, 2)),
                pa.field("volume", pa.decimal128(18, 3)),
            ]
        ),
    )


def test_export_in_memory_parquet(sample_arrow_table: pa.Table) -> None:
    """1. Export to parquet in-memory returns valid parquet bytes."""
    data = export_arrow_table(sample_arrow_table, fmt="parquet")
    assert isinstance(data, bytes)
    read_table = pq.read_table(io.BytesIO(data))
    assert read_table.num_rows == 3
    assert read_table.column_names == ["id", "asset", "price", "volume"]


def test_export_in_memory_arrow(sample_arrow_table: pa.Table) -> None:
    """2. Export to arrow IPC in-memory returns valid arrow bytes."""
    data = export_arrow_table(sample_arrow_table, fmt="arrow")
    assert isinstance(data, bytes)
    reader = paipc.open_file(io.BytesIO(data))
    read_table = reader.read_all()
    assert read_table.num_rows == 3
    assert read_table.column("id").to_pylist() == [1, 2, 3]


def test_export_in_memory_csv(sample_arrow_table: pa.Table) -> None:
    """3. Export to CSV in-memory returns valid RFC-4180 CSV bytes."""
    data = export_arrow_table(sample_arrow_table, fmt="csv")
    assert isinstance(data, bytes)
    csv_table = pacsv.read_csv(io.BytesIO(data))
    assert csv_table.num_rows == 3
    assert "asset" in csv_table.column_names


def test_export_in_memory_json(sample_arrow_table: pa.Table) -> None:
    """4. Export to JSON in-memory returns valid JSON bytes."""
    data = export_arrow_table(sample_arrow_table, fmt="json")
    assert isinstance(data, bytes)
    parsed = json.loads(data.decode("utf-8"))
    assert len(parsed) == 3
    assert parsed[0]["asset"] == "BTC"
    assert parsed[0]["id"] == 1


def test_export_to_file_atomic_parquet(sample_arrow_table: pa.Table, tmp_path: Path) -> None:
    """5. Atomic file export writes parquet and cleans up tmp files."""
    out_file = tmp_path / "exports" / "data.parquet"
    res = export_arrow_table(sample_arrow_table, fmt="parquet", output_path=out_file)
    assert res == out_file
    assert out_file.exists()

    # Ensure no lingering .tmp files
    tmp_files = list(out_file.parent.glob("*.tmp"))
    assert len(tmp_files) == 0

    read_table = pq.read_table(out_file)
    assert read_table.num_rows == 3


def test_export_to_file_atomic_arrow(sample_arrow_table: pa.Table, tmp_path: Path) -> None:
    """6. Atomic file export writes arrow IPC and replaces cleanly."""
    out_file = tmp_path / "data.arrow"
    res = export_arrow_table(sample_arrow_table, fmt="arrow", output_path=out_file)
    assert res == out_file
    assert out_file.exists()

    reader = paipc.open_file(out_file)
    assert reader.read_all().num_rows == 3


def test_export_to_file_atomic_csv(sample_arrow_table: pa.Table, tmp_path: Path) -> None:
    """7. Atomic file export writes CSV file."""
    out_file = tmp_path / "data.csv"
    res = export_arrow_table(sample_arrow_table, fmt="csv", output_path=out_file)
    assert res == out_file
    assert out_file.exists()
    content = out_file.read_text(encoding="utf-8")
    assert "id,asset,price,volume" in content or "id" in content


def test_export_to_file_atomic_json(sample_arrow_table: pa.Table, tmp_path: Path) -> None:
    """8. Atomic file export writes JSON file."""
    out_file = tmp_path / "data.json"
    res = export_arrow_table(sample_arrow_table, fmt="json", output_path=out_file)
    assert res == out_file
    assert out_file.exists()
    parsed = json.loads(out_file.read_text(encoding="utf-8"))
    assert len(parsed) == 3


def test_export_alias_export_data(sample_arrow_table: pa.Table, tmp_path: Path) -> None:
    """9. export_data is an alias of export_arrow_table."""
    out_file = tmp_path / "alias.json"
    res = export_data(sample_arrow_table, fmt="json", output_path=out_file)
    assert res == out_file
    assert out_file.exists()


def test_export_unsupported_format(sample_arrow_table: pa.Table) -> None:
    """10. Unsupported format raises ValueError."""
    with pytest.raises(ValueError, match="Unsupported export format: 'xml'"):
        export_arrow_table(sample_arrow_table, fmt="xml")


def test_export_empty_table(tmp_path: Path) -> None:
    """11. Exporting an empty table succeeds across all formats."""
    schema = pa.schema([pa.field("val", pa.int64()), pa.field("name", pa.string())])
    empty_table = pa.Table.from_pylist([], schema=schema)

    for fmt in ["parquet", "arrow", "csv", "json"]:
        data = export_arrow_table(empty_table, fmt=fmt)
        assert isinstance(data, bytes)
        file_path = tmp_path / f"empty.{fmt}"
        res = export_arrow_table(empty_table, fmt=fmt, output_path=file_path)
        assert isinstance(res, Path)
        assert res.exists()


def test_export_cleanup_on_failure(sample_arrow_table: pa.Table, tmp_path: Path) -> None:
    """12. If writing fails, temporary file is cleaned up."""
    out_file = tmp_path / "fail.parquet"

    with (
        patch("pyarrow.parquet.write_table", side_effect=RuntimeError("disk full")),
        pytest.raises(RuntimeError, match="disk full"),
    ):
        export_arrow_table(sample_arrow_table, fmt="parquet", output_path=out_file)

    assert not out_file.exists()
    tmp_files = list(tmp_path.glob("*.tmp"))
    assert len(tmp_files) == 0


def test_load_query_file(tmp_path: Path) -> None:
    """13. load_query_file reads file or raises FileNotFoundError."""
    q_file = tmp_path / "test.sql"
    q_file.write_text("SELECT 42 AS answer;", encoding="utf-8")
    sql = load_query_file(q_file)
    assert sql == "SELECT 42 AS answer;"

    with pytest.raises(FileNotFoundError, match="Query file not found"):
        load_query_file(tmp_path / "nonexistent.sql")


def test_bind_named_parameters() -> None:
    """14. bind_named_parameters converts :param to $param and preserves ::CAST."""
    sql = "SELECT id, price::DOUBLE FROM t WHERE asset = :asset AND trade_date >= :start_date"
    prepared, cleaned = bind_named_parameters(sql, {":asset": "BTC", "start_date": "2026-01-01"})
    expected_sql = (
        "SELECT id, price::DOUBLE FROM t WHERE asset = $asset AND trade_date >= $start_date"
    )
    assert prepared == expected_sql
    assert cleaned == {"asset": "BTC", "start_date": "2026-01-01"}


def test_bind_named_parameters_invalid_key() -> None:
    """15. Invalid parameter identifier raises ValueError."""
    with pytest.raises(ValueError, match="Invalid parameter key"):
        bind_named_parameters("SELECT 1", {"invalid-key!": "val"})


def test_execute_parameterized_query_and_service(tmp_path: Path) -> None:
    """16. Execute parameterized query and QueryService against DuckDB."""
    db_path = tmp_path / "test.duckdb"
    manager = DuckDBManager(db_path=db_path, curated_dir=tmp_path / "curated")
    with manager:
        con = manager.get_connection()
        con.execute("CREATE TABLE items (id INT, asset VARCHAR, val DOUBLE)")
        con.execute("INSERT INTO items VALUES (1, 'BTC', 100.0), (2, 'ETH', 200.0)")

        # Direct function
        tbl = execute_parameterized_query(
            manager,
            "SELECT * FROM items WHERE asset = :asset AND val >= :min_val",
            {"asset": "BTC", "min_val": 50.0},
        )
        assert tbl.num_rows == 1
        assert tbl.column("asset")[0].as_py() == "BTC"

        # Service class
        service = QueryService(manager)
        tbl_service = service.execute(
            "SELECT COUNT(*) AS c FROM items WHERE asset = :asset",
            {"asset": "BTC"},
        )
        assert tbl_service.column("c")[0].as_py() == 1

        # Service execute_file
        sql_file = tmp_path / "count.sql"
        sql_file.write_text("SELECT id FROM items WHERE id = :id", encoding="utf-8")
        tbl_file = service.execute_file(sql_file, {"id": 2})
        assert tbl_file.num_rows == 1
        assert tbl_file.column("id")[0].as_py() == 2


def test_parameter_injection_safety(tmp_path: Path) -> None:
    """17. Parameters are passed safely without SQL injection."""
    db_path = tmp_path / "test.duckdb"
    manager = DuckDBManager(db_path=db_path, curated_dir=tmp_path / "curated")
    with manager:
        con = manager.get_connection()
        con.execute("CREATE TABLE users (username VARCHAR, secret VARCHAR)")
        con.execute("INSERT INTO users VALUES ('admin', 'pass123'), ('guest', 'guest123')")

        # Attempt SQL injection via parameter value
        injection_payload = "admin' OR '1'='1"
        tbl = execute_parameterized_query(
            manager,
            "SELECT * FROM users WHERE username = :user",
            {"user": injection_payload},
        )
        # Should find zero records because value is treated literally
        assert tbl.num_rows == 0


def test_cli_query_with_file_and_params(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """18. CLI query command with --file, --param, and --format json."""
    db_path = tmp_path / "test.duckdb"
    manager = DuckDBManager(db_path=db_path, curated_dir=tmp_path / "curated")
    with manager:
        con = manager.get_connection()
        con.execute("CREATE TABLE data (name VARCHAR, score INT)")
        con.execute("INSERT INTO data VALUES ('alpha', 95), ('beta', 80)")

    query_file = tmp_path / "filter.sql"
    query_file.write_text("SELECT * FROM data WHERE score >= :min_score", encoding="utf-8")

    exit_code = main(
        [
            "query",
            "--db-path",
            str(db_path),
            "--file",
            str(query_file),
            "--param",
            "min_score=90",
            "--format",
            "json",
        ]
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    results = json.loads(captured.out)
    assert len(results) == 1
    assert results[0]["name"] == "alpha"


def test_cli_query_export_to_file(tmp_path: Path) -> None:
    """19. CLI query command exporting to parquet file via --output."""
    db_path = tmp_path / "test.duckdb"
    manager = DuckDBManager(db_path=db_path, curated_dir=tmp_path / "curated")
    with manager:
        con = manager.get_connection()
        con.execute("CREATE TABLE tbl (x INT)")
        con.execute("INSERT INTO tbl VALUES (10), (20), (30)")

    out_parquet = tmp_path / "output.parquet"
    exit_code = main(
        [
            "query",
            "--db-path",
            str(db_path),
            "--sql",
            "SELECT * FROM tbl ORDER BY x ASC",
            "--format",
            "parquet",
            "--output",
            str(out_parquet),
        ]
    )
    assert exit_code == 0
    assert out_parquet.exists()
    tbl = pq.read_table(out_parquet)
    assert tbl.num_rows == 3


def test_cli_query_error_handling(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """20. CLI query invalid parameter format or missing files returns error."""
    db_path = tmp_path / "test.duckdb"
    manager = DuckDBManager(db_path=db_path, curated_dir=tmp_path / "curated")
    with manager:
        pass

    # Invalid parameter format (no '=')
    exit_code = main(
        [
            "query",
            "--db-path",
            str(db_path),
            "--sql",
            "SELECT 1",
            "--param",
            "bad_param_no_equal",
        ]
    )
    assert exit_code == 2
    assert "invalid param format" in capsys.readouterr().err

    # Non-existent query file
    exit_code_file = main(
        [
            "query",
            "--db-path",
            str(db_path),
            "--file",
            str(tmp_path / "missing.sql"),
        ]
    )
    assert exit_code_file == 2
    assert "query file not found" in capsys.readouterr().err
