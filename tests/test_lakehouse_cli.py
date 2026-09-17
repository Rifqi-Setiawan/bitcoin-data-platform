"""CLI integration tests for the bitcoin-data lakehouse subcommand."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.lakehouse.models import DEFAULT_TRADES_SCHEMA


@pytest.fixture
def catalog_dir(tmp_path: Path) -> Path:
    """Provide a temporary catalog directory."""
    return tmp_path / "lakehouse_catalog"


def _create_sample_jsonl(path: Path, count: int, product_id: str = "BTC-USD") -> None:
    """Helper to write sample JSON Lines events."""
    now = datetime.now(UTC).isoformat()
    with open(path, "w", encoding="utf-8") as f:
        for i in range(count):
            line = {
                "source": "coinbase_exchange",
                "product_id": product_id,
                "trade_id": 100 + i,
                "sequence": 5000 + i,
                "price": "67123.45",
                "size": "0.10",
                "side": "buy",
                "time_utc": now,
                "ingested_at_utc": now,
            }
            f.write(json.dumps(line) + "\n")


def test_cli_lakehouse_init_success(catalog_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Verify bitcoin-data lakehouse init registers a new table and exits 0."""
    cmd = [
        "lakehouse",
        "init",
        "--table",
        "trades",
        "--catalog-dir",
        str(catalog_dir),
    ]
    exit_code = main(cmd)
    assert exit_code == 0

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["status"] == "created"
    assert payload["table_name"] == "trades"
    assert "location" in payload


def test_cli_lakehouse_init_duplicate_fails(
    catalog_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify running init twice for same table returns error exit code 2."""
    cmd = [
        "lakehouse",
        "init",
        "--table",
        "trades",
        "--catalog-dir",
        str(catalog_dir),
    ]
    assert main(cmd) == 0

    exit_code_dup = main(cmd)
    assert exit_code_dup == 2
    captured = capsys.readouterr()
    assert "already exists" in captured.err


def test_cli_lakehouse_write_jsonl(
    catalog_dir: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify writing jsonl file to lakehouse table commits snapshot and exits 0."""
    main(["lakehouse", "init", "--table", "trades", "--catalog-dir", str(catalog_dir)])
    capsys.readouterr()

    jsonl_path = tmp_path / "batch.jsonl"
    _create_sample_jsonl(jsonl_path, count=8)

    write_cmd = [
        "lakehouse",
        "write",
        "--table",
        "trades",
        "--input-file",
        str(jsonl_path),
        "--catalog-dir",
        str(catalog_dir),
    ]
    assert main(write_cmd) == 0

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["status"] == "committed"
    assert payload["snapshot_id"] == 1
    assert payload["added_records"] == 8
    assert payload["total_records"] == 8


def test_cli_lakehouse_write_parquet(
    catalog_dir: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify writing parquet file with multiple assets partitions correctly."""
    main(["lakehouse", "init", "--table", "trades", "--catalog-dir", str(catalog_dir)])
    capsys.readouterr()

    now = datetime.now(UTC)
    parquet_path = tmp_path / "batch.parquet"
    tbl = pa.table(
        {
            "source": ["coinbase_exchange", "coinbase_exchange"],
            "product_id": ["BTC-USD", "ETH-USD"],
            "trade_id": [1, 2],
            "sequence": [10, 11],
            "price": pa.array(
                [Decimal("67000.00"), Decimal("3500.00")],
                type=pa.decimal128(38, 18),
            ),
            "size": pa.array(
                [Decimal("0.5"), Decimal("2.0")],
                type=pa.decimal128(38, 18),
            ),
            "side": ["buy", "sell"],
            "time_utc": pa.array([now, now], type=pa.timestamp("us", tz="UTC")),
            "ingested_at_utc": pa.array([now, now], type=pa.timestamp("us", tz="UTC")),
        },
        schema=DEFAULT_TRADES_SCHEMA,
    )
    pq.write_table(tbl, parquet_path)

    write_cmd = [
        "lakehouse",
        "write",
        "--table",
        "trades",
        "--input-file",
        str(parquet_path),
        "--catalog-dir",
        str(catalog_dir),
    ]
    assert main(write_cmd) == 0

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["snapshot_id"] == 1
    assert payload["added_records"] == 2


def test_cli_lakehouse_write_missing_file_fails(
    catalog_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify writing non-existent input file returns exit code 2."""
    main(["lakehouse", "init", "--table", "trades", "--catalog-dir", str(catalog_dir)])

    cmd = [
        "lakehouse",
        "write",
        "--table",
        "trades",
        "--input-file",
        "/path/to/ghost.jsonl",
        "--catalog-dir",
        str(catalog_dir),
    ]
    assert main(cmd) == 2
    captured = capsys.readouterr()
    assert "does not exist" in captured.err


def test_cli_lakehouse_compact(
    catalog_dir: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify compact subcommand merges micro-batch files."""
    main(["lakehouse", "init", "--table", "trades", "--catalog-dir", str(catalog_dir)])

    # Ingest 3 batches
    for i in range(3):
        f = tmp_path / f"batch_{i}.jsonl"
        _create_sample_jsonl(f, count=4)
        main(
            [
                "lakehouse",
                "write",
                "--table",
                "trades",
                "--input-file",
                str(f),
                "--catalog-dir",
                str(catalog_dir),
            ]
        )

    capsys.readouterr()

    compact_cmd = [
        "lakehouse",
        "compact",
        "--table",
        "trades",
        "--target-size-mb",
        "128",
        "--catalog-dir",
        str(catalog_dir),
    ]
    assert main(compact_cmd) == 0

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["compacted_files_count"] == 3
    assert payload["new_files_count"] == 1
    assert payload["rows_processed"] == 12
    assert payload["new_snapshot_id"] == 4


def test_cli_lakehouse_time_travel_snapshot_and_time(
    catalog_dir: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify time-travel queries by snapshot ID and current head."""
    main(["lakehouse", "init", "--table", "trades", "--catalog-dir", str(catalog_dir)])

    f1 = tmp_path / "b1.jsonl"
    _create_sample_jsonl(f1, count=5)
    main(
        [
            "lakehouse",
            "write",
            "--table",
            "trades",
            "--input-file",
            str(f1),
            "--catalog-dir",
            str(catalog_dir),
        ]
    )

    f2 = tmp_path / "b2.jsonl"
    _create_sample_jsonl(f2, count=3)
    main(
        [
            "lakehouse",
            "write",
            "--table",
            "trades",
            "--input-file",
            str(f2),
            "--catalog-dir",
            str(catalog_dir),
        ]
    )

    capsys.readouterr()

    # Query snapshot 1
    tt_snap1 = [
        "lakehouse",
        "time-travel",
        "--table",
        "trades",
        "--as-of-snapshot",
        "1",
        "--catalog-dir",
        str(catalog_dir),
    ]
    assert main(tt_snap1) == 0
    payload_s1 = json.loads(capsys.readouterr().out)
    assert payload_s1["snapshot_id"] == 1
    assert payload_s1["rows_count"] == 5

    capsys.readouterr()

    # Query current snapshot
    tt_curr = [
        "lakehouse",
        "time-travel",
        "--table",
        "trades",
        "--catalog-dir",
        str(catalog_dir),
    ]
    assert main(tt_curr) == 0
    payload_curr = json.loads(capsys.readouterr().out)
    assert payload_curr["snapshot_id"] == 2
    assert payload_curr["rows_count"] == 8


def test_cli_lakehouse_vacuum(
    catalog_dir: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify vacuum subcommand executes and returns summary JSON."""
    main(["lakehouse", "init", "--table", "trades", "--catalog-dir", str(catalog_dir)])

    # Ingest 2 batches then compact
    for i in range(2):
        f = tmp_path / f"b_{i}.jsonl"
        _create_sample_jsonl(f, count=3)
        main(
            [
                "lakehouse",
                "write",
                "--table",
                "trades",
                "--input-file",
                str(f),
                "--catalog-dir",
                str(catalog_dir),
            ]
        )

    main(
        [
            "lakehouse",
            "compact",
            "--table",
            "trades",
            "--catalog-dir",
            str(catalog_dir),
        ]
    )

    capsys.readouterr()

    # Run vacuum dry-run
    vac_dry_cmd = [
        "lakehouse",
        "vacuum",
        "--table",
        "trades",
        "--retain-days",
        "0",
        "--dry-run",
        "--catalog-dir",
        str(catalog_dir),
    ]
    assert main(vac_dry_cmd) == 0
    dry_out = json.loads(capsys.readouterr().out)
    assert dry_out["dry_run"] is True
    assert dry_out["expired_snapshots_count"] == 2
    assert dry_out["deleted_files_count"] == 2

    capsys.readouterr()

    # Run vacuum real execution
    vac_real_cmd = [
        "lakehouse",
        "vacuum",
        "--table",
        "trades",
        "--retain-days",
        "0",
        "--catalog-dir",
        str(catalog_dir),
    ]
    assert main(vac_real_cmd) == 0
    real_out = json.loads(capsys.readouterr().out)
    assert real_out["dry_run"] is False
    assert real_out["deleted_files_count"] == 2


def test_cli_lakehouse_missing_subcommand_fails(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify running lakehouse without a subcommand returns exit 2."""
    assert main(["lakehouse"]) == 2
    captured = capsys.readouterr()
    assert "requires a subcommand" in captured.err
