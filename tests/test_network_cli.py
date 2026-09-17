"""Tests for network CLI subcommands (fetch-network, promote-network, and cross-domain query)."""

import json
from pathlib import Path
from typing import Any

import httpx

from bitcoin_data_platform.cli import main
from bitcoin_data_platform.sources.coin_metrics_client import CoinMetricsClient
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager


def _mock_payload(start: str = "2026-01-01", end: str = "2026-01-02") -> dict[str, Any]:
    return {
        "data": [
            {
                "asset": "btc",
                "time": f"{start}T00:00:00.000000000Z",
                "TxCnt": "345000",
                "AdrActCnt": "850000",
            },
            {
                "asset": "btc",
                "time": f"{end}T00:00:00.000000000Z",
                "TxCnt": "360000",
                "AdrActCnt": "880000",
            },
        ]
    }


def test_fetch_network_cli_success(tmp_path: Path, capsys: Any) -> None:
    """1. fetch-network saves raw gzip envelope and prints JSON summary."""
    payload = _mock_payload("2026-01-01", "2026-01-02")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload, headers={"x-request-id": "req-net-1"})

    client = CoinMetricsClient(transport=httpx.MockTransport(handler))
    output_dir = tmp_path / "raw" / "coin_metrics"
    db_path = tmp_path / "state" / "platform.duckdb"

    argv = [
        "fetch-network",
        "--start",
        "2026-01-01",
        "--end",
        "2026-01-02",
        "--output-dir",
        str(output_dir),
        "--db-path",
        str(db_path),
    ]

    code = main(argv, network_client=client)
    assert code == 0

    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["status"] == "success"
    assert summary["records_ingested"] == 2
    assert len(summary["files_written"]) == 1

    written_file = Path(summary["files_written"][0])
    assert written_file.exists()
    assert written_file.name.endswith(".json.gz")


def test_fetch_network_cli_invalid_date_range(tmp_path: Path, capsys: Any) -> None:
    """2. fetch-network exits with code 2 when start date is after end date."""
    argv = [
        "fetch-network",
        "--start",
        "2026-01-05",
        "--end",
        "2026-01-01",
        "--output-dir",
        str(tmp_path / "raw"),
        "--db-path",
        str(tmp_path / "state" / "platform.duckdb"),
    ]

    code = main(argv)
    assert code == 2
    captured = capsys.readouterr()
    assert "must not be after --end" in captured.err


def test_fetch_network_cli_source_unavailable(tmp_path: Path, capsys: Any) -> None:
    """3. fetch-network exits with code 3 when Coin Metrics API is unavailable."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="Service Unavailable")

    client = CoinMetricsClient(
        transport=httpx.MockTransport(handler),
        max_retries=2,
        sleeper=lambda _: None,
        jitter=False,
    )

    argv = [
        "fetch-network",
        "--start",
        "2026-01-01",
        "--end",
        "2026-01-02",
        "--output-dir",
        str(tmp_path / "raw"),
        "--db-path",
        str(tmp_path / "state" / "platform.duckdb"),
    ]

    code = main(argv, network_client=client)
    assert code == 3
    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["status"] == "failure"


def test_fetch_network_cli_contract_violation(tmp_path: Path, capsys: Any) -> None:
    """4. fetch-network exits with code 4 when response violates contract."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "asset": "btc",
                        "time": "2026-01-01T00:00:00Z",
                        "TxCnt": "-100",  # Negative count
                        "AdrActCnt": "500",
                    }
                ]
            },
        )

    client = CoinMetricsClient(transport=httpx.MockTransport(handler))

    argv = [
        "fetch-network",
        "--start",
        "2026-01-01",
        "--end",
        "2026-01-01",
        "--output-dir",
        str(tmp_path / "raw"),
        "--db-path",
        str(tmp_path / "state" / "platform.duckdb"),
    ]

    code = main(argv, network_client=client)
    assert code == 4
    captured = capsys.readouterr()
    assert "Contract violation" in captured.err


def test_promote_network_cli_lifecycle(tmp_path: Path, capsys: Any) -> None:
    """5. End-to-end fetch, promote-network, and cross-domain query."""
    raw_dir = tmp_path / "raw" / "coin_metrics"
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    # Step 1: Fetch network data
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_mock_payload("2026-01-01", "2026-01-02"))

    client = CoinMetricsClient(transport=httpx.MockTransport(handler))
    fetch_code = main(
        [
            "fetch-network",
            "--start",
            "2026-01-01",
            "--end",
            "2026-01-02",
            "--output-dir",
            str(raw_dir),
            "--db-path",
            str(db_path),
        ],
        network_client=client,
    )
    assert fetch_code == 0
    # Clear stdout after step 1
    capsys.readouterr()

    # Step 2: Promote network data
    promote_code = main(
        [
            "promote-network",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
        ]
    )
    assert promote_code == 0

    captured = capsys.readouterr()
    promote_summary = json.loads(captured.out)
    assert promote_summary["status"] == "success"
    assert promote_summary["rows_promoted"] == 2
    assert promote_summary["partitions_written"] == 1
    assert promote_summary["watermark_utc"] == "2026-01-02T00:00:00Z"

    # Step 3: Query DuckDB view fact_network_metrics_daily
    db_manager = DuckDBManager(db_path=db_path, curated_dir=curated_dir)
    res = db_manager.execute_query(
        "SELECT transaction_count, active_addresses_count "
        "FROM fact_network_metrics_daily ORDER BY metric_date_utc;"
    )
    assert len(res) == 2
    assert res[0]["transaction_count"] == 345000
    assert res[1]["active_addresses_count"] == 880000

    # Step 4: Query cross-domain mart view via CLI query
    query_code = main(
        [
            "query",
            "--db-path",
            str(db_path),
            "--sql",
            "SELECT trade_date_utc, transaction_count "
            "FROM mart_btc_market_and_network_daily ORDER BY trade_date_utc;",
        ]
    )
    assert query_code == 0


def test_promote_network_no_envelopes(tmp_path: Path, capsys: Any) -> None:
    """6. promote-network with empty raw dir returns 0 rows promoted."""
    raw_dir = tmp_path / "empty_raw"
    raw_dir.mkdir(parents=True)
    curated_dir = tmp_path / "curated"
    db_path = tmp_path / "state" / "platform.duckdb"

    code = main(
        [
            "promote-network",
            "--raw-dir",
            str(raw_dir),
            "--curated-dir",
            str(curated_dir),
            "--db-path",
            str(db_path),
        ]
    )
    assert code == 0
    captured = capsys.readouterr()
    summary = json.loads(captured.out)
    assert summary["rows_promoted"] == 0
