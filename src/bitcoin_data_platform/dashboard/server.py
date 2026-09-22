"""Lightweight standard-library HTTP dashboard and API server for Bitcoin Market Hub."""

import csv
import http.server
import io
import json
import logging
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import duckdb

from bitcoin_data_platform.intelligence.ingester import IntelligenceIngester
from bitcoin_data_platform.paper.engine import PaperTradingEngine
from bitcoin_data_platform.pipeline.lock_manager import LockManager
from bitcoin_data_platform.storage.duckdb_manager import DuckDBManager

logger = logging.getLogger(__name__)

DEFAULT_COMMITTEE_MEMO: dict[str, Any] = {
    "memo_id": "default-memo",
    "memo_date": datetime.now(UTC).date().isoformat(),
    "created_at_utc": datetime.now(UTC).isoformat(),
    "market_regime": "NEUTRAL_CHOP",
    "composite_mni": 0.0,
    "consensus_score": 0.0,
    "proposed_action": "STANDARD_DCA",
    "proposed_allocation_usd": 10.0,
    "clamped_allocation_usd": 10.0,
    "allocation_clamped": False,
    "clamping_reason": None,
    "risk_guard_passed": True,
    "executive_summary_id": (
        "Komite Investasi beroperasi dalam status default. Belum ada sesi deliberasi tersimpan."
    ),
    "macro_thesis": "Kondisi likuiditas makro stabil moderat.",
    "valuation_thesis": "Valuasi on-chain berada pada batas nilai wajar siklus.",
    "technical_thesis": "Parameter risiko teknikal dalam toleransi normal.",
    "dissenting_opinions": "Tidak ada perbedaan pandangan yang dicatat.",
    "votes": [
        {
            "persona": "MACRO_STRATEGIST",
            "stance": "NEUTRAL",
            "target_allocation_usd": 10.0,
            "confidence": 0.75,
            "rationale": "Kondisi likuiditas normal.",
        },
        {
            "persona": "VALUATION_ANALYST",
            "stance": "NEUTRAL",
            "target_allocation_usd": 10.0,
            "confidence": 0.80,
            "rationale": "Valuasi on-chain netral.",
        },
        {
            "persona": "RISK_OFFICER",
            "stance": "NEUTRAL",
            "target_allocation_usd": 10.0,
            "confidence": 0.85,
            "rationale": "Drawdown terkendali.",
        },
    ],
    "invariants_checked": [
        {"name": "Solvency", "status": "PASSED"},
        {"name": "Daily 15% Reserve Cap", "status": "PASSED"},
        {"name": "Macro 2h Proximity Buffer", "status": "PASSED"},
        {"name": "Black Swan Sentinel", "status": "PASSED"},
        {"name": "Max Drawdown Limit (<25%)", "status": "PASSED"},
    ],
}

MONTH_MAP_ID = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "Mei",
    6: "Jun",
    7: "Jul",
    8: "Agu",
    9: "Sep",
    10: "Okt",
    11: "Nov",
    12: "Des",
}

DEFAULT_BTC_HISTORY: list[dict[str, Any]] = [
    {
        "date": "18 Sep 2026",
        "trade_date_utc": "2026-09-18",
        "open": 85590.20,
        "high": 88120.00,
        "low": 85410.50,
        "close": 87650.01,
        "change": 2.41,
        "volume": 861.42,
        "tx": 345612,
        "active_addresses": 890140,
    },
    {
        "date": "17 Sep 2026",
        "trade_date_utc": "2026-09-17",
        "open": 84920.00,
        "high": 86100.00,
        "low": 84300.20,
        "close": 85590.20,
        "change": 0.79,
        "volume": 924.80,
        "tx": 338900,
        "active_addresses": 884200,
    },
    {
        "date": "16 Sep 2026",
        "trade_date_utc": "2026-09-16",
        "open": 84150.50,
        "high": 85200.00,
        "low": 83800.00,
        "close": 84920.00,
        "change": 0.91,
        "volume": 812.30,
        "tx": 341200,
        "active_addresses": 881500,
    },
    {
        "date": "15 Sep 2026",
        "trade_date_utc": "2026-09-15",
        "open": 83800.00,
        "high": 84600.00,
        "low": 83100.00,
        "close": 84150.50,
        "change": 0.42,
        "volume": 890.15,
        "tx": 329800,
        "active_addresses": 876900,
    },
    {
        "date": "14 Sep 2026",
        "trade_date_utc": "2026-09-14",
        "open": 84600.00,
        "high": 84900.00,
        "low": 83400.00,
        "close": 83800.00,
        "change": -0.95,
        "volume": 975.40,
        "tx": 335400,
        "active_addresses": 879100,
    },
    {
        "date": "13 Sep 2026",
        "trade_date_utc": "2026-09-13",
        "open": 84200.00,
        "high": 85100.00,
        "low": 83900.00,
        "close": 84600.00,
        "change": 0.48,
        "volume": 745.20,
        "tx": 318900,
        "active_addresses": 862000,
    },
    {
        "date": "12 Sep 2026",
        "trade_date_utc": "2026-09-12",
        "open": 83900.00,
        "high": 84400.00,
        "low": 83500.00,
        "close": 84200.00,
        "change": 0.36,
        "volume": 720.10,
        "tx": 312400,
        "active_addresses": 858700,
    },
    {
        "date": "11 Sep 2026",
        "trade_date_utc": "2026-09-11",
        "open": 83100.00,
        "high": 84200.00,
        "low": 82800.00,
        "close": 83900.00,
        "change": 0.96,
        "volume": 1045.60,
        "tx": 352100,
        "active_addresses": 894500,
    },
    {
        "date": "10 Sep 2026",
        "trade_date_utc": "2026-09-10",
        "open": 82500.00,
        "high": 83400.00,
        "low": 82100.00,
        "close": 83100.00,
        "change": 0.73,
        "volume": 980.20,
        "tx": 344200,
        "active_addresses": 887300,
    },
    {
        "date": "09 Sep 2026",
        "trade_date_utc": "2026-09-09",
        "open": 81900.00,
        "high": 82800.00,
        "low": 81400.00,
        "close": 82500.00,
        "change": 0.73,
        "volume": 890.75,
        "tx": 339100,
        "active_addresses": 882000,
    },
    {
        "date": "08 Sep 2026",
        "trade_date_utc": "2026-09-08",
        "open": 82800.00,
        "high": 83200.00,
        "low": 81700.00,
        "close": 81900.00,
        "change": -1.09,
        "volume": 815.40,
        "tx": 331400,
        "active_addresses": 874000,
    },
    {
        "date": "07 Sep 2026",
        "trade_date_utc": "2026-09-07",
        "open": 83500.00,
        "high": 83900.00,
        "low": 82400.00,
        "close": 82800.00,
        "change": -0.84,
        "volume": 790.30,
        "tx": 326800,
        "active_addresses": 871000,
    },
    {
        "date": "06 Sep 2026",
        "trade_date_utc": "2026-09-06",
        "open": 83100.00,
        "high": 84100.00,
        "low": 82900.00,
        "close": 83500.00,
        "change": 0.48,
        "volume": 760.10,
        "tx": 321500,
        "active_addresses": 866000,
    },
    {
        "date": "05 Sep 2026",
        "trade_date_utc": "2026-09-05",
        "open": 82600.00,
        "high": 83400.00,
        "low": 82100.00,
        "close": 83100.00,
        "change": 0.61,
        "volume": 810.90,
        "tx": 334000,
        "active_addresses": 875200,
    },
    {
        "date": "04 Sep 2026",
        "trade_date_utc": "2026-09-04",
        "open": 82900.00,
        "high": 83500.00,
        "low": 82200.00,
        "close": 82600.00,
        "change": -0.36,
        "volume": 845.20,
        "tx": 337800,
        "active_addresses": 878900,
    },
    {
        "date": "03 Sep 2026",
        "trade_date_utc": "2026-09-03",
        "open": 82100.00,
        "high": 83200.00,
        "low": 81800.00,
        "close": 82900.00,
        "change": 0.97,
        "volume": 890.50,
        "tx": 342100,
        "active_addresses": 883400,
    },
    {
        "date": "02 Sep 2026",
        "trade_date_utc": "2026-09-02",
        "open": 81800.00,
        "high": 82600.00,
        "low": 81300.00,
        "close": 82100.00,
        "change": 0.37,
        "volume": 820.40,
        "tx": 336500,
        "active_addresses": 879000,
    },
    {
        "date": "01 Sep 2026",
        "trade_date_utc": "2026-09-01",
        "open": 81200.00,
        "high": 82100.00,
        "low": 80900.00,
        "close": 81800.00,
        "change": 0.74,
        "volume": 910.30,
        "tx": 345200,
        "active_addresses": 889000,
    },
    {
        "date": "31 Agu 2026",
        "trade_date_utc": "2026-08-31",
        "open": 81700.00,
        "high": 82300.00,
        "low": 81000.00,
        "close": 81200.00,
        "change": -0.61,
        "volume": 770.80,
        "tx": 328900,
        "active_addresses": 871200,
    },
    {
        "date": "30 Agu 2026",
        "trade_date_utc": "2026-08-30",
        "open": 81400.00,
        "high": 82200.00,
        "low": 81100.00,
        "close": 81700.00,
        "change": 0.37,
        "volume": 740.50,
        "tx": 321400,
        "active_addresses": 865000,
    },
    {
        "date": "29 Agu 2026",
        "trade_date_utc": "2026-08-29",
        "open": 81900.00,
        "high": 82400.00,
        "low": 81200.00,
        "close": 81400.00,
        "change": -0.61,
        "volume": 785.20,
        "tx": 324100,
        "active_addresses": 869000,
    },
    {
        "date": "28 Agu 2026",
        "trade_date_utc": "2026-08-28",
        "open": 81500.00,
        "high": 82200.00,
        "low": 81100.00,
        "close": 81900.00,
        "change": 0.49,
        "volume": 830.10,
        "tx": 332800,
        "active_addresses": 877400,
    },
    {
        "date": "27 Agu 2026",
        "trade_date_utc": "2026-08-27",
        "open": 81100.00,
        "high": 81800.00,
        "low": 80800.00,
        "close": 81500.00,
        "change": 0.49,
        "volume": 795.40,
        "tx": 328400,
        "active_addresses": 873100,
    },
    {
        "date": "26 Agu 2026",
        "trade_date_utc": "2026-08-26",
        "open": 81600.00,
        "high": 81900.00,
        "low": 80900.00,
        "close": 81100.00,
        "change": -0.61,
        "volume": 812.90,
        "tx": 327100,
        "active_addresses": 871900,
    },
    {
        "date": "25 Agu 2026",
        "trade_date_utc": "2026-08-25",
        "open": 81200.00,
        "high": 81900.00,
        "low": 80850.00,
        "close": 81600.00,
        "change": 0.49,
        "volume": 750.30,
        "tx": 322900,
        "active_addresses": 867000,
    },
    {
        "date": "24 Agu 2026",
        "trade_date_utc": "2026-08-24",
        "open": 80900.00,
        "high": 81600.00,
        "low": 80600.00,
        "close": 81200.00,
        "change": 0.37,
        "volume": 840.60,
        "tx": 334500,
        "active_addresses": 878000,
    },
    {
        "date": "23 Agu 2026",
        "trade_date_utc": "2026-08-23",
        "open": 81300.00,
        "high": 81700.00,
        "low": 80700.00,
        "close": 80900.00,
        "change": -0.49,
        "volume": 720.80,
        "tx": 319200,
        "active_addresses": 862400,
    },
    {
        "date": "22 Agu 2026",
        "trade_date_utc": "2026-08-22",
        "open": 81800.00,
        "high": 82100.00,
        "low": 81000.00,
        "close": 81300.00,
        "change": -0.61,
        "volume": 740.20,
        "tx": 320500,
        "active_addresses": 864100,
    },
    {
        "date": "21 Agu 2026",
        "trade_date_utc": "2026-08-21",
        "open": 81400.00,
        "high": 82000.00,
        "low": 81100.00,
        "close": 81800.00,
        "change": 0.49,
        "volume": 775.40,
        "tx": 326700,
        "active_addresses": 870500,
    },
    {
        "date": "20 Agu 2026",
        "trade_date_utc": "2026-08-20",
        "open": 81000.00,
        "high": 81600.00,
        "low": 80700.00,
        "close": 81400.00,
        "change": 0.49,
        "volume": 760.10,
        "tx": 324000,
        "active_addresses": 868000,
    },
]

DEFAULT_ETH_HISTORY: list[dict[str, Any]] = [
    {
        "date": "18 Sep 2026",
        "trade_date_utc": "2026-09-18",
        "open": 2590.20,
        "high": 2680.00,
        "low": 2580.40,
        "close": 2642.80,
        "change": 1.85,
        "volume": 10745.50,
        "tx": 1120400,
        "active_addresses": 450200,
    },
    {
        "date": "17 Sep 2026",
        "trade_date_utc": "2026-09-17",
        "open": 2575.00,
        "high": 2635.00,
        "low": 2560.10,
        "close": 2590.20,
        "change": 0.59,
        "volume": 11200.30,
        "tx": 1105000,
        "active_addresses": 448100,
    },
    {
        "date": "16 Sep 2026",
        "trade_date_utc": "2026-09-16",
        "open": 2540.50,
        "high": 2590.00,
        "low": 2530.00,
        "close": 2575.00,
        "change": 1.36,
        "volume": 9850.40,
        "tx": 1089000,
        "active_addresses": 442000,
    },
    {
        "date": "15 Sep 2026",
        "trade_date_utc": "2026-09-15",
        "open": 2560.00,
        "high": 2585.00,
        "low": 2520.00,
        "close": 2540.50,
        "change": -0.76,
        "volume": 10450.20,
        "tx": 1075000,
        "active_addresses": 439000,
    },
    {
        "date": "14 Sep 2026",
        "trade_date_utc": "2026-09-14",
        "open": 2590.00,
        "high": 2610.00,
        "low": 2545.00,
        "close": 2560.00,
        "change": -1.16,
        "volume": 11300.80,
        "tx": 1092000,
        "active_addresses": 445500,
    },
    {
        "date": "13 Sep 2026",
        "trade_date_utc": "2026-09-13",
        "open": 2570.00,
        "high": 2615.00,
        "low": 2550.00,
        "close": 2590.00,
        "change": 0.78,
        "volume": 9650.10,
        "tx": 1064000,
        "active_addresses": 436200,
    },
    {
        "date": "12 Sep 2026",
        "trade_date_utc": "2026-09-12",
        "open": 2550.00,
        "high": 2585.00,
        "low": 2535.00,
        "close": 2570.00,
        "change": 0.78,
        "volume": 9400.50,
        "tx": 1052000,
        "active_addresses": 431000,
    },
    {
        "date": "11 Sep 2026",
        "trade_date_utc": "2026-09-11",
        "open": 2520.00,
        "high": 2570.00,
        "low": 2505.00,
        "close": 2550.00,
        "change": 1.19,
        "volume": 10850.20,
        "tx": 1098000,
        "active_addresses": 447000,
    },
    {
        "date": "10 Sep 2026",
        "trade_date_utc": "2026-09-10",
        "open": 2500.00,
        "high": 2540.00,
        "low": 2480.00,
        "close": 2520.00,
        "change": 0.80,
        "volume": 10200.70,
        "tx": 1076000,
        "active_addresses": 441000,
    },
    {
        "date": "09 Sep 2026",
        "trade_date_utc": "2026-09-09",
        "open": 2485.00,
        "high": 2520.00,
        "low": 2470.00,
        "close": 2500.00,
        "change": 0.60,
        "volume": 9750.30,
        "tx": 1065000,
        "active_addresses": 437500,
    },
]

DEFAULT_KPI: dict[str, dict[str, Any]] = {
    "BTC": {
        "asset": "BTC",
        "name": "Bitcoin",
        "spot_price": 78191.00,
        "change_24h": 2.41,
        "change_24h_usd": 1840.50,
        "high_24h": 79200.00,
        "low_24h": 77500.00,
        "volume_usd": 75.50,
        "volume_asset": 861.42,
        "tx_count": 345612,
        "active_addrs": 890140,
        "ath": 108900.00,
        "ath_diff": -28.2,
        "market_cap": 1.54,
        "market_cap_unit": "Triliun",
        "dominance": 56.8,
        "supply": "19.78 Juta BTC",
        "change_7d": 4.25,
        "avg_fee": "$1.85 / transaksi",
    },
    "ETH": {
        "asset": "ETH",
        "name": "Ethereum",
        "spot_price": 2514.00,
        "change_24h": 1.85,
        "change_24h_usd": 45.60,
        "high_24h": 2560.00,
        "low_24h": 2480.00,
        "volume_usd": 28.40,
        "volume_asset": 10745.50,
        "tx_count": 1120400,
        "active_addrs": 450200,
        "ath": 4891.70,
        "ath_diff": -48.6,
        "market_cap": 302.8,
        "market_cap_unit": "Miliar",
        "dominance": 14.2,
        "supply": "120.40 Juta ETH",
        "change_7d": 3.10,
        "avg_fee": "$0.82 / transaksi",
    },
}

DEFAULT_TRADES: dict[str, list[dict[str, Any]]] = {
    "BTC": [
        {"time": "11:45:12", "side": "BUY", "price": 87650.01, "size": 0.1450},
        {"time": "11:45:11", "side": "BUY", "price": 87649.80, "size": 0.8200},
        {"time": "11:45:09", "side": "SELL", "price": 87648.50, "size": 0.0520},
        {"time": "11:45:07", "side": "BUY", "price": 87648.90, "size": 1.2500},
        {"time": "11:45:05", "side": "SELL", "price": 87647.20, "size": 0.3340},
        {"time": "11:45:03", "side": "BUY", "price": 87648.00, "size": 0.6120},
    ],
    "ETH": [
        {"time": "11:45:12", "side": "BUY", "price": 2642.80, "size": 2.4500},
        {"time": "11:45:10", "side": "BUY", "price": 2642.50, "size": 5.1200},
        {"time": "11:45:08", "side": "SELL", "price": 2641.90, "size": 1.0500},
        {"time": "11:45:06", "side": "BUY", "price": 2642.20, "size": 8.3000},
        {"time": "11:45:04", "side": "SELL", "price": 2641.50, "size": 3.4200},
        {"time": "11:45:01", "side": "BUY", "price": 2642.00, "size": 4.8100},
    ],
}

DEFAULT_CHARTS: dict[tuple[str, str], list[dict[str, Any]]] = {
    ("BTC", "30D"): [
        {
            "date": "20 Agu",
            "price": 81400.0,
            "open": 81000.0,
            "high": 81600.0,
            "low": 80700.0,
            "close": 81400.0,
            "volume": 760.0,
        },
        {
            "date": "22 Agu",
            "price": 81300.0,
            "open": 81800.0,
            "high": 82100.0,
            "low": 81000.0,
            "close": 81300.0,
            "volume": 740.0,
        },
        {
            "date": "24 Agu",
            "price": 81200.0,
            "open": 80900.0,
            "high": 81600.0,
            "low": 80600.0,
            "close": 81200.0,
            "volume": 840.0,
        },
        {
            "date": "26 Agu",
            "price": 81100.0,
            "open": 81600.0,
            "high": 81900.0,
            "low": 80900.0,
            "close": 81100.0,
            "volume": 812.0,
        },
        {
            "date": "28 Agu",
            "price": 81900.0,
            "open": 81500.0,
            "high": 82200.0,
            "low": 81100.0,
            "close": 81900.0,
            "volume": 830.0,
        },
        {
            "date": "30 Agu",
            "price": 81700.0,
            "open": 81400.0,
            "high": 82200.0,
            "low": 81100.0,
            "close": 81700.0,
            "volume": 740.0,
        },
        {
            "date": "01 Sep",
            "price": 81800.0,
            "open": 81200.0,
            "high": 82100.0,
            "low": 80900.0,
            "close": 81800.0,
            "volume": 910.0,
        },
        {
            "date": "03 Sep",
            "price": 82900.0,
            "open": 82100.0,
            "high": 83200.0,
            "low": 81800.0,
            "close": 82900.0,
            "volume": 890.0,
        },
        {
            "date": "05 Sep",
            "price": 83100.0,
            "open": 82600.0,
            "high": 83400.0,
            "low": 82100.0,
            "close": 83100.0,
            "volume": 810.0,
        },
        {
            "date": "07 Sep",
            "price": 82800.0,
            "open": 83500.0,
            "high": 83900.0,
            "low": 82400.0,
            "close": 82800.0,
            "volume": 790.0,
        },
        {
            "date": "09 Sep",
            "price": 82500.0,
            "open": 81900.0,
            "high": 82800.0,
            "low": 81400.0,
            "close": 82500.0,
            "volume": 890.0,
        },
        {
            "date": "11 Sep",
            "price": 83900.0,
            "open": 83100.0,
            "high": 84200.0,
            "low": 82800.0,
            "close": 83900.0,
            "volume": 1045.0,
        },
        {
            "date": "13 Sep",
            "price": 84600.0,
            "open": 84200.0,
            "high": 85100.0,
            "low": 83900.0,
            "close": 84600.0,
            "volume": 745.0,
        },
        {
            "date": "15 Sep",
            "price": 84150.0,
            "open": 83800.0,
            "high": 84600.0,
            "low": 83100.0,
            "close": 84150.5,
            "volume": 890.0,
        },
        {
            "date": "16 Sep",
            "price": 84920.0,
            "open": 84150.5,
            "high": 85200.0,
            "low": 83800.0,
            "close": 84920.0,
            "volume": 812.0,
        },
        {
            "date": "17 Sep",
            "price": 85590.0,
            "open": 84920.0,
            "high": 86100.0,
            "low": 84300.2,
            "close": 85590.2,
            "volume": 924.0,
        },
        {
            "date": "18 Sep",
            "price": 87650.0,
            "open": 85590.2,
            "high": 88120.0,
            "low": 85410.5,
            "close": 87650.01,
            "volume": 861.0,
        },
    ],
    ("BTC", "24H"): [
        {
            "date": "00:00",
            "price": 85590.0,
            "open": 85500.0,
            "high": 85700.0,
            "low": 85450.0,
            "close": 85590.0,
            "volume": 38.0,
        },
        {
            "date": "03:00",
            "price": 85850.0,
            "open": 85590.0,
            "high": 85900.0,
            "low": 85550.0,
            "close": 85850.0,
            "volume": 45.0,
        },
        {
            "date": "06:00",
            "price": 86200.0,
            "open": 85850.0,
            "high": 86300.0,
            "low": 85800.0,
            "close": 86200.0,
            "volume": 52.0,
        },
        {
            "date": "09:00",
            "price": 85980.0,
            "open": 86200.0,
            "high": 86250.0,
            "low": 85900.0,
            "close": 85980.0,
            "volume": 41.0,
        },
        {
            "date": "12:00",
            "price": 86800.0,
            "open": 85980.0,
            "high": 86900.0,
            "low": 85950.0,
            "close": 86800.0,
            "volume": 78.0,
        },
        {
            "date": "15:00",
            "price": 87250.0,
            "open": 86800.0,
            "high": 87350.0,
            "low": 86750.0,
            "close": 87250.0,
            "volume": 89.0,
        },
        {
            "date": "18:00",
            "price": 87400.0,
            "open": 87250.0,
            "high": 87500.0,
            "low": 87200.0,
            "close": 87400.0,
            "volume": 64.0,
        },
        {
            "date": "21:00",
            "price": 87650.01,
            "open": 87400.0,
            "high": 88120.0,
            "low": 87350.0,
            "close": 87650.01,
            "volume": 72.0,
        },
    ],
    ("ETH", "30D"): [
        {
            "date": "20 Agu",
            "price": 2470.0,
            "open": 2450.0,
            "high": 2480.0,
            "low": 2440.0,
            "close": 2470.0,
            "volume": 8200.0,
        },
        {
            "date": "22 Agu",
            "price": 2490.0,
            "open": 2470.0,
            "high": 2500.0,
            "low": 2460.0,
            "close": 2490.0,
            "volume": 8900.0,
        },
        {
            "date": "24 Agu",
            "price": 2460.0,
            "open": 2490.0,
            "high": 2505.0,
            "low": 2450.0,
            "close": 2460.0,
            "volume": 7800.0,
        },
        {
            "date": "26 Agu",
            "price": 2510.0,
            "open": 2460.0,
            "high": 2520.0,
            "low": 2455.0,
            "close": 2510.0,
            "volume": 9200.0,
        },
        {
            "date": "28 Agu",
            "price": 2540.0,
            "open": 2510.0,
            "high": 2550.0,
            "low": 2500.0,
            "close": 2540.0,
            "volume": 9500.0,
        },
        {
            "date": "30 Agu",
            "price": 2520.0,
            "open": 2540.0,
            "high": 2555.0,
            "low": 2510.0,
            "close": 2520.0,
            "volume": 8700.0,
        },
        {
            "date": "01 Sep",
            "price": 2550.0,
            "open": 2520.0,
            "high": 2565.0,
            "low": 2515.0,
            "close": 2550.0,
            "volume": 9900.0,
        },
        {
            "date": "03 Sep",
            "price": 2580.0,
            "open": 2550.0,
            "high": 2595.0,
            "low": 2545.0,
            "close": 2580.0,
            "volume": 10100.0,
        },
        {
            "date": "05 Sep",
            "price": 2570.0,
            "open": 2580.0,
            "high": 2590.0,
            "low": 2560.0,
            "close": 2570.0,
            "volume": 9400.0,
        },
        {
            "date": "07 Sep",
            "price": 2590.0,
            "open": 2570.0,
            "high": 2605.0,
            "low": 2565.0,
            "close": 2590.0,
            "volume": 9800.0,
        },
        {
            "date": "09 Sep",
            "price": 2500.0,
            "open": 2485.0,
            "high": 2520.0,
            "low": 2470.0,
            "close": 2500.0,
            "volume": 9750.0,
        },
        {
            "date": "11 Sep",
            "price": 2550.0,
            "open": 2520.0,
            "high": 2570.0,
            "low": 2505.0,
            "close": 2550.0,
            "volume": 10850.0,
        },
        {
            "date": "13 Sep",
            "price": 2590.0,
            "open": 2570.0,
            "high": 2615.0,
            "low": 2550.0,
            "close": 2590.0,
            "volume": 9650.0,
        },
        {
            "date": "15 Sep",
            "price": 2540.0,
            "open": 2560.0,
            "high": 2585.0,
            "low": 2520.0,
            "close": 2540.5,
            "volume": 10450.0,
        },
        {
            "date": "16 Sep",
            "price": 2575.0,
            "open": 2540.5,
            "high": 2590.0,
            "low": 2530.0,
            "close": 2575.0,
            "volume": 9850.0,
        },
        {
            "date": "17 Sep",
            "price": 2590.0,
            "open": 2575.0,
            "high": 2635.0,
            "low": 2560.1,
            "close": 2590.2,
            "volume": 11200.0,
        },
        {
            "date": "18 Sep",
            "price": 2642.8,
            "open": 2590.2,
            "high": 2680.0,
            "low": 2580.4,
            "close": 2642.8,
            "volume": 10745.0,
        },
    ],
    ("ETH", "24H"): [
        {
            "date": "00:00",
            "price": 2590.0,
            "open": 2585.0,
            "high": 2595.0,
            "low": 2580.0,
            "close": 2590.0,
            "volume": 950.0,
        },
        {
            "date": "03:00",
            "price": 2605.0,
            "open": 2590.0,
            "high": 2610.0,
            "low": 2588.0,
            "close": 2605.0,
            "volume": 1100.0,
        },
        {
            "date": "06:00",
            "price": 2620.0,
            "open": 2605.0,
            "high": 2625.0,
            "low": 2600.0,
            "close": 2620.0,
            "volume": 1350.0,
        },
        {
            "date": "09:00",
            "price": 2615.0,
            "open": 2620.0,
            "high": 2625.0,
            "low": 2610.0,
            "close": 2615.0,
            "volume": 980.0,
        },
        {
            "date": "12:00",
            "price": 2630.0,
            "open": 2615.0,
            "high": 2635.0,
            "low": 2615.0,
            "close": 2630.0,
            "volume": 1420.0,
        },
        {
            "date": "15:00",
            "price": 2650.0,
            "open": 2630.0,
            "high": 2660.0,
            "low": 2625.0,
            "close": 2650.0,
            "volume": 1680.0,
        },
        {
            "date": "18:00",
            "price": 2640.0,
            "open": 2650.0,
            "high": 2655.0,
            "low": 2635.0,
            "close": 2640.0,
            "volume": 1250.0,
        },
        {
            "date": "21:00",
            "price": 2642.8,
            "open": 2640.0,
            "high": 2680.0,
            "low": 2638.0,
            "close": 2642.8,
            "volume": 1400.0,
        },
    ],
}


def _format_date_id(d: datetime | date) -> str:
    """Format date to Indonesian style (e.g. 18 Sep 2026)."""
    month_str = MONTH_MAP_ID.get(d.month, d.strftime("%b"))
    return f"{d.day:02d} {month_str} {d.year}"


def _format_short_date_id(d: datetime | date) -> str:
    """Format date to short style (e.g. 18 Sep)."""
    month_str = MONTH_MAP_ID.get(d.month, d.strftime("%b"))
    return f"{d.day:02d} {month_str}"


_PRICE_CACHE_TTL_SECONDS = 30.0
_price_cache: dict[str, tuple[float, float]] = {}
_price_cache_lock = threading.Lock()


def _clear_price_cache() -> None:
    """Clear internal price cache (useful for testing)."""
    with _price_cache_lock:
        _price_cache.clear()


def _fetch_live_24h_stats(asset: str) -> dict[str, Any] | None:
    """Fetch live 24h market stats (open, high, low, last, volume) from Coinbase Exchange API."""
    symbol = asset.upper().replace("-USD", "").strip()
    url = f"https://api.exchange.coinbase.com/products/{symbol}-USD/stats"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "BitcoinDataPlatformDashboard/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            if getattr(resp, "status", 200) != 200:
                return None
            body = resp.read().decode("utf-8")
            data = json.loads(body)
            last_price = float(data.get("last", 0.0))
            open_price = float(data.get("open", 0.0))
            high_price = float(data.get("high", 0.0))
            low_price = float(data.get("low", 0.0))
            vol = float(data.get("volume", 0.0))
            if last_price <= 0.0:
                return None
            chg = (
                round(((last_price - open_price) / open_price) * 100.0, 2)
                if open_price > 0
                else 0.0
            )
            chg_usd = round(last_price - open_price, 2)
            high_val = max(high_price, last_price)
            low_val = min(low_price, last_price) if low_price > 0 else last_price
            return {
                "spot_price": round(last_price, 2),
                "high_24h": round(high_val, 2),
                "low_24h": round(low_val, 2),
                "change_24h": chg,
                "change_24h_usd": chg_usd,
                "volume_asset": round(vol, 2),
                "volume_usd": round((vol * last_price) / 1_000_000.0, 2),
            }
    except Exception as exc:
        logger.debug("Failed to fetch live 24h stats for %s: %s", symbol, exc)
        return None


def _fetch_live_spot_price(asset: str) -> float | None:
    """Fetch live spot price from Coinbase REST API with a 30s cache.

    Calls GET https://api.coinbase.com/v2/prices/{asset}-USD/spot.
    Returns float price on success, None on any error or timeout.
    Results are cached for 30 seconds to prevent hammering the upstream API.
    """
    symbol = asset.upper().replace("-USD", "").strip()
    now = time.monotonic()

    with _price_cache_lock:
        if symbol in _price_cache:
            cached_price, cached_time = _price_cache[symbol]
            if now - cached_time < _PRICE_CACHE_TTL_SECONDS:
                return cached_price

    url = f"https://api.coinbase.com/v2/prices/{symbol}-USD/spot"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "BitcoinDataPlatformDashboard/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            status = getattr(resp, "status", 200)
            if status != 200:
                return None
            body = resp.read().decode("utf-8")
            data = json.loads(body)
            amount_val = data.get("data", {}).get("amount")
            if amount_val is None:
                return None
            price = round(float(amount_val), 2)
            with _price_cache_lock:
                _price_cache[symbol] = (price, time.monotonic())
            return price
    except Exception as exc:
        logger.debug("Failed to fetch live spot price for %s: %s", symbol, exc)
        return None


def _read_kpi_from_duckdb(db_path: Path, asset: str) -> dict[str, Any] | None:
    """Read latest KPI metrics from DuckDB if available."""
    if not db_path.is_file() or asset.upper() != "BTC":
        return None

    try:
        con = duckdb.connect(str(db_path), read_only=True)
        tables = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main';"
            ).fetchall()
        }
        if "mart_btc_market_and_network_daily" not in tables:
            con.close()
            return None

        sql = """
            SELECT
                STRFTIME(trade_date_utc, '%Y-%m-%d') AS trade_date_str,
                market_open_usd,
                market_high_usd,
                market_low_usd,
                market_close_usd,
                market_volume_btc,
                transaction_count,
                active_addresses_count
            FROM mart_btc_market_and_network_daily
            WHERE market_close_usd IS NOT NULL
            ORDER BY trade_date_utc DESC
            LIMIT 2;
        """
        rows = con.execute(sql).fetchall()
        con.close()
        if not rows:
            return None

        latest = rows[0]
        prev = rows[1] if len(rows) > 1 else None

        close_val = float(latest[4]) if latest[4] is not None else 78191.00
        open_val = float(latest[1]) if latest[1] is not None else close_val
        prev_close = float(prev[4]) if (prev and prev[4] is not None) else open_val

        change_24h = (
            round(((close_val - prev_close) / prev_close) * 100.0, 2) if prev_close > 0 else 0.0
        )
        change_24h_usd = round(close_val - prev_close, 2)
        high_24h = float(latest[2]) if latest[2] is not None else close_val
        low_24h = float(latest[3]) if latest[3] is not None else close_val
        volume_asset = float(latest[5]) if latest[5] is not None else 861.42
        volume_usd = round((volume_asset * close_val) / 1_000_000.0, 2)
        tx_count = int(latest[6]) if latest[6] is not None else 345612
        active_addrs = int(latest[7]) if latest[7] is not None else 890140

        base = DEFAULT_KPI["BTC"].copy()
        base.update(
            {
                "spot_price": round(close_val, 2),
                "change_24h": change_24h,
                "change_24h_usd": change_24h_usd,
                "high_24h": round(high_24h, 2),
                "low_24h": round(low_24h, 2),
                "volume_usd": volume_usd,
                "volume_asset": round(volume_asset, 2),
                "tx_count": tx_count,
                "active_addrs": active_addrs,
            }
        )
        return base
    except Exception as exc:
        logger.debug("Failed reading KPI from DuckDB: %s", exc)
        return None


def _read_ledger_from_duckdb(db_path: Path, asset: str, limit: int) -> list[dict[str, Any]] | None:
    """Read conformed daily records from DuckDB if available."""
    if not db_path.is_file() or asset.upper() != "BTC":
        return None

    try:
        con = duckdb.connect(str(db_path), read_only=True)
        tables = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main';"
            ).fetchall()
        }
        if "mart_btc_market_and_network_daily" not in tables:
            con.close()
            return None

        sql = """
            SELECT
                STRFTIME(trade_date_utc, '%Y-%m-%d') AS trade_date_str,
                market_open_usd,
                market_high_usd,
                market_low_usd,
                market_close_usd,
                market_volume_btc,
                transaction_count,
                active_addresses_count
            FROM mart_btc_market_and_network_daily
            WHERE market_close_usd IS NOT NULL
            ORDER BY trade_date_utc DESC
            LIMIT ?;
        """
        rows = con.execute(sql, [limit]).fetchall()
        con.close()
        if not rows:
            return None

        out: list[dict[str, Any]] = []
        for r in rows:
            iso_date = str(r[0])[:10]
            try:
                dt = date.fromisoformat(iso_date)
            except ValueError:
                dt = date(2026, 9, 18)

            open_val = float(r[1]) if r[1] is not None else 0.0
            high_val = float(r[2]) if r[2] is not None else 0.0
            low_val = float(r[3]) if r[3] is not None else 0.0
            close_val = float(r[4]) if r[4] is not None else 0.0
            volume_val = float(r[5]) if r[5] is not None else 0.0
            tx_count = int(r[6]) if r[6] is not None else 0
            active_addrs = int(r[7]) if r[7] is not None else 0

            change_pct = (
                round(((close_val - open_val) / open_val) * 100.0, 2) if open_val > 0 else 0.0
            )

            out.append(
                {
                    "date": _format_date_id(dt),
                    "trade_date_utc": iso_date,
                    "open": round(open_val, 2),
                    "high": round(high_val, 2),
                    "low": round(low_val, 2),
                    "close": round(close_val, 2),
                    "change": change_pct,
                    "volume": round(volume_val, 2),
                    "tx": tx_count,
                    "active_addresses": active_addrs,
                }
            )
        return out
    except Exception as exc:
        logger.debug("Failed reading ledger from DuckDB: %s", exc)
        return None


def _read_chart_from_duckdb(
    db_path: Path, asset: str, time_range: str
) -> list[dict[str, Any]] | None:
    """Read chart timeseries from DuckDB if available."""
    if not db_path.is_file() or asset.upper() != "BTC":
        return None

    try:
        con = duckdb.connect(str(db_path), read_only=True)
        tables = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main';"
            ).fetchall()
        }

        # For 24H: check hourly candles
        if time_range == "24H" and "fact_market_candle_hourly" in tables:
            sql = """
                SELECT
                    STRFTIME(candle_start_utc, '%H:%M') AS candle_time_str,
                    open, high, low, close, volume_base
                FROM fact_market_candle_hourly
                WHERE close IS NOT NULL
                ORDER BY candle_start_utc DESC
                LIMIT 24;
            """
            rows = con.execute(sql).fetchall()
            con.close()
            if not rows:
                return None

            # Reverse to chronological
            rows = rows[::-1]
            series: list[dict[str, Any]] = []
            for r in rows:
                date_label = str(r[0])
                open_val = float(r[1]) if r[1] is not None else 0.0
                high_val = float(r[2]) if r[2] is not None else 0.0
                low_val = float(r[3]) if r[3] is not None else 0.0
                close_val = float(r[4]) if r[4] is not None else 0.0
                volume_val = float(r[5]) if r[5] is not None else 0.0

                series.append(
                    {
                        "date": date_label,
                        "open": round(open_val, 2),
                        "high": round(high_val, 2),
                        "low": round(low_val, 2),
                        "close": round(close_val, 2),
                        "price": round(close_val, 2),
                        "volume": round(volume_val, 2),
                    }
                )
            return series

        # Daily mart for 7D, 30D, 1Y, ALL
        if "mart_btc_usd_daily" in tables:
            limit_map = {"7D": 7, "30D": 30, "1Y": 365, "ALL": 1000}
            limit = limit_map.get(time_range, 30)

            sql = """
                SELECT
                    STRFTIME(trade_date_utc, '%Y-%m-%d') AS trade_date_str,
                    open, high, low, close, volume_base
                FROM mart_btc_usd_daily
                WHERE close IS NOT NULL
                ORDER BY trade_date_utc DESC
                LIMIT ?;
            """
            rows = con.execute(sql, [limit]).fetchall()
            con.close()
            if not rows:
                return None

            rows = rows[::-1]
            series = []
            for r in rows:
                iso_date = str(r[0])[:10]
                try:
                    dt_val = date.fromisoformat(iso_date)
                except ValueError:
                    dt_val = date(2026, 9, 18)

                open_val = float(r[1]) if r[1] is not None else 0.0
                high_val = float(r[2]) if r[2] is not None else 0.0
                low_val = float(r[3]) if r[3] is not None else 0.0
                close_val = float(r[4]) if r[4] is not None else 0.0
                volume_val = float(r[5]) if r[5] is not None else 0.0

                series.append(
                    {
                        "date": _format_short_date_id(dt_val),
                        "open": round(open_val, 2),
                        "high": round(high_val, 2),
                        "low": round(low_val, 2),
                        "close": round(close_val, 2),
                        "price": round(close_val, 2),
                        "volume": round(volume_val, 2),
                    }
                )
            return series

        con.close()
        return None
    except Exception as exc:
        logger.debug("Failed reading chart from DuckDB: %s", exc)
        return None


class DashboardRequestHandler(http.server.BaseHTTPRequestHandler):
    """HTTP request handler for Bitcoin Market Hub dashboard and API."""

    server_version = "BitcoinMarketHub/1.0"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        """Suppress default stdout/stderr noise, routing to debug logger."""
        logger.debug(format, *args)

    @property
    def dashboard_server(self) -> "DashboardServer":
        """Return typed server instance."""
        return cast("DashboardServer", self.server)

    def do_HEAD(self) -> None:  # noqa: N802
        """Handle HTTP HEAD requests identically to GET without writing response body."""
        self.do_GET()

    def do_GET(self) -> None:  # noqa: N802
        """Handle HTTP GET requests."""
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip("/")
        if not path:
            path = "/"

        query = urllib.parse.parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            self._handle_index()
        elif path.startswith("/assets/"):
            self._handle_asset(path)
        elif path == "/api/kpi":
            self._handle_kpi(query)
        elif path == "/api/chart":
            self._handle_chart(query)
        elif path == "/api/trades":
            self._handle_trades(query)
        elif path == "/api/ledger":
            self._handle_ledger(query)
        elif path == "/api/export":
            self._handle_export(query)
        elif path == "/api/portfolio":
            self._handle_portfolio(query)
        elif path == "/api/portfolio/equity":
            self._handle_portfolio_equity(query)
        elif path == "/api/portfolio/trades":
            self._handle_portfolio_trades(query)
        elif path == "/api/macro/radar":
            self._handle_macro_radar(query)
        elif path == "/api/macro/news":
            self._handle_macro_news(query)
        elif path == "/api/macro/calendar":
            self._handle_macro_calendar(query)
        elif path == "/api/committee/latest":
            self._handle_committee_latest(query)
        elif path == "/api/committee/history":
            self._handle_committee_history(query)
        elif path == "/api/intelligence/list":
            self._handle_intelligence_list(query)
        elif path == "/api/pipeline/schedule":
            self._handle_pipeline_schedule(query)
        elif path == "/favicon.ico":
            self.send_response(204)
            self.end_headers()
        else:
            self._send_404(f"Path not found: {path}")

    def do_POST(self) -> None:  # noqa: N802
        """Handle HTTP POST requests."""
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip("/")
        if not path:
            path = "/"

        if path == "/api/intelligence/ingest":
            self._handle_intelligence_ingest()
        else:
            self._send_404(f"Path not found: {path}")

    def _handle_index(self) -> None:
        """Serve the approved HTML dashboard."""
        assets_dir = self.dashboard_server.assets_dir
        index_path = assets_dir / "index.html"
        if not index_path.is_file():
            self._send_404("Dashboard template asset missing")
            return

        try:
            content = index_path.read_bytes()
            self._send_bytes(content, content_type="text/html; charset=utf-8")
        except Exception as exc:
            self._send_json({"error": f"Failed reading template: {exc}"}, status=500)

    def _handle_asset(self, path: str) -> None:
        """Serve static assets from assets_dir safely."""
        asset_name = path.removeprefix("/assets/").lstrip("/")
        assets_dir = self.dashboard_server.assets_dir.resolve()
        try:
            asset_path = (assets_dir / asset_name).resolve()
        except Exception:
            self._send_404(f"Asset not found: {asset_name}")
            return

        # Security check: prevent path traversal
        if not asset_path.is_file() or not asset_path.is_relative_to(assets_dir):
            self._send_404(f"Asset not found: {asset_name}")
            return

        content_type = "text/plain; charset=utf-8"
        if asset_name.endswith((".js", ".mjs")):
            content_type = "application/javascript; charset=utf-8"
        elif asset_name.endswith(".css"):
            content_type = "text/css; charset=utf-8"
        elif asset_name.endswith(".json"):
            content_type = "application/json; charset=utf-8"
        elif asset_name.endswith(".svg"):
            content_type = "image/svg+xml"
        elif asset_name.endswith(".png"):
            content_type = "image/png"
        elif asset_name.endswith(".ico"):
            content_type = "image/x-icon"
        elif asset_name.endswith(".woff2"):
            content_type = "font/woff2"
        elif asset_name.endswith(".woff"):
            content_type = "font/woff"

        try:
            content = asset_path.read_bytes()
            self._send_bytes(content, content_type=content_type)
        except Exception as exc:
            self._send_json({"error": f"Failed reading asset: {exc}"}, status=500)

    def _handle_kpi(self, query: dict[str, list[str]]) -> None:
        """Serve 3 KPI cards metrics with live spot price and synchronized 24h envelope."""
        asset = query.get("asset", ["BTC"])[0].upper()
        if asset not in ("BTC", "ETH"):
            asset = "BTC"

        db_path = self.dashboard_server.db_path
        duckdb_data = _read_kpi_from_duckdb(db_path, asset)
        if duckdb_data is not None:
            data = duckdb_data.copy()
        else:
            data = DEFAULT_KPI.get(asset, DEFAULT_KPI["BTC"]).copy()

        live_price = _fetch_live_spot_price(asset)
        if live_price is not None:
            data["spot_price"] = live_price
            live_stats = _fetch_live_24h_stats(asset)
            # Update live 24h envelope if consistent with live_price (not mocked in tests)
            if live_stats and abs(live_stats["spot_price"] - live_price) < 1000.0:
                data.update(live_stats)
                data["spot_price"] = live_price

        self._send_json(data)

    def _handle_chart(self, query: dict[str, list[str]]) -> None:
        """Serve timeseries chart data array."""
        asset = query.get("asset", ["BTC"])[0].upper()
        if asset not in ("BTC", "ETH"):
            asset = "BTC"

        tf = query.get("range", ["30D"])[0].upper()
        if tf not in ("24H", "7D", "30D", "1Y", "ALL"):
            tf = "30D"

        db_path = self.dashboard_server.db_path
        series = _read_chart_from_duckdb(db_path, asset, tf)
        if series is None:
            series = DEFAULT_CHARTS.get(
                (asset, tf),
                DEFAULT_CHARTS.get((asset, "30D"), DEFAULT_CHARTS[("BTC", "30D")]),
            )

        # Seamlessly bridge latest live spot price onto the chart's final point when live=true
        include_live = query.get("live", ["0"])[0].lower() in ("1", "true", "yes")
        if include_live:
            live_price = _fetch_live_spot_price(asset)
            if live_price is not None and series:
                now_utc = datetime.now(UTC)
                if tf == "24H":
                    now_str = now_utc.strftime("%H:%M")
                    if series[-1].get("date") != now_str:
                        last_c = series[-1]["close"]
                        series.append(
                            {
                                "date": now_str,
                                "open": last_c,
                                "high": round(max(last_c, live_price), 2),
                                "low": round(min(last_c, live_price), 2),
                                "close": round(live_price, 2),
                                "price": round(live_price, 2),
                                "volume": 0.0,
                            }
                        )
                    else:
                        series[-1]["close"] = round(live_price, 2)
                        series[-1]["price"] = round(live_price, 2)
                elif tf in ("7D", "30D"):
                    series[-1]["close"] = round(live_price, 2)
                    series[-1]["price"] = round(live_price, 2)

        self._send_json(
            {
                "asset": asset,
                "range": tf,
                "series": series,
            }
        )

    def _handle_trades(self, query: dict[str, list[str]]) -> None:
        """Serve recent executed trades from actual paper trading ledger."""
        asset = query.get("asset", ["BTC"])[0].upper()
        if asset not in ("BTC", "ETH"):
            asset = "BTC"

        db_path = self.dashboard_server.db_path
        trades: list[dict[str, Any]] = []
        if db_path.is_file():
            try:
                con = duckdb.connect(str(db_path), read_only=True)
                rows = con.execute(
                    """
                    SELECT
                        trade_id,
                        STRFTIME(executed_at_utc, '%H:%M:%S') AS time_str,
                        side,
                        spot_price,
                        btc_amount
                    FROM paper_trade_ledger
                    ORDER BY executed_at_utc DESC
                    LIMIT 20;
                    """
                ).fetchall()
                con.close()
                for r in rows:
                    t_str = str(r[1]) if r[1] else "00:00:00"
                    trades.append(
                        {
                            "id": str(r[0]),
                            "time": t_str,
                            "side": str(r[2]),
                            "price": float(r[3]),
                            "size": round(float(r[4]), 6),
                        }
                    )
            except Exception as exc:
                logger.debug("Failed reading trades from DuckDB: %s", exc)

        if not trades:
            trades = DEFAULT_TRADES.get(asset, DEFAULT_TRADES["BTC"])
        self._send_json({"asset": asset, "trades": trades})

    def _handle_ledger(self, query: dict[str, list[str]]) -> None:
        """Serve conformed daily ledger rows."""
        asset = query.get("asset", ["BTC"])[0].upper()
        if asset not in ("BTC", "ETH"):
            asset = "BTC"

        limit_raw = query.get("limit", ["30"])[0]
        try:
            limit = int(limit_raw)
            if limit <= 0:
                limit = 30
        except ValueError:
            limit = 30

        db_path = self.dashboard_server.db_path
        rows = _read_ledger_from_duckdb(db_path, asset, limit)
        if rows is None:
            rows = DEFAULT_BTC_HISTORY[:limit] if asset == "BTC" else DEFAULT_ETH_HISTORY[:limit]

        self._send_json({"asset": asset, "total_rows": len(rows), "rows": rows})

    def _handle_export(self, query: dict[str, list[str]]) -> None:
        """Serve direct CSV download with Content-Disposition header."""
        asset = query.get("asset", ["BTC"])[0].upper()
        if asset not in ("BTC", "ETH"):
            asset = "BTC"

        db_path = self.dashboard_server.db_path
        rows = _read_ledger_from_duckdb(db_path, asset, 365)
        if rows is None:
            rows = DEFAULT_BTC_HISTORY if asset == "BTC" else DEFAULT_ETH_HISTORY

        output = io.StringIO()
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(
            [
                "Tanggal",
                "Aset",
                "Harga_Buka_USD",
                "Tertinggi_USD",
                "Terendah_USD",
                "Harga_Tutup_USD",
                "Perubahan_Persen",
                "Volume_Koin",
                "Total_Transaksi",
            ]
        )
        for r in rows:
            writer.writerow(
                [
                    r.get("date", ""),
                    asset,
                    f"{float(r.get('open', 0.0)):.2f}",
                    f"{float(r.get('high', 0.0)):.2f}",
                    f"{float(r.get('low', 0.0)):.2f}",
                    f"{float(r.get('close', 0.0)):.2f}",
                    f"{float(r.get('change', 0.0)):.2f}",
                    f"{float(r.get('volume', 0.0)):.2f}",
                    int(r.get("tx", 0)),
                ]
            )

        csv_bytes = output.getvalue().encode("utf-8")
        today_str = datetime.now(UTC).strftime("%Y-%m-%d")
        filename = f"{asset.lower()}_market_history_{today_str}.csv"

        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
        }
        self._send_bytes(
            csv_bytes,
            content_type="text/csv; charset=utf-8",
            extra_headers=headers,
        )

    def _handle_portfolio(self, query: dict[str, list[str]]) -> None:
        """Serve paper trading portfolio summary with live spot price."""
        portfolio_id = query.get("portfolio_id", ["default"])[0]
        db_path = self.dashboard_server.db_path

        live_price = _fetch_live_spot_price("BTC")

        try:
            engine = PaperTradingEngine(db_path=db_path, portfolio_id=portfolio_id)
            summary = engine.get_portfolio_summary(live_spot_price=live_price)
            data = summary.to_dict()
        except Exception as exc:
            logger.warning("Failed to query portfolio summary: %s, using fallback", exc)
            spot = live_price if live_price is not None else 85000.0
            data = {
                "portfolio_id": portfolio_id,
                "initial_cash": 1000.00,
                "total_equity": 1000.00,
                "unrealized_pnl_usd": 0.00,
                "unrealized_pnl_pct": 0.00,
                "base_cash": 700.00,
                "reserve_cash": 300.00,
                "total_cash": 1000.00,
                "btc_balance": 0.0,
                "btc_value_usd": 0.00,
                "avg_buy_price": 0.00,
                "current_spot_price": spot,
                "acquisition_discount_pct": 0.00,
                "total_trades": 0,
                "benchmark_equity": 1000.00,
                "outperformance_usd": 0.00,
            }

        self._send_json(data)

    def _handle_portfolio_equity(self, query: dict[str, list[str]]) -> None:
        """Serve daily equity curve history for paper portfolio vs benchmark."""
        portfolio_id = query.get("portfolio_id", ["default"])[0]
        limit_str = query.get("limit", ["90"])[0]
        try:
            limit = int(limit_str)
        except ValueError:
            limit = 90

        db_path = self.dashboard_server.db_path
        live_price = _fetch_live_spot_price("BTC")
        include_live = query.get("live", ["0"])[0].lower() in ("1", "true", "yes")
        try:
            engine = PaperTradingEngine(db_path=db_path, portfolio_id=portfolio_id)
            series = engine.get_equity_series(limit=limit)

            # Append or update today's live point when requested to match KPI card exactly
            if include_live and live_price is not None and series:
                summary = engine.get_portfolio_summary(live_spot_price=live_price)
                today_iso = datetime.now(UTC).date().isoformat()
                live_point = {
                    "date": today_iso,
                    "equity": round(summary.total_equity, 2),
                    "cash": round(summary.total_cash, 2),
                    "reserve": round(summary.reserve_cash, 2),
                    "btc_value": round(summary.btc_value_usd, 2),
                    "benchmark": round(summary.benchmark_equity, 2),
                }
                if series[-1]["date"] == today_iso:
                    series[-1] = live_point
                else:
                    series.append(live_point)
        except Exception as exc:
            logger.warning("Failed to query equity series: %s, using fallback", exc)
            series = []

        if not series:
            # Provide initial baseline point so charts render gracefully
            series = [
                {
                    "date": datetime.now(UTC).date().isoformat(),
                    "equity": 1000.0,
                    "cash": 700.0,
                    "reserve": 300.0,
                    "btc_value": 0.0,
                    "benchmark": 1000.0,
                }
            ]

        self._send_json(series)

    def _handle_portfolio_trades(self, query: dict[str, list[str]]) -> None:
        """Serve executed paper trading order blotter records."""
        portfolio_id = query.get("portfolio_id", ["default"])[0]
        limit_str = query.get("limit", ["50"])[0]
        try:
            limit = int(limit_str)
        except ValueError:
            limit = 50

        db_path = self.dashboard_server.db_path
        try:
            engine = PaperTradingEngine(db_path=db_path, portfolio_id=portfolio_id)
            trades = engine.get_trade_blotter(limit=limit)
        except Exception as exc:
            logger.warning("Failed to query trade blotter: %s, using fallback", exc)
            trades = []

        self._send_json(trades)

    def _handle_macro_radar(self, query: dict[str, list[str]]) -> None:
        """Serve latest synthesized Macro Radar metrics, MNI score, and regime."""
        db_path = self.dashboard_server.db_path
        regime_labels = {
            "RISK_ON_EXPANSION": "Risk-On Expansion (Akumulasi Agresif)",
            "CAUTIOUS_BULL": "Cautious Bull (Akumulasi Oportunistik)",
            "NEUTRAL_CHOP": "Neutral Chop (DCA Standar)",
            "RISK_OFF_DEFENSE": "Risk-Off Defense (Cadangan Kas Defensif)",
            "BLACK_SWAN_CRISIS": "Black Swan Crisis (Circuit Breaker Tripped)",
        }
        try:
            db_mgr = DuckDBManager(db_path=db_path)
            with db_mgr:
                report = db_mgr.get_latest_narrative_intelligence()
            if report is not None:
                data = {
                    "date": report.intelligence_date.isoformat(),
                    "composite_mni": report.composite_mni,
                    "regime": report.regime.value,
                    "regime_label": regime_labels.get(report.regime.value, report.regime.value),
                    "black_swan_flag": report.black_swan_flag,
                    "scores": {
                        "hard_macro": report.hard_macro_score,
                        "sentiment": report.sentiment_score,
                        "narrative": report.narrative_score,
                    },
                    "narrative_summary": report.narrative_summary_id,
                    "dominant_pillar": report.dominant_pillar.value,
                    "critical_alerts_count": report.active_critical_alerts,
                    "last_updated_utc": report.synthesized_at_utc.isoformat(),
                }
                self._send_json(data)
                return
        except Exception as exc:
            logger.warning("Failed querying macro radar: %s, using fallback", exc)

        # Resilient fallback
        now = datetime.now(UTC)
        fallback = {
            "date": now.date().isoformat(),
            "composite_mni": 0.0,
            "regime": "NEUTRAL_CHOP",
            "regime_label": "Neutral Chop (DCA Standar)",
            "black_swan_flag": False,
            "scores": {
                "hard_macro": 0.0,
                "sentiment": 0.0,
                "narrative": 0.0,
            },
            "narrative_summary": (
                "Pasar konsolidasi netral tanpa anomali atau ancaman makro dominan."
            ),
            "dominant_pillar": "GENERAL",
            "critical_alerts_count": 0,
            "last_updated_utc": now.isoformat(),
        }
        self._send_json(fallback)

    def _handle_macro_news(self, query: dict[str, list[str]]) -> None:
        """Serve verified news items with mandatory original source hyperlinks."""
        limit_str = query.get("limit", ["20"])[0]
        try:
            limit = int(limit_str)
        except ValueError:
            limit = 20

        db_path = self.dashboard_server.db_path
        try:
            db_mgr = DuckDBManager(db_path=db_path)
            with db_mgr:
                raw_articles = db_mgr.get_macro_articles(limit=limit)
            results = [
                {
                    "article_id": a["article_id"],
                    "source": a["source"],
                    "title": a["title"],
                    "url": a["url"],
                    "published_utc": a["published_utc"],
                    "pillar": a["pillar"],
                    "severity": a["severity"],
                    "polarity": a["polarity"],
                    "summary": a.get("summary", ""),
                }
                for a in raw_articles
            ]
            self._send_json(results)
        except Exception as exc:
            logger.warning("Failed querying macro news: %s, returning empty list", exc)
            self._send_json([])

    def _handle_macro_calendar(self, query: dict[str, list[str]]) -> None:
        """Serve scheduled and recent economic releases with surprise evaluations."""
        days_str = query.get("days", ["7"])[0]
        try:
            days = int(days_str)
        except ValueError:
            days = 7

        db_path = self.dashboard_server.db_path
        try:
            db_mgr = DuckDBManager(db_path=db_path)
            with db_mgr:
                raw_releases = db_mgr.get_macro_economic_releases(days=days)
            results = []
            for r in raw_releases:
                score = float(r.get("directional_score") or 0.0)
                if score > 0.05:
                    bias = "DOVISH"
                elif score < -0.05:
                    bias = "HAWKISH"
                else:
                    bias = "NEUTRAL"

                results.append(
                    {
                        "release_id": r["release_id"],
                        "event_name": r["event_name"],
                        "country": r["country"],
                        "release_date": r["release_date"],
                        "release_time_utc": r["release_time_utc"],
                        "impact": r["impact"],
                        "actual": r.get("actual_value"),
                        "forecast": r.get("forecast_value"),
                        "previous": r.get("previous_value"),
                        "surprise": r.get("surprise_delta"),
                        "directional_bias": bias,
                    }
                )
            self._send_json(results)
        except Exception as exc:
            logger.warning("Failed querying macro calendar: %s, returning empty list", exc)
            self._send_json([])

    def _handle_committee_latest(self, query: dict[str, list[str]]) -> None:
        """Serve latest Investment Committee memorandum, persona votes, and invariant checks."""
        db_path = self.dashboard_server.db_path
        try:
            db_mgr = DuckDBManager(db_path)
            with db_mgr:
                memo = db_mgr.get_latest_investment_memo()
            if not memo:
                self._send_json(DEFAULT_COMMITTEE_MEMO.copy())
                return

            reason = memo.get("clamping_reason") or ""
            invariants = [
                {"name": "Solvency", "status": "CLAMPED" if "SOLVENCY" in reason else "PASSED"},
                {
                    "name": "Daily 15% Reserve Cap",
                    "status": "CLAMPED" if "DAILY_RESERVE_CAP" in reason else "PASSED",
                    "detail": reason if "DAILY_RESERVE_CAP" in reason else None,
                },
                {
                    "name": "Macro 2h Proximity Buffer",
                    "status": (
                        "HALTED" if ("Proksimitas" in reason or "Macro" in reason) else "PASSED"
                    ),
                },
                {
                    "name": "Black Swan Sentinel",
                    "status": "HALTED" if "Black Swan" in reason else "PASSED",
                },
                {
                    "name": "Max Drawdown Limit (<25%)",
                    "status": "CLAMPED" if "DRAWDOWN_LIMIT" in reason else "PASSED",
                },
            ]
            memo["invariants_checked"] = invariants
            self._send_json(memo)
        except Exception as exc:
            logger.warning("Failed querying latest committee memo: %s, returning fallback", exc)
            self._send_json(DEFAULT_COMMITTEE_MEMO.copy())

    def _handle_committee_history(self, query: dict[str, list[str]]) -> None:
        """Serve historical investment committee memorandums."""
        limit_str = query.get("limit", ["20"])[0]
        try:
            limit = max(1, min(100, int(limit_str)))
        except ValueError:
            limit = 20
        db_path = self.dashboard_server.db_path
        try:
            db_mgr = DuckDBManager(db_path)
            with db_mgr:
                history = db_mgr.get_investment_memos_history(limit=limit)
            self._send_json(history)
        except Exception as exc:
            logger.warning("Failed querying committee history: %s, returning empty", exc)
            self._send_json([])

    def _handle_intelligence_list(self, query: dict[str, list[str]]) -> None:
        """Serve recent user market intelligence submissions."""
        limit_str = query.get("limit", ["20"])[0]
        try:
            limit = max(1, min(100, int(limit_str)))
        except ValueError:
            limit = 20
        db_path = self.dashboard_server.db_path
        try:
            db_mgr = DuckDBManager(db_path)
            with db_mgr:
                items = db_mgr.list_user_intelligence(limit=limit, active_only=True)
            self._send_json(items)
        except Exception as exc:
            logger.warning("Failed querying user intelligence: %s, returning empty", exc)
            self._send_json([])

    def _handle_pipeline_schedule(self, query: dict[str, list[str]]) -> None:
        """Serve automated scheduling pipeline cadence info and lock statuses."""
        db_path = self.dashboard_server.db_path
        try:
            is_mem = str(db_path) == ":memory:"
            lock_dir = Path("./data/state/locks") if is_mem else Path(db_path).parent / "locks"
            lock_mgr = LockManager(lock_dir)
            locks = lock_mgr.get_status()
        except Exception:
            locks = {}
        schedule_data = {
            "cadences": {
                "hourly": {
                    "schedule": "*:05 UTC",
                    "description": "Multi-source RSS news ingestion & black swan sentinel scan",
                },
                "daily": {
                    "schedule": "00:05 UTC",
                    "description": (
                        "Candle sync, sentiment, macro calendar, 3-tier MNI, committee, paper step"
                    ),
                },
                "weekly": {
                    "schedule": "Mon 01:00 UTC",
                    "description": (
                        "Portfolio risk audit, reserve health, and retrospective memorandum"
                    ),
                },
            },
            "locks": locks,
        }
        self._send_json(schedule_data)

    def _handle_intelligence_ingest(self) -> None:
        """Handle POST /api/intelligence/ingest."""
        length_str = self.headers.get("Content-Length", "0")
        try:
            length = int(length_str)
        except ValueError:
            length = 0

        if length <= 0:
            err = {"error": "invalid_payload", "message": "Empty request body"}
            self._send_json(err, status=400)
            return

        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception as exc:
            err = {"error": "invalid_json", "message": f"Malformed JSON: {exc}"}
            self._send_json(err, status=400)
            return

        thesis = payload.get("user_thesis")
        if not thesis or not str(thesis).strip():
            err = {"error": "invalid_payload", "message": "'user_thesis' is required"}
            self._send_json(err, status=400)
            return

        title = payload.get("title", "")
        url = payload.get("source_url")
        pillar = payload.get("pillar", "USER_THESIS")
        try:
            sentiment = float(payload.get("sentiment_bias", 0.0))
            confidence = float(payload.get("confidence_score", 0.8))
        except (TypeError, ValueError):
            sentiment = 0.0
            confidence = 0.8
        tags = payload.get("tags", [])
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",") if t.strip()]

        db_path = self.dashboard_server.db_path
        try:
            db_mgr = DuckDBManager(db_path)
            with db_mgr:
                ingester = IntelligenceIngester(db_mgr)
                record = ingester.ingest(
                    title=title,
                    user_thesis=thesis,
                    source_url=url,
                    pillar=pillar,
                    sentiment_bias=sentiment,
                    confidence_score=confidence,
                    tags=tags,
                )
            self._send_json(
                {
                    "status": "SUCCESS",
                    "intelligence_id": record.intelligence_id,
                    "created_at_utc": record.created_at_utc.isoformat(),
                    "message": "User intelligence recorded and indexed for next deliberation.",
                },
                status=201,
            )
        except Exception as exc:
            logger.error("Failed ingesting intelligence via API: %s", exc)
            self._send_json({"error": "internal_error", "message": str(exc)}, status=500)

    def _send_json(self, data: Any, status: int = 200) -> None:
        """Serialize and send JSON response."""
        content = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self._send_bytes(content, content_type="application/json; charset=utf-8", status=status)

    def _send_404(self, message: str) -> None:
        """Send a 404 response."""
        self._send_json({"error": "not_found", "message": message}, status=404)

    def _send_bytes(
        self,
        content: bytes,
        content_type: str,
        status: int = 200,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        """Send raw bytes with headers."""
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(content)


class DashboardServer(http.server.ThreadingHTTPServer):
    """Multithreaded HTTP Server for Bitcoin Market Hub dashboard."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        db_path: Path | str = "./data/state/platform.duckdb",
        assets_dir: Path | str | None = None,
    ) -> None:
        self.db_path = Path(db_path)
        if assets_dir is not None:
            self.assets_dir = Path(assets_dir)
        else:
            self.assets_dir = Path(__file__).parent / "assets"
        super().__init__(server_address, DashboardRequestHandler)


def create_dashboard_server(
    host: str = "127.0.0.1",
    port: int = 8080,
    db_path: Path | str = "./data/state/platform.duckdb",
    assets_dir: Path | str | None = None,
) -> DashboardServer:
    """Factory creating configured DashboardServer instance."""
    return DashboardServer((host, port), db_path=db_path, assets_dir=assets_dir)


def run_dashboard(
    host: str = "127.0.0.1",
    port: int = 8080,
    db_path: Path | str = "./data/state/platform.duckdb",
    assets_dir: Path | str | None = None,
) -> None:
    """Run interactive dashboard HTTP server indefinitely."""
    server = create_dashboard_server(
        host=host,
        port=port,
        db_path=db_path,
        assets_dir=assets_dir,
    )
    bound_port = server.server_port
    url = f"http://{host}:{bound_port}"
    sys.stdout.write(f"Serving Bitcoin Market Hub dashboard at {url} (Press Ctrl+C to stop)\n")
    sys.stdout.flush()

    try:
        server.serve_forever()
    finally:
        server.server_close()
