"""Multi-format analytical exporter for Arrow Tables."""

import contextlib
import json
import os
import uuid
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.ipc as paipc
import pyarrow.parquet as pq

ExportFormat = Literal["json", "csv", "parquet", "arrow", "ipc"]
SUPPORTED_FORMATS: set[str] = {"json", "csv", "parquet", "arrow", "ipc"}


def export_arrow_table(
    table: pa.Table,
    fmt: str,
    output_path: Path | str | None = None,
) -> bytes | Path:
    """Export PyArrow Table to desired format (json, csv, parquet, arrow).

    If output_path is specified, performs an atomic write (.tmp followed by os.replace)
    and returns the resulting Path. If output_path is None, returns serialized bytes.
    """
    normalized_fmt = fmt.lower().strip()
    if normalized_fmt not in SUPPORTED_FORMATS:
        supported = ", ".join(sorted(["json", "csv", "parquet", "arrow"]))
        raise ValueError(f"Unsupported export format: '{fmt}'. Supported formats are: {supported}")

    if output_path is not None:
        target_path = Path(output_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target_path.with_name(f".{target_path.name}.{uuid.uuid4().hex}.tmp")

        try:
            if normalized_fmt == "parquet":
                pq.write_table(table, tmp_path, compression="snappy")
            elif normalized_fmt in ("arrow", "ipc"):
                with paipc.new_file(str(tmp_path), table.schema) as writer:
                    writer.write_table(table)
            elif normalized_fmt == "csv":
                pacsv.write_csv(table, tmp_path)
            elif normalized_fmt == "json":
                json_str = json.dumps(table.to_pylist(), default=str, indent=2)
                tmp_path.write_text(json_str, encoding="utf-8")

            os.replace(tmp_path, target_path)
            return target_path
        except Exception:
            if tmp_path.exists():
                with contextlib.suppress(OSError):
                    tmp_path.unlink()
            raise

    # In-memory export
    if normalized_fmt == "parquet":
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="snappy")
        return bytes(sink.getvalue().to_pybytes())

    if normalized_fmt in ("arrow", "ipc"):
        sink = pa.BufferOutputStream()
        with paipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        return bytes(sink.getvalue().to_pybytes())

    if normalized_fmt == "csv":
        sink = pa.BufferOutputStream()
        pacsv.write_csv(table, sink)
        return bytes(sink.getvalue().to_pybytes())

    # json format
    json_bytes = json.dumps(table.to_pylist(), default=str, indent=2).encode("utf-8")
    return json_bytes


# Semantic alias
export_data = export_arrow_table
