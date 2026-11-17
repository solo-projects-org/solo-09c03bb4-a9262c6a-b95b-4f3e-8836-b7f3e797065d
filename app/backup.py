"""备机 CSV 解析。

格式：两列 —— UTC 时间、温度。
- 支持表头（中/英文别名，列序任意），也支持无表头（UTC 在前、温度在后）。
- 行号为数据行序号（从 1 起，不含表头）。
- 同一 UTC 出现多行时保留全部（合并阶段据此判定异温冲突）。
"""
from __future__ import annotations

import csv
import io
import math

from .timeutil import parse_utc

HEADER_ALIASES = {
    "utc": {"utc", "utc_time", "time", "timestamp", "datetime", "utc时间", "时间", "时间utc"},
    "temperature": {"temperature", "temp", "温度"},
}


def _looks_time(cell: str) -> bool:
    try:
        parse_utc(cell)
        return True
    except ValueError:
        return False


def _looks_numeric(cell: str) -> bool:
    try:
        float(cell.strip())
        return True
    except ValueError:
        return False


def _match_header(cells: list[str]) -> dict[str, int] | None:
    norm = [c.strip().lower() for c in cells]
    mapping: dict[str, int] = {}
    for field, aliases in HEADER_ALIASES.items():
        for idx, cell in enumerate(norm):
            if cell in aliases:
                mapping[field] = idx
                break
        else:
            return None
    return mapping


def parse_backup_csv(text: str, max_rows: int):
    """返回 (rows, errors)。

    rows: [{"row_no", "utc", "temperature"}, ...]
    errors: [{"row", "message"}, ...]；有任何错误时 rows 为空（整批拒绝）。
    """
    errors: list[dict] = []
    rows: list[dict] = []

    records = [rec for rec in csv.reader(io.StringIO(text)) if any(c.strip() for c in rec)]
    if not records:
        return [], [{"row": 0, "message": "CSV 内容为空"}]

    header = _match_header(records[0])
    if header is not None:
        data_records = records[1:]
        col = header
    elif records[0] and not _looks_time(records[0][0]):
        return [], [{
            "row": 0,
            "message": "无法识别表头：需要列 utc_time,temperature"
            "（或中文别名 UTC时间,温度）；无表头时按此顺序排列",
        }]
    else:
        data_records = records
        col = {"utc": 0, "temperature": 1}

    if not data_records:
        return [], [{"row": 0, "message": "CSV 中没有数据行"}]

    for row_no, rec in enumerate(data_records, start=1):
        if len(rows) >= max_rows:
            errors.append({"row": row_no, "message": f"超过单文件最大行数 {max_rows}"})
            break
        if len(rec) <= max(col.values()):
            errors.append({"row": row_no, "message": "列数不足，应为 2 列"})
            continue

        utc_raw = rec[col["utc"]].strip()
        tmp_raw = rec[col["temperature"]].strip()

        try:
            utc = parse_utc(utc_raw)
        except ValueError:
            errors.append({"row": row_no, "message": f"UTC 时间无法解析，实际为 {utc_raw!r}"})
            continue
        try:
            temperature = float(tmp_raw)
            if not math.isfinite(temperature):
                raise ValueError
        except ValueError:
            errors.append({"row": row_no, "message": f"温度须为有限数值，实际为 {tmp_raw!r}"})
            continue

        rows.append({"row_no": row_no, "utc": utc, "temperature": temperature})

    if errors:
        return [], errors
    return rows, []
