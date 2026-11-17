"""备机补数：备机文件版本、冲突判定与「主机 + 备机」合并快照。

规则（与 README 一致）：
- 备机点只有在断流闭区间内、且距任一主机已发布读数严格超过 5 秒时才补入；
  主机读数优先，备机不能覆盖主机。
- 备机文件内同一 UTC 存在不同温度 → 该断流区间不可交付，冲突原样列出。
- 每次合并结果都是不可变快照，绑定「一个主机已发布校时版本 + 一个备机文件版本」；
  合并提交事务内复核备机/主机变更代次，过期则整次丢弃重算，绝不混版。
- 主机重算未完成、备机文件已更换或已有更新合并结果时，旧结果仅可回看
  （``deliverable=False`` 且给出原因）。
"""
from __future__ import annotations

import bisect
import json
import logging
from datetime import datetime, timezone

from .calibration import StaleState
from .db import Database
from .timeutil import fmt_utc, parse_utc, utcnow_iso

log = logging.getLogger("merge")

MIN_GAP_MS = 5000  # 备机点距主机点须严格超过 5 秒


class WaitForMaster(Exception):
    """主机存在尚未重算/发布的变更，暂不生成合并结果。"""


class UnknownMergeVersion(Exception):
    """请求回看的合并版本不存在。"""


class BadMergeDate(ValueError):
    """日期格式不合法。"""


def utc_ms(dt: datetime) -> int:
    return round(dt.timestamp() * 1000)


def _temp_key(value: float) -> float:
    return round(float(value), 9)


# ---------------------------------------------------------------- 备机文件

def _analyze_backup_rows(rows: list[dict], start_ms: int, end_ms: int):
    """把解析后的备机行整理为 (待入库行, 冲突列表)。

    冲突：同一 UTC（毫秒精度）在断流区间内出现不同温度。
    同一 UTC 同温的重复行不算冲突，入库后合并时只保留最小行号。
    """
    prepared = []
    groups: dict[int, list[dict]] = {}
    for r in rows:
        ms = utc_ms(r["utc"])
        inside = start_ms <= ms <= end_ms
        item = {
            "row_no": r["row_no"],
            "utc": r["utc"],
            "utc_time": fmt_utc(r["utc"]),
            "utc_ms": ms,
            "temperature": float(r["temperature"]),
            "in_interval": 1 if inside else 0,
        }
        prepared.append(item)
        if inside:
            groups.setdefault(ms, []).append(item)

    conflicts = []
    for ms in sorted(groups):
        group = sorted(groups[ms], key=lambda x: x["row_no"])
        if len({_temp_key(x["temperature"]) for x in group}) > 1:
            conflicts.append({
                "utc_ms": ms,
                "utc_time": group[0]["utc_time"],
                "details": [
                    {"row_no": x["row_no"], "utc_time": x["utc_time"],
                     "temperature": x["temperature"]}
                    for x in group
                ],
            })
    return prepared, conflicts


def replace_backup_file(
    db: Database,
    filename: str,
    start_dt: datetime,
    end_dt: datetime,
    rows: list[dict],
    max_points: int,
) -> dict:
    """整批替换备机文件。

    新文件在单个事务内插入并切换为 active；校验在调用前完成，
    因此任何失败都不会影响当前生效的旧文件。
    """
    if not rows:
        raise ValueError("没有可入库的备机数据行")
    if len(rows) > max_points:
        raise ValueError(f"备机文件最多 {max_points} 点")
    start_ms, end_ms = utc_ms(start_dt), utc_ms(end_dt)
    if start_ms > end_ms:
        raise ValueError("断流区间起点不能晚于终点")

    prepared, conflicts = _analyze_backup_rows(rows, start_ms, end_ms)
    in_interval = sum(1 for r in prepared if r["in_interval"])

    with db.tx() as cur:
        now = utcnow_iso()
        file_id = cur.execute(
            "INSERT INTO backup_files(filename, created_at, interval_start, interval_end,"
            " interval_start_ms, interval_end_ms, rows_total, rows_in_interval,"
            " conflict_count, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
            (filename[:200], now, fmt_utc(start_dt), fmt_utc(end_dt), start_ms, end_ms,
             len(prepared), in_interval, len(conflicts)),
        ).lastrowid
        cur.executemany(
            "INSERT INTO backup_rows(file_id, row_no, utc_time, utc_ms, temperature, in_interval)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            [
                (file_id, r["row_no"], r["utc_time"], r["utc_ms"], r["temperature"],
                 r["in_interval"])
                for r in prepared
            ],
        )
        cur.executemany(
            "INSERT INTO backup_conflicts(file_id, utc_time, utc_ms, details)"
            " VALUES (?, ?, ?, ?)",
            [
                (file_id, c["utc_time"], c["utc_ms"],
                 json.dumps(c["details"], ensure_ascii=False))
                for c in conflicts
            ],
        )
        cur.execute("UPDATE backup_files SET is_active = 0 WHERE is_active = 1")
        cur.execute("UPDATE backup_files SET is_active = 1 WHERE id = ?", (file_id,))
        cur.execute(
            "UPDATE meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT)"
            " WHERE key = 'merge_pending_rev'"
        )
    return {
        "file_id": file_id,
        "active": True,
        "rows_total": len(prepared),
        "rows_in_interval": in_interval,
        "rows_outside_interval": len(prepared) - in_interval,
        "conflict_count": len(conflicts),
        "conflicts": conflicts,
    }


def active_backup_file(db: Database) -> dict | None:
    row = db.query_one("SELECT * FROM backup_files WHERE is_active = 1")
    return dict(row) if row else None


def list_backup_files(db: Database, limit: int = 20) -> list[dict]:
    return [dict(r) for r in db.query(
        "SELECT * FROM backup_files ORDER BY id DESC LIMIT ?", (limit,)
    )]


def list_conflicts(db: Database, file_id: int) -> list[dict]:
    return [
        {
            "utc_time": c["utc_time"],
            "utc_ms": c["utc_ms"],
            "details": json.loads(c["details"]),
        }
        for c in db.query(
            "SELECT * FROM backup_conflicts WHERE file_id = ? ORDER BY utc_ms",
            (file_id,),
        )
    ]


def list_merged_results(db: Database, limit: int = 20) -> list[dict]:
    return [dict(r) for r in db.query(
        "SELECT * FROM merged_results ORDER BY version DESC LIMIT ?", (limit,)
    )]


# ---------------------------------------------------------------- 合并计算

def load_merge_snapshot(db: Database):
    """在锁内一次性读取主机发布版、备机 active 文件与合并代次。"""
    def fn(conn):
        meta = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM meta")}
        state_gen = int(meta.get("state_generation", "0"))
        calibrated_gen = int(meta.get("calibrated_generation", "-1"))
        pending_rev = int(meta.get("merge_pending_rev", "0"))
        built_rev = int(meta.get("merge_built_rev", "0"))
        calc_version = conn.execute(
            "SELECT version FROM calc_state WHERE id = 1"
        ).fetchone()["version"]
        if state_gen != calibrated_gen:
            # 主机有变更尚未重算：此刻只能回看旧合并结果
            raise WaitForMaster(
                f"主机待重算（代次 {state_gen} != {calibrated_gen}）"
            )
        active = conn.execute("SELECT * FROM backup_files WHERE is_active = 1").fetchone()
        master = [dict(r) for r in conn.execute(
            """
            SELECT c.reading_id, c.utc_date, c.anchor_version,
                   r.batch_id, r.row_no, r.segment, r.local_seconds, r.temperature,
                   c.utc_time
            FROM calibrated c JOIN readings r ON r.id = c.reading_id
            ORDER BY c.utc_time, c.reading_id
            """
        )]
        backup_rows = []
        if active is not None:
            backup_rows = [dict(r) for r in conn.execute(
                "SELECT * FROM backup_rows WHERE file_id = ? ORDER BY row_no",
                (active["id"],),
            )]
        return {
            "pending_rev": pending_rev,
            "built_rev": built_rev,
            "calc_version": calc_version,
            "active": dict(active) if active else None,
            "master": master,
            "backup_rows": backup_rows,
        }

    return db.snapshot(fn)


def compute_merged(master: list[dict], backup_rows: list[dict],
                   start_ms: int, end_ms: int) -> dict:
    """纯函数：生成合并读数与统计（不含提交）。"""
    master_rows = []
    master_ms = []
    for m in master:
        ms = utc_ms(parse_utc(m["utc_time"]))
        master_ms.append(ms)
        master_rows.append({
            "source": "master",
            "source_ref": m["reading_id"],
            "batch_id": m["batch_id"],
            "backup_file_id": None,
            "row_no": m["row_no"],
            "segment": m["segment"],
            "local_seconds": m["local_seconds"],
            "temperature": m["temperature"],
            "utc_time": m["utc_time"],
            "utc_ms": ms,
            "utc_date": m["utc_date"],
            "anchor_version": m["anchor_version"],
            "calc_version": None,  # 由 build_and_commit 填为主机发布版
        })
    master_ms.sort()

    groups: dict[int, list[dict]] = {}
    omitted_outside = 0
    for r in backup_rows:
        if not r["in_interval"] or not (start_ms <= r["utc_ms"] <= end_ms):
            omitted_outside += 1
            continue
        groups.setdefault(r["utc_ms"], []).append(r)

    conflicts = []
    backup_used = []
    omitted_conflict = omitted_duplicate = omitted_near = 0
    for ms in sorted(groups):
        group = sorted(groups[ms], key=lambda x: x["row_no"])
        if len({_temp_key(x["temperature"]) for x in group}) > 1:
            omitted_conflict += len(group)
            conflicts.append({
                "utc_ms": ms,
                "utc_time": group[0]["utc_time"],
                "details": [
                    {"row_no": x["row_no"], "utc_time": x["utc_time"],
                     "temperature": x["temperature"]}
                    for x in group
                ],
            })
            continue
        keep = group[0]
        omitted_duplicate += len(group) - 1
        idx = bisect.bisect_left(master_ms, ms)
        near = []
        if idx < len(master_ms):
            near.append(master_ms[idx] - ms)
        if idx > 0:
            near.append(ms - master_ms[idx - 1])
        if near and min(near) <= MIN_GAP_MS:
            omitted_near += 1
            continue
        backup_used.append({
            "source": "backup",
            "source_ref": keep["id"],
            "batch_id": None,
            "backup_file_id": keep["file_id"],
            "row_no": keep["row_no"],
            "segment": None,
            "local_seconds": None,
            "temperature": keep["temperature"],
            "utc_time": keep["utc_time"],
            "utc_ms": keep["utc_ms"],
            "utc_date": datetime.fromtimestamp(
                keep["utc_ms"] / 1000, tz=timezone.utc
            ).date().isoformat(),
            "anchor_version": None,
            "calc_version": None,
        })

    rows = master_rows + backup_used
    rows.sort(key=lambda r: (r["utc_ms"], 0 if r["source"] == "master" else 1, r["row_no"]))
    return {
        "rows": rows,
        "conflicts": conflicts,
        "status": "undeliverable" if conflicts else "ready",
        "deliverable": 0 if conflicts else 1,
        "master_count": len(master_rows),
        "backup_count": len(backup_used),
        "omitted_near_master": omitted_near,
        "omitted_outside": omitted_outside,
        "omitted_duplicate": omitted_duplicate,
        "omitted_conflict": omitted_conflict,
        "conflict_count": len(conflicts),
    }


def build_and_commit(db: Database, captured_rev: int) -> int | None:
    """按一致快照合并并原子提交；主机/备机变化则整次作废。"""
    snap = load_merge_snapshot(db)
    active = snap["active"]
    if active is None:
        # 没有备机文件：不产生合并结果，只记录本次代次已处理
        with db.tx() as cur:
            cur_rev = int(cur.execute(
                "SELECT value FROM meta WHERE key = 'merge_pending_rev'"
            ).fetchone()["value"])
            if cur_rev != captured_rev:
                raise StaleState(f"合并代次已变化：{captured_rev} -> {cur_rev}")
            cur.execute("UPDATE meta SET value = ? WHERE key = 'merge_built_rev'",
                        (str(captured_rev),))
            cur.execute(
                "UPDATE merge_state SET status = 'idle', last_error = '', updated_at = ?"
                " WHERE id = 1", (utcnow_iso(),),
            )
        return None

    result = compute_merged(snap["master"], snap["backup_rows"],
                            active["interval_start_ms"], active["interval_end_ms"])
    now = utcnow_iso()
    with db.tx() as cur:
        cur_rev = int(cur.execute(
            "SELECT value FROM meta WHERE key = 'merge_pending_rev'"
        ).fetchone()["value"])
        if cur_rev != captured_rev:
            raise StaleState(f"合并代次已变化：{captured_rev} -> {cur_rev}")
        # 再次确认主机发布版未变（代次复核的兜底）
        cur_calc = cur.execute(
            "SELECT version FROM calc_state WHERE id = 1"
        ).fetchone()["version"]
        if cur_calc != snap["calc_version"]:
            raise StaleState("主机校时版本已变化")
        # 再次确认备机文件未被替换（与代次复核互为兜底）
        cur_active = cur.execute(
            "SELECT id FROM backup_files WHERE is_active = 1"
        ).fetchone()
        if cur_active is None or cur_active["id"] != active["id"]:
            raise StaleState("备机文件已更换")
        version = cur.execute(
            "INSERT INTO merged_results(master_calc_version, backup_file_id,"
            " interval_start, interval_end, status, deliverable, conflict_count,"
            " master_count, backup_count, omitted_near_master, omitted_outside,"
            " omitted_duplicate, omitted_conflict, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (snap["calc_version"], active["id"], active["interval_start"],
             active["interval_end"], result["status"], result["deliverable"],
             result["conflict_count"], result["master_count"], result["backup_count"],
             result["omitted_near_master"], result["omitted_outside"],
             result["omitted_duplicate"], result["omitted_conflict"], now),
        ).lastrowid
        cur.executemany(
            "INSERT INTO merged_readings(merged_version, source, source_ref, batch_id,"
            " backup_file_id, row_no, segment, local_seconds, temperature, utc_time,"
            " utc_ms, utc_date, anchor_version, calc_version)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (version, r["source"], r["source_ref"], r["batch_id"],
                 r["backup_file_id"], r["row_no"], r["segment"], r["local_seconds"],
                 r["temperature"], r["utc_time"], r["utc_ms"], r["utc_date"],
                 r["anchor_version"],
                 snap["calc_version"] if r["source"] == "master" else None)
                for r in result["rows"]
            ],
        )
        cur.executemany(
            "INSERT INTO merged_conflicts(merged_version, utc_time, utc_ms, details)"
            " VALUES (?, ?, ?, ?)",
            [
                (version, c["utc_time"], c["utc_ms"],
                 json.dumps(c["details"], ensure_ascii=False))
                for c in result["conflicts"]
            ],
        )
        cur.execute(
            "UPDATE merge_state SET version = ?, status = 'idle', last_error = '',"
            " updated_at = ? WHERE id = 1",
            (version, now),
        )
        cur.execute("UPDATE meta SET value = ? WHERE key = 'merge_built_rev'",
                    (str(captured_rev),))
    return version


# ---------------------------------------------------------------- 查询

def merge_info(db: Database) -> dict:
    state = db.query_one("SELECT * FROM merge_state WHERE id = 1")
    meta = {r["key"]: r["value"] for r in db.query("SELECT key, value FROM meta")}
    latest = db.query_one(
        "SELECT * FROM merged_results ORDER BY version DESC LIMIT 1"
    )
    calc_version = db.query_one("SELECT version FROM calc_state WHERE id = 1")["version"]
    active = active_backup_file(db)
    pending = meta.get("merge_pending_rev", "0") != meta.get("merge_built_rev", "0")
    dirty = meta.get("state_generation", "0") != meta.get("calibrated_generation", "-1")
    current_ok = (
        latest is not None
        and not pending
        and not dirty
        and active is not None
        and latest["backup_file_id"] == active["id"]
        and latest["master_calc_version"] == calc_version
        and latest["version"] == state["version"]
    )
    return {
        "version": state["version"],
        "status": state["status"],
        "last_error": state["last_error"],
        "updated_at": state["updated_at"],
        "pending_rev": int(meta.get("merge_pending_rev", "0")),
        "built_rev": int(meta.get("merge_built_rev", "0")),
        "pending": pending,
        "dirty": dirty,
        "latest": dict(latest) if latest else None,
        "latest_current": current_ok,
        "latest_deliverable": bool(latest and latest["deliverable"] and current_ok),
        "active_file": active,
    }


def merged_day(db: Database, day_iso: str, version: int | None = None) -> dict:
    """在一个只读快照内读取指定日期（或指定回看版本）的合并结果。"""
    day_iso = (day_iso or "").strip()
    if day_iso:
        try:
            datetime.strptime(day_iso, "%Y-%m-%d")
        except ValueError:
            raise BadMergeDate("日期格式须为 YYYY-MM-DD")
    def fn(conn):
        day = day_iso
        meta = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM meta")}
        state_gen = int(meta.get("state_generation", "0"))
        calibrated_gen = int(meta.get("calibrated_generation", "-1"))
        pending_rev = int(meta.get("merge_pending_rev", "0"))
        built_rev = int(meta.get("merge_built_rev", "0"))
        calc_version = conn.execute(
            "SELECT version FROM calc_state WHERE id = 1").fetchone()["version"]
        mstate = conn.execute("SELECT * FROM merge_state WHERE id = 1").fetchone()
        active = conn.execute("SELECT * FROM backup_files WHERE is_active = 1").fetchone()
        active = dict(active) if active else None

        if version is None:
            target = conn.execute(
                "SELECT * FROM merged_results ORDER BY version DESC LIMIT 1"
            ).fetchone()
        else:
            target = conn.execute(
                "SELECT * FROM merged_results WHERE version = ?", (version,)
            ).fetchone()
            if target is None:
                raise UnknownMergeVersion(version)

        if target is None:
            return {
                "available": False,
                "date": day,
                "reason": "尚无备机文件或合并结果尚未生成",
                "active_file": active,
                "merge": {
                    "status": mstate["status"],
                    "current_version": mstate["version"],
                    "pending": pending_rev != built_rev,
                },
                "deliverable": False,
                "count": 0,
                "readings": [],
                "conflicts": [],
            }

        t = dict(target)
        if not day:
            day = t["interval_start"][:10]
        file_row = conn.execute(
            "SELECT filename FROM backup_files WHERE id = ?", (t["backup_file_id"],)
        ).fetchone()
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM merged_readings WHERE merged_version = ? AND utc_date = ?"
            " ORDER BY utc_ms, CASE source WHEN 'master' THEN 0 ELSE 1 END, row_no",
            (t["version"], day),
        )]
        conflicts = [
            {
                "utc_time": c["utc_time"],
                "utc_ms": c["utc_ms"],
                "details": json.loads(c["details"]),
            }
            for c in conn.execute(
                "SELECT * FROM merged_conflicts WHERE merged_version = ? ORDER BY utc_ms",
                (t["version"],),
            )
        ]

        reasons = []
        if calc_version != t["master_calc_version"]:
            reasons.append(
                f"主机已发布新校时版本 v{calc_version}，本结果基于 v{t['master_calc_version']}"
            )
        if state_gen != calibrated_gen:
            reasons.append("主机有上传/锚点变更尚未重算，旧结果仅供回看")
        if active is None:
            reasons.append("当前没有生效的备机文件")
        elif active["id"] != t["backup_file_id"]:
            reasons.append(
                f"备机文件已更换（当前 #{active['id']}，本结果基于 #{t['backup_file_id']}）"
            )
        if pending_rev != built_rev:
            reasons.append("合并结果待重算")
        historical = version is not None and version != mstate["version"]
        if historical:
            reasons.append(f"历史合并版本 v{t['version']}（当前 v{mstate['version']}），仅供回看")

        stale = bool(reasons)
        current = not historical and t["version"] == mstate["version"]
        return {
            "available": True,
            "date": day,
            "version": t["version"],
            "current_version": mstate["version"],
            "current": current,
            "historical": historical,
            "master_calc_version": t["master_calc_version"],
            "backup_file_id": t["backup_file_id"],
            "backup_filename": file_row["filename"] if file_row else "",
            "interval_start": t["interval_start"],
            "interval_end": t["interval_end"],
            "status": t["status"],
            "deliverable": bool(t["deliverable"]) and not stale,
            "stale": stale,
            "stale_reasons": reasons,
            "created_at": t["created_at"],
            "omitted": {
                "near_master": t["omitted_near_master"],
                "outside_interval": t["omitted_outside"],
                "duplicate": t["omitted_duplicate"],
                "conflict": t["omitted_conflict"],
            },
            "count": len(rows),
            "master_count": sum(1 for r in rows if r["source"] == "master"),
            "backup_count": sum(1 for r in rows if r["source"] == "backup"),
            "readings": rows,
            "conflicts": conflicts,
        }

    return db.snapshot(fn)
