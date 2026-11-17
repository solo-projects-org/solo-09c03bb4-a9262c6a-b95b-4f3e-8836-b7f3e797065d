"""SQLite 数据访问层。

单连接 + 可重入锁串行化所有访问；写操作走 ``tx()``（BEGIN IMMEDIATE 事务）。
数据量（≤2000 点）极小，简单可靠优先。
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

from .timeutil import utcnow_iso

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    filename      TEXT NOT NULL DEFAULT '',
    created_at    TEXT NOT NULL,
    rows_accepted INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS readings (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id      INTEGER NOT NULL REFERENCES batches(id),
    row_no        INTEGER NOT NULL,          -- CSV 内的原始行号（数据行从 1 起）
    segment       INTEGER NOT NULL,          -- 开机段号
    local_seconds INTEGER NOT NULL,          -- 记录仪本地秒数
    temperature   REAL NOT NULL,
    created_at    TEXT NOT NULL,
    UNIQUE (segment, local_seconds)          -- 同段秒数不能重复
);

CREATE TABLE IF NOT EXISTS anchors (
    segment    INTEGER PRIMARY KEY,
    local1     INTEGER NOT NULL,
    utc1       TEXT NOT NULL,
    local2     INTEGER NOT NULL,
    utc2       TEXT NOT NULL,
    version    INTEGER NOT NULL,             -- 每次更正 +1，更正须携带所见版本
    updated_at TEXT NOT NULL
);

-- 当前生效的校时结果（整体换版，永远完整）
CREATE TABLE IF NOT EXISTS calibrated (
    reading_id     INTEGER PRIMARY KEY REFERENCES readings(id),
    utc_time       TEXT NOT NULL,
    utc_date       TEXT NOT NULL,
    anchor_version INTEGER NOT NULL,
    calc_version   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_calibrated_date ON calibrated(utc_date);

CREATE TABLE IF NOT EXISTS calc_state (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    version    INTEGER NOT NULL,             -- 校时结果版本号，单调递增
    status     TEXT NOT NULL,                -- idle / running / failed
    last_error TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);

-- ---------------------------------------------------------------- 备机补数
-- 每次上传备机 CSV 生成一个不可变文件版本；整批替换生效，失败不改变 active。
CREATE TABLE IF NOT EXISTS backup_files (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    filename         TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL,
    interval_start   TEXT NOT NULL,          -- 断流闭区间起点（UTC，含）
    interval_end     TEXT NOT NULL,          -- 断流闭区间终点（UTC，含）
    interval_start_ms INTEGER NOT NULL,
    interval_end_ms   INTEGER NOT NULL,
    rows_total       INTEGER NOT NULL,
    rows_in_interval INTEGER NOT NULL,
    conflict_count   INTEGER NOT NULL DEFAULT 0,
    is_active        INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_backup_files_active
    ON backup_files(is_active) WHERE is_active = 1;

CREATE TABLE IF NOT EXISTS backup_rows (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id     INTEGER NOT NULL REFERENCES backup_files(id),
    row_no      INTEGER NOT NULL,            -- 备机 CSV 数据行号（从 1 起）
    utc_time    TEXT NOT NULL,
    utc_ms      INTEGER NOT NULL,
    temperature REAL NOT NULL,
    in_interval INTEGER NOT NULL,            -- 1 表示落在断流区间内
    UNIQUE (file_id, row_no)
);
CREATE INDEX IF NOT EXISTS idx_backup_rows_file ON backup_rows(file_id, in_interval);

CREATE TABLE IF NOT EXISTS backup_conflicts (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id  INTEGER NOT NULL REFERENCES backup_files(id),
    utc_time TEXT NOT NULL,
    utc_ms   INTEGER NOT NULL,
    details  TEXT NOT NULL                   -- JSON：[{row_no, utc_time, temperature}, ...]
);
CREATE INDEX IF NOT EXISTS idx_backup_conflicts_file ON backup_conflicts(file_id);

-- 合并结果也是不可变快照：绑定一个主机发布版 + 一个备机文件。
CREATE TABLE IF NOT EXISTS merged_results (
    version              INTEGER PRIMARY KEY AUTOINCREMENT,
    master_calc_version  INTEGER NOT NULL,   -- 使用的主机已发布校时版本
    backup_file_id       INTEGER NOT NULL REFERENCES backup_files(id),
    interval_start       TEXT NOT NULL,
    interval_end         TEXT NOT NULL,
    status               TEXT NOT NULL,      -- ready / undeliverable
    deliverable          INTEGER NOT NULL,   -- 1/0；冲突区间为 0
    conflict_count       INTEGER NOT NULL,
    master_count         INTEGER NOT NULL,
    backup_count         INTEGER NOT NULL,
    omitted_near_master  INTEGER NOT NULL,
    omitted_outside      INTEGER NOT NULL,
    omitted_duplicate    INTEGER NOT NULL,
    omitted_conflict     INTEGER NOT NULL,
    created_at           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS merged_readings (
    merged_version INTEGER NOT NULL REFERENCES merged_results(version),
    source         TEXT NOT NULL CHECK (source IN ('master', 'backup')),
    source_ref     INTEGER NOT NULL,         -- master: readings.id；backup: backup_rows.id
    batch_id       INTEGER,                  -- 仅主机
    backup_file_id INTEGER,                  -- 仅备机
    row_no         INTEGER NOT NULL,         -- 原 CSV 数据行号
    segment        INTEGER,                  -- 仅主机
    local_seconds  INTEGER,                  -- 仅主机
    temperature    REAL NOT NULL,
    utc_time       TEXT NOT NULL,
    utc_ms         INTEGER NOT NULL,
    utc_date       TEXT NOT NULL,
    anchor_version INTEGER,                  -- 仅主机
    calc_version   INTEGER,                  -- 仅主机：取自的主机发布版
    PRIMARY KEY (merged_version, source, source_ref)
);
CREATE INDEX IF NOT EXISTS idx_merged_readings_day
    ON merged_readings(merged_version, utc_date, utc_ms);

CREATE TABLE IF NOT EXISTS merged_conflicts (
    merged_version INTEGER NOT NULL REFERENCES merged_results(version),
    utc_time       TEXT NOT NULL,
    utc_ms         INTEGER NOT NULL,
    details        TEXT NOT NULL,
    PRIMARY KEY (merged_version, utc_ms)
);

CREATE TABLE IF NOT EXISTS merge_state (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    version    INTEGER NOT NULL DEFAULT 0,   -- 当前生效的合并结果版本
    status     TEXT NOT NULL DEFAULT 'idle', -- idle / running / failed
    last_error TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: str):
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA busy_timeout = 5000")
        if path != ":memory:":
            self._conn.execute("PRAGMA journal_mode = WAL")
        self._lock = threading.RLock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('state_generation', '0')"
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('calibrated_generation', '-1')"
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('merge_pending_rev', '1')"
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('merge_built_rev', '0')"
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO calc_state(id, version, status, updated_at)"
                " VALUES (1, 0, 'idle', ?)",
                (utcnow_iso(),),
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO merge_state(id, version, status, updated_at)"
                " VALUES (1, 0, 'idle', ?)",
                (utcnow_iso(),),
            )

    @contextmanager
    def tx(self):
        """写事务：BEGIN IMMEDIATE，异常自动回滚（不留半版）。"""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                yield cur
                cur.execute("COMMIT")
            except BaseException:
                cur.execute("ROLLBACK")
                raise
            finally:
                cur.close()

    def snapshot(self, fn):
        """在锁内执行只读操作，保证读到一致快照。"""
        with self._lock:
            return fn(self._conn)

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def query_one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._conn.execute(sql, params)

    def close(self) -> None:
        with self._lock:
            self._conn.close()
