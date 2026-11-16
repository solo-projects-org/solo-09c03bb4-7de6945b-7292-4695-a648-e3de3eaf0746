"""SQLite 连接与建表。

并发模型说明：
- WAL 模式允许读写并发；写入统一以 ``BEGIN IMMEDIATE`` 立即获取 RESERVED 锁，
  配合 ``busy_timeout`` 让并发写请求排队等待而不是立刻报 SQLITE_BUSY。
- 每个业务操作使用独立连接，单条连接内的全部 DML 处于同一个事务，
  提交失败/异常一律 rollback，保证“事件更新 + 日报失效”原子可见，
  不会出现半份日报或混合版本。
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

DB_PATH = Path(os.environ.get("DOWNTIME_DB_PATH", "/data/gateway.db"))

# 测试钩子：可被替换为返回包装连接的工厂，用于注入写失败验证回滚
connection_factory = sqlite3.connect


def get_db_path() -> Path:
    return DB_PATH


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = connection_factory(
        str(DB_PATH),
        timeout=15.0,
        isolation_level=None,  # 关闭隐式事务，手动 BEGIN/COMMIT
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


SCHEMA = """
-- 事件修订流水：同一 (设备号, 事件号) 的每个修订号一行，全量留痕
CREATE TABLE IF NOT EXISTS event_revisions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id     TEXT NOT NULL,
    event_id      TEXT NOT NULL,
    revision      INTEGER NOT NULL,
    event_time    TEXT NOT NULL,          -- UTC ISO-8601
    status        TEXT NOT NULL CHECK (status IN ('RUNNING', 'STOPPED')),
    received_at   TEXT NOT NULL,
    UNIQUE (device_id, event_id, revision)
);

-- 每个设备的事件数据版号：每次“接受”一条新事件修订 +1（同版同文幂等不增版）
CREATE TABLE IF NOT EXISTS device_versions (
    device_id      TEXT PRIMARY KEY,
    current_version INTEGER NOT NULL DEFAULT 0
);

-- 网关独立上报的状态采样：同一设备内采样号唯一。
-- 采样不设修订号：同号同文幂等，同号异文直接拒收。
CREATE TABLE IF NOT EXISTS samples (
    device_id     TEXT NOT NULL,
    sample_id     TEXT NOT NULL,
    sample_time   TEXT NOT NULL,          -- UTC ISO-8601
    status        TEXT NOT NULL CHECK (status IN ('RUNNING', 'STOPPED')),
    received_at   TEXT NOT NULL,
    PRIMARY KEY (device_id, sample_id)
);

-- 每个设备的采样数据版号：每次“接受”一条新采样 +1（同号同文幂等不增版）
CREATE TABLE IF NOT EXISTS sample_versions (
    device_id      TEXT PRIMARY KEY,
    current_version INTEGER NOT NULL DEFAULT 0
);

-- 按设备 + UTC 日期的日报；内容为 JSON，invalidate 只置标记不删行
CREATE TABLE IF NOT EXISTS reports (
    device_id          TEXT NOT NULL,
    report_date        TEXT NOT NULL,     -- YYYY-MM-DD（UTC）
    data_version       INTEGER NOT NULL,
    status             TEXT NOT NULL CHECK (status IN ('CURRENT', 'INVALIDATED')),
    content            TEXT NOT NULL,
    generated_at       TEXT NOT NULL,
    invalidated_at     TEXT,
    invalidated_reason TEXT,
    PRIMARY KEY (device_id, report_date)
);

CREATE INDEX IF NOT EXISTS idx_event_rev_lookup
    ON event_revisions (device_id, event_id, revision);
CREATE INDEX IF NOT EXISTS idx_event_time
    ON event_revisions (device_id, event_time);

CREATE INDEX IF NOT EXISTS idx_sample_time
    ON samples (device_id, sample_time);

-- 每设备每 UTC 日期的采样核查结果；两份来源版号缺一不可
CREATE TABLE IF NOT EXISTS sample_checks (
    device_id           TEXT NOT NULL,
    check_date          TEXT NOT NULL,     -- YYYY-MM-DD（UTC）
    event_version       INTEGER NOT NULL,  -- 事件数据版号
    sample_version      INTEGER NOT NULL,  -- 采样数据版号
    status              TEXT NOT NULL CHECK (status IN ('CURRENT', 'INVALIDATED')),
    content             TEXT NOT NULL,
    generated_at        TEXT NOT NULL,
    invalidated_at      TEXT,
    invalidated_reason  TEXT,
    PRIMARY KEY (device_id, check_date)
);

CREATE INDEX IF NOT EXISTS idx_check_status
    ON sample_checks (device_id, status);

-- 每个事件当前生效修订（同版异文被拒收，因此同 revision 只有一行）
CREATE VIEW IF NOT EXISTS latest_events AS
SELECT r.device_id,
       r.event_id,
       r.revision,
       r.event_time,
       r.status
FROM event_revisions r
JOIN (
    SELECT device_id, event_id, MAX(revision) AS max_revision
    FROM event_revisions
    GROUP BY device_id, event_id
) m
  ON m.device_id = r.device_id
 AND m.event_id = r.event_id
 AND m.max_revision = r.revision;
"""


def init_db() -> None:
    # 建库属于一次性 DDL（SQLite 中 DDL 同样事务化），
    # executescript 自身会先提交挂起事务，这里不另开显式事务。
    conn = get_connection()
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()
