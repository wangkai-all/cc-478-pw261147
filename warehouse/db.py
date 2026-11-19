"""SQLite 存储层:schema、连接与通用助手。

设计要点:
- fact_events 按 (event_id, record_version) 版本化保存,ingest_seq 单调递增,
  增量汇总以 ingest_seq 为水位线(watermark)。
- effective_events 视图只取每个 event_id 的最高版本,更正自然生效。
- agg_day / agg_week / agg_month 按 run_id 保存完整快照,
  agg_pointer 指向当前可查询版本;失败 run 不切换指针,旧版本保留可查。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS warehouse_config (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  tz TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_batch (
  batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL DEFAULT 'api',
  note TEXT,
  submitted_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_events (
  ingest_seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL,
  record_version INTEGER NOT NULL,
  occurred_at_utc INTEGER NOT NULL,   -- epoch 秒(UTC)
  day_local TEXT NOT NULL,            -- 配置时区下的 YYYY-MM-DD
  region TEXT NOT NULL,
  courier TEXT,
  amount_cents INTEGER NOT NULL,      -- 金额一律存整数分,核对值精确可比
  qty INTEGER NOT NULL,
  batch_id INTEGER NOT NULL REFERENCES ingest_batch(batch_id),
  ingested_at TEXT NOT NULL,
  UNIQUE (event_id, record_version)
);

CREATE VIEW IF NOT EXISTS effective_events AS
SELECT e.* FROM fact_events e
WHERE (e.event_id, e.record_version) IN (
  SELECT event_id, MAX(record_version) FROM fact_events GROUP BY event_id
);

CREATE TABLE IF NOT EXISTS ingest_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER NOT NULL REFERENCES ingest_batch(batch_id),
  event_id TEXT NOT NULL,
  record_version INTEGER NOT NULL,
  action TEXT NOT NULL,               -- insert|correction|duplicate|stale|conflict|error
  is_late INTEGER NOT NULL DEFAULT 0,
  day_local TEXT,
  detail TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agg_run (
  run_id INTEGER PRIMARY KEY AUTOINCREMENT,
  level TEXT NOT NULL,                -- day|week|month
  status TEXT NOT NULL DEFAULT 'running',  -- running|success|failed
  noop INTEGER NOT NULL DEFAULT 0,    -- 1 = 无脏数据的安全空跑(如重复回补后)
  wm_from INTEGER NOT NULL DEFAULT 0, -- 处理的水位线区间 (wm_from, wm_to]
  wm_to INTEGER NOT NULL DEFAULT 0,
  prev_run_id INTEGER,                -- 同层上一个成功 run(版本链)
  day_run_id INTEGER,                 -- week/month 衍生自哪个 day run(层级血缘)
  buckets_touched INTEGER NOT NULL DEFAULT 0,
  rows_written INTEGER NOT NULL DEFAULT 0,
  recon_ok INTEGER,                   -- 1/0,成功 run 的核对结果
  error TEXT,
  started_at TEXT NOT NULL,
  finished_at TEXT
);

CREATE TABLE IF NOT EXISTS agg_pointer (
  level TEXT PRIMARY KEY,
  run_id INTEGER NOT NULL REFERENCES agg_run(run_id)
);

CREATE TABLE IF NOT EXISTS agg_day (
  run_id INTEGER NOT NULL REFERENCES agg_run(run_id),
  day TEXT NOT NULL,                  -- 本地日 YYYY-MM-DD
  region TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  events INTEGER NOT NULL,
  PRIMARY KEY (run_id, day, region)
);

CREATE TABLE IF NOT EXISTS agg_week (
  run_id INTEGER NOT NULL REFERENCES agg_run(run_id),
  iso_year INTEGER NOT NULL,
  iso_week INTEGER NOT NULL,          -- ISO 周(周一起),可跨月
  week_start TEXT NOT NULL,           -- 周一(本地日)
  week_end TEXT NOT NULL,             -- 周日(本地日)
  region TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  events INTEGER NOT NULL,
  PRIMARY KEY (run_id, iso_year, iso_week, region)
);

CREATE TABLE IF NOT EXISTS agg_month (
  run_id INTEGER NOT NULL REFERENCES agg_run(run_id),
  month TEXT NOT NULL,                -- 本地月 YYYY-MM
  region TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  events INTEGER NOT NULL,
  PRIMARY KEY (run_id, month, region)
);

-- 更正影响审计:每个 run 触及桶的旧值→新值
CREATE TABLE IF NOT EXISTS agg_run_bucket (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL REFERENCES agg_run(run_id),
  level TEXT NOT NULL,
  bucket TEXT NOT NULL,               -- 日 'YYYY-MM-DD' | 周 '2026-W44' | 月 'YYYY-MM'
  region TEXT NOT NULL,
  old_amount_cents INTEGER, old_qty INTEGER, old_events INTEGER,  -- NULL = 新桶
  new_amount_cents INTEGER, new_qty INTEGER, new_events INTEGER   -- NULL = 桶被清空
);

-- 核对值:每个 run 的控制总额,与事件层总额比对
CREATE TABLE IF NOT EXISTS agg_run_recon (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL REFERENCES agg_run(run_id),
  level TEXT NOT NULL,                -- events|day|week|month
  total_amount_cents INTEGER NOT NULL,
  total_qty INTEGER NOT NULL,
  total_events INTEGER NOT NULL
);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(path: str) -> sqlite3.Connection:
    # isolation_level=None → 自动提交;汇总写路径显式 BEGIN IMMEDIATE/COMMIT/ROLLBACK
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(conn: sqlite3.Connection, tz: str = "Asia/Shanghai") -> None:
    conn.executescript(SCHEMA)
    row = conn.execute("SELECT tz FROM warehouse_config WHERE id = 1").fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO warehouse_config(id, tz, created_at, updated_at) VALUES (1, ?, ?, ?)",
            (tz, utcnow(), utcnow()),
        )


def get_tz(conn: sqlite3.Connection) -> str:
    return conn.execute("SELECT tz FROM warehouse_config WHERE id = 1").fetchone()["tz"]
