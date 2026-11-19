"""日/周/月三级增量汇总引擎。

核心机制:
- 版本化快照:每个成功 run 写入该层完整快照(脏桶重算 + 未脏桶从前版复制),
  agg_pointer 指向当前可查询版本 → 任意历史版本可切换查询。
- 增量回补:以 ingest_seq 水位线定位变更事件,取其全部版本涉及桶为脏桶
  (覆盖更正把事件搬到其他日期/分区的情况),只重算脏桶。
- 层级传播:week/month 由指定 day run 的快照上卷(day_run_id 血缘),
  周(ISO,可跨月)与月都由本地日推导,保证跨月边界与时区一致。
- 可审计:agg_run 记录水位线区间/血缘/状态;agg_run_bucket 记录每桶旧值→新值;
  agg_run_recon 记录控制总额核对。
- 失败保留旧版:快照写入 + 核对 + 指针切换在同一事务;任何异常(含核对不符、
  演练注入的失败)整体回滚,指针不动,上一版仍可查询。
"""
from __future__ import annotations

from collections import defaultdict
from zoneinfo import ZoneInfo

from . import calendar as cal
from .db import utcnow

LEVELS = ("day", "week", "month")


class ReconMismatch(Exception):
    """核对值不符:汇总结果与事件层控制总额不一致。"""


class SimulatedFailure(Exception):
    """演练用:在事务内注入失败,验证回滚与旧版保留。"""


# ---------------------------------------------------------------- run 记录

def _mark_interrupted(conn) -> None:
    """进程崩溃等遗留的 running run 标记为失败,保持审计表干净。"""
    conn.execute(
        "UPDATE agg_run SET status='failed', error='interrupted: superseded by newer run', "
        "finished_at=? WHERE status='running'",
        (utcnow(),),
    )


def _get_run(conn, run_id):
    if run_id is None:
        return None
    return conn.execute("SELECT * FROM agg_run WHERE run_id=?", (run_id,)).fetchone()


def pointer(conn, level: str):
    row = conn.execute("SELECT run_id FROM agg_pointer WHERE level=?", (level,)).fetchone()
    return row["run_id"] if row else None


def _create_run(conn, level, wm_from, wm_to, prev_run_id, day_run_id) -> int:
    cur = conn.execute(
        "INSERT INTO agg_run(level, status, wm_from, wm_to, prev_run_id, day_run_id, started_at) "
        "VALUES (?, 'running', ?, ?, ?, ?, ?)",
        (level, wm_from, wm_to, prev_run_id, day_run_id, utcnow()),
    )
    return cur.lastrowid


def _finish(conn, run_id, status, noop=0, buckets=0, rows=0, recon_ok=None, error=None) -> None:
    conn.execute(
        "UPDATE agg_run SET status=?, noop=?, buckets_touched=?, rows_written=?, recon_ok=?, "
        "error=?, finished_at=? WHERE run_id=?",
        (status, noop, buckets, rows, recon_ok, error, utcnow(), run_id),
    )


def _flip_pointer(conn, level, run_id) -> None:
    conn.execute(
        "INSERT INTO agg_pointer(level, run_id) VALUES (?, ?) "
        "ON CONFLICT(level) DO UPDATE SET run_id=excluded.run_id",
        (level, run_id),
    )


# ---------------------------------------------------------------- 汇总计算

def _events_totals(conn, wm_to: int):
    row = conn.execute(
        "SELECT COALESCE(SUM(amount_cents),0) a, COALESCE(SUM(qty),0) q, COUNT(*) n "
        "FROM effective_events WHERE ingest_seq <= ?",
        (wm_to,),
    ).fetchone()
    return (row["a"], row["q"], row["n"])


def _table_totals(conn, table: str, run_id: int):
    row = conn.execute(
        f"SELECT COALESCE(SUM(amount_cents),0) a, COALESCE(SUM(qty),0) q, "
        f"COALESCE(SUM(events),0) n FROM {table} WHERE run_id=?",
        (run_id,),
    ).fetchone()
    return (row["a"], row["q"], row["n"])


def _write_recon(conn, run_id, level, totals) -> None:
    conn.execute(
        "INSERT INTO agg_run_recon(run_id, level, total_amount_cents, total_qty, total_events) "
        "VALUES (?, ?, ?, ?, ?)",
        (run_id, level, totals[0], totals[1], totals[2]),
    )


def _insert_bucket_delta(conn, run_id, level, bucket, region, old, new) -> None:
    conn.execute(
        "INSERT INTO agg_run_bucket(run_id, level, bucket, region, "
        "old_amount_cents, old_qty, old_events, new_amount_cents, new_qty, new_events) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            run_id, level, bucket, region,
            old[0] if old else None, old[1] if old else None, old[2] if old else None,
            new[0] if new else None, new[1] if new else None, new[2] if new else None,
        ),
    )


# ---------------------------------------------------------------- day 层

def _run_day(conn, full_rebuild: bool = False, simulate: bool = False) -> int:
    prev_id = pointer(conn, "day")
    prev = _get_run(conn, prev_id)
    prev_wm = prev["wm_to"] if prev else 0
    max_seq = conn.execute("SELECT COALESCE(MAX(ingest_seq),0) s FROM fact_events").fetchone()["s"]

    if full_rebuild:
        dirty = {(r["day_local"], r["region"]) for r in
                 conn.execute("SELECT DISTINCT day_local, region FROM fact_events")}
    else:
        # 变更事件的全部版本涉及的桶都算脏:更正可能把事件搬到别的日期/分区
        dirty = {(r["day_local"], r["region"]) for r in conn.execute(
            """SELECT DISTINCT day_local, region FROM fact_events
               WHERE event_id IN (SELECT DISTINCT event_id FROM fact_events WHERE ingest_seq > ?)""",
            (prev_wm,))}

    run_id = _create_run(conn, "day", prev_wm, max_seq, prev_id, None)
    if not dirty:
        _finish(conn, run_id, "success", noop=1, recon_ok=1)
        _write_recon(conn, run_id, "events", _events_totals(conn, max_seq))
        _write_recon(conn, run_id, "day",
                     _table_totals(conn, "agg_day", prev_id) if prev_id else (0, 0, 0))
        return run_id

    try:
        conn.execute("BEGIN IMMEDIATE")
        prev_rows = {}
        if prev_id:
            for r in conn.execute("SELECT * FROM agg_day WHERE run_id=?", (prev_id,)):
                prev_rows[(r["day"], r["region"])] = (r["amount_cents"], r["qty"], r["events"])

        new_vals = {}
        for day, region in sorted(dirty):
            r = conn.execute(
                "SELECT COALESCE(SUM(amount_cents),0) a, COALESCE(SUM(qty),0) q, COUNT(*) n "
                "FROM effective_events WHERE day_local=? AND region=?",
                (day, region),
            ).fetchone()
            if r["n"] > 0:
                new_vals[(day, region)] = (r["a"], r["q"], r["n"])

        rows_written = 0
        for (day, region), vals in prev_rows.items():  # 未脏桶从前版复制
            if (day, region) not in dirty:
                conn.execute(
                    "INSERT INTO agg_day(run_id, day, region, amount_cents, qty, events) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (run_id, day, region, vals[0], vals[1], vals[2]))
                rows_written += 1
        for (day, region), vals in new_vals.items():  # 脏桶写新值
            conn.execute(
                "INSERT INTO agg_day(run_id, day, region, amount_cents, qty, events) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (run_id, day, region, vals[0], vals[1], vals[2]))
            rows_written += 1

        for (day, region) in sorted(dirty):  # 审计:旧值→新值
            _insert_bucket_delta(conn, run_id, "day", day, region,
                                 prev_rows.get((day, region)), new_vals.get((day, region)))

        ev = _events_totals(conn, max_seq)
        mine = _table_totals(conn, "agg_day", run_id)
        _write_recon(conn, run_id, "events", ev)
        _write_recon(conn, run_id, "day", mine)
        if ev != mine:
            raise ReconMismatch(f"day run {run_id}: 事件层{ev} != 汇总层{mine}")
        if simulate:
            raise SimulatedFailure("演练:day 层事务内模拟失败")
        _flip_pointer(conn, "day", run_id)
        _finish(conn, run_id, "success", buckets=len(dirty), rows=rows_written, recon_ok=1)
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        _finish(conn, run_id, "failed", error=f"{type(e).__name__}: {e}")
    return run_id


# ---------------------------------------------------------------- week / month 层

def _run_rollup(conn, level: str, day_run_id: int, lag_from: int = 0, simulate: bool = False) -> int:
    """week/month 共用:由指定 day run 的快照上卷,只重算含脏日的桶。

    lag_from:该层当前指针血缘所在的 day run_id;脏日 = day run (lag_from, day_run_id]
    区间内全部日层变更。层失败后指针落后,下次运行自动按此区间补跑追平。
    """
    assert level in ("week", "month")
    table = f"agg_{level}"
    prev_id = pointer(conn, level)
    prev = _get_run(conn, prev_id)
    day_run = _get_run(conn, day_run_id)
    dirty_days = [r["bucket"] for r in conn.execute(
        "SELECT DISTINCT bucket FROM agg_run_bucket WHERE level='day' AND run_id > ? AND run_id <= ?",
        (lag_from, day_run_id))]
    wm_from = prev["wm_to"] if prev else day_run["wm_from"]
    run_id = _create_run(conn, level, wm_from, day_run["wm_to"], prev_id, day_run_id)
    if not dirty_days:
        _finish(conn, run_id, "success", noop=1, recon_ok=1)
        return run_id

    def key_of(day: str):
        if level == "week":
            y, w, _, _ = cal.day_to_week(day)
            return (y, w)
        return (cal.day_to_month(day),)

    dirty_keys = {key_of(d) for d in dirty_days}

    try:
        conn.execute("BEGIN IMMEDIATE")
        day_rows = conn.execute(
            "SELECT day, region, amount_cents, qty, events FROM agg_day WHERE run_id=?",
            (day_run_id,)).fetchall()
        grouped = defaultdict(lambda: [0, 0, 0])  # (key..., region) -> [amount, qty, events]
        for r in day_rows:
            gk = key_of(r["day"]) + (r["region"],)
            g = grouped[gk]
            g[0] += r["amount_cents"]
            g[1] += r["qty"]
            g[2] += r["events"]

        prev_rows = {}
        if prev_id:
            for r in conn.execute(f"SELECT * FROM {table} WHERE run_id=?", (prev_id,)):
                k = ((r["iso_year"], r["iso_week"]) if level == "week" else (r["month"],)) + (r["region"],)
                prev_rows[k] = r

        def insert_row(key, region, vals):
            if level == "week":
                y, w = key
                ws, we = cal.week_bounds(y, w)
                conn.execute(
                    "INSERT INTO agg_week(run_id, iso_year, iso_week, week_start, week_end, region, "
                    "amount_cents, qty, events) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (run_id, y, w, ws, we, region, vals[0], vals[1], vals[2]))
            else:
                conn.execute(
                    "INSERT INTO agg_month(run_id, month, region, amount_cents, qty, events) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (run_id, key[0], region, vals[0], vals[1], vals[2]))

        rows_written = 0
        for k, r in prev_rows.items():  # 未脏桶复制
            bucket_key = k[:-1]
            if bucket_key not in dirty_keys:
                insert_row(bucket_key, k[-1], (r["amount_cents"], r["qty"], r["events"]))
                rows_written += 1
        new_keys = set()
        for gk, vals in grouped.items():  # 脏桶重算
            bucket_key, region = gk[:-1], gk[-1]
            if bucket_key in dirty_keys:
                insert_row(bucket_key, region, tuple(vals))
                new_keys.add(gk)
                rows_written += 1

        # 审计:脏桶旧值→新值
        touched = defaultdict(set)
        for k in prev_rows:
            if k[:-1] in dirty_keys:
                touched[k[:-1]].add(k[-1])
        for gk in new_keys:
            touched[gk[:-1]].add(gk[-1])
        for bucket_key, regions in sorted(touched.items()):
            for region in sorted(regions):
                old_r = prev_rows.get(bucket_key + (region,))
                old = (old_r["amount_cents"], old_r["qty"], old_r["events"]) if old_r else None
                new = tuple(grouped[bucket_key + (region,)]) if bucket_key + (region,) in new_keys else None
                if level == "week":
                    bucket = cal.week_key(*bucket_key)
                else:
                    bucket = bucket_key[0]
                _insert_bucket_delta(conn, run_id, level, bucket, region, old, new)

        day_totals = _table_totals(conn, "agg_day", day_run_id)
        mine = _table_totals(conn, table, run_id)
        _write_recon(conn, run_id, "events", _events_totals(conn, day_run["wm_to"]))
        _write_recon(conn, run_id, level, mine)
        if day_totals != mine:
            raise ReconMismatch(f"{level} run {run_id}: day 层{day_totals} != {level} 层{mine}")
        if simulate:
            raise SimulatedFailure(f"演练:{level} 层事务内模拟失败")
        _flip_pointer(conn, level, run_id)
        _finish(conn, run_id, "success", buckets=len(dirty_keys), rows=rows_written, recon_ok=1)
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        _finish(conn, run_id, "failed", error=f"{type(e).__name__}: {e}")
    return run_id


# ---------------------------------------------------------------- 入口

def run_all(conn, full_rebuild: bool = False, simulate_failure: str | None = None) -> dict:
    """跑一次增量汇总:day → (有变更时) week、month。

    simulate_failure ∈ {'day','week','month'} 用于演练:在该层事务内注入失败,
    验证回滚与上一版保留。返回各层 run 记录。
    """
    _mark_interrupted(conn)
    results = {}
    day_id = _run_day(conn, full_rebuild=full_rebuild, simulate=(simulate_failure == "day"))
    results["day"] = dict(_get_run(conn, day_id))
    day_pointer = pointer(conn, "day")
    if day_pointer is not None:
        for level in ("week", "month"):
            # 滞后检测:该层指针血缘落后于 day 指针(新变更或此前失败)则补跑
            lvl_ptr = pointer(conn, level)
            lvl_run = _get_run(conn, lvl_ptr)
            lag_from = lvl_run["day_run_id"] if lvl_run else 0
            if lag_from != day_pointer:
                rid = _run_rollup(conn, level, day_pointer, lag_from=lag_from,
                                  simulate=(simulate_failure == level))
                results[level] = dict(_get_run(conn, rid))
    return results


def change_timezone(conn, new_tz: str) -> dict:
    """切换配置时区:重算全部事件的本地日,并全量重建三级汇总。"""
    ZoneInfo(new_tz)  # 校验时区名
    conn.execute("UPDATE warehouse_config SET tz=?, updated_at=? WHERE id=1", (new_tz, utcnow()))
    for r in conn.execute("SELECT ingest_seq, occurred_at_utc FROM fact_events").fetchall():
        conn.execute(
            "UPDATE fact_events SET day_local=? WHERE ingest_seq=?",
            (cal.epoch_to_day(r["occurred_at_utc"], new_tz), r["ingest_seq"]),
        )
    return run_all(conn, full_rebuild=True)
