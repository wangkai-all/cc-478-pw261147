"""事实仓库核心行为测试:时区、跨月周、更正传播、迟到、重复回补、失败保留旧版。"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from warehouse import aggregate as agg
from warehouse import calendar as cal
from warehouse import ingest as ing
from warehouse.db import connect, get_tz, init_db


@pytest.fixture()
def conn(tmp_path):
    c = connect(str(tmp_path / "test.db"))
    init_db(c, tz="Asia/Shanghai")
    yield c
    c.close()


def ev(event_id, occurred_at, amount, region="华东", qty=1, version=1, courier=None):
    return {
        "event_id": event_id, "record_version": version, "occurred_at": occurred_at,
        "region": region, "courier": courier, "amount": amount, "qty": qty,
    }


def totals(conn, table, run_id):
    return agg._table_totals(conn, table, run_id)


def events_totals(conn):
    return agg._events_totals(conn, 1 << 62)


# ---------------------------------------------------------------- 日历/时区

def test_timezone_day_boundary():
    tz = "Asia/Shanghai"
    # UTC 2026-10-31 16:30 = 上海 2026-11-01 00:30 → 落在 11-01
    assert cal.epoch_to_day(cal.parse_occurred_at("2026-10-31T16:30:00Z"), tz) == "2026-11-01"
    # UTC 2026-10-31 15:59 = 上海 2026-10-31 23:59 → 仍在 10-31
    assert cal.epoch_to_day(cal.parse_occurred_at("2026-10-31T15:59:00Z"), tz) == "2026-10-31"
    # 带偏移解析
    assert cal.parse_occurred_at("2026-11-01T00:30:00+08:00") == cal.parse_occurred_at("2026-10-31T16:30:00Z")


def test_iso_week_crosses_month():
    # 2026-10-26(周一) ~ 2026-11-01(周日) 同属 ISO 2026-W44,横跨 10/11 月
    assert cal.day_to_week("2026-10-26")[:2] == (2026, 44)
    assert cal.day_to_week("2026-11-01")[:2] == (2026, 44)
    assert cal.day_to_week("2026-11-02")[:2] == (2026, 45)
    assert cal.week_bounds(2026, 44) == ("2026-10-26", "2026-11-01")


# ---------------------------------------------------------------- 摄取

def test_ingest_dedup_correction_stale_conflict(conn):
    ing.ingest_batch(conn, [ev("A", "2026-11-01T10:00:00+08:00", 100)])
    # 重复回补:完全相同 → duplicate,不产生新行
    r = ing.ingest_batch(conn, [ev("A", "2026-11-01T10:00:00+08:00", 100)])
    assert r["counts"]["duplicate"] == 1
    assert conn.execute("SELECT COUNT(*) c FROM fact_events").fetchone()["c"] == 1
    # 更正:更高版本 → correction,旧行保留
    r = ing.ingest_batch(conn, [ev("A", "2026-11-01T10:00:00+08:00", 250, version=2)])
    assert r["counts"]["correction"] == 1
    assert conn.execute("SELECT COUNT(*) c FROM fact_events").fetchone()["c"] == 2
    # 生效值 = 最高版本
    row = conn.execute("SELECT amount_cents FROM effective_events").fetchone()
    assert row["amount_cents"] == 25000
    # 乱序旧版本 → stale
    r = ing.ingest_batch(conn, [ev("A", "2026-11-01T10:00:00+08:00", 100, version=1)])
    assert r["counts"]["stale"] == 1
    # 同版本不同内容 → conflict
    r = ing.ingest_batch(conn, [ev("A", "2026-11-01T10:00:00+08:00", 999, version=2)])
    assert r["counts"]["conflict"] == 1
    assert conn.execute("SELECT COUNT(*) c FROM fact_events").fetchone()["c"] == 2


def test_late_detection(conn):
    ing.ingest_batch(conn, [ev("A", "2026-10-28T10:00:00+08:00", 100)])
    agg.run_all(conn)
    # 事件日期已被 day 版本覆盖 → 迟到
    r = ing.ingest_batch(conn, [ev("B", "2026-10-28T12:00:00+08:00", 50)])
    assert r["items"][0]["is_late"] is True
    # 新日期不算迟到
    r = ing.ingest_batch(conn, [ev("C", "2026-11-05T12:00:00+08:00", 50)])
    assert r["items"][0]["is_late"] is False


# ---------------------------------------------------------------- 汇总与一致性

def seed_week_crossing(conn):
    """跨月周 W44 (10-26~11-01) + W45 部分,三个分区。"""
    events = []
    days = ["2026-10-26", "2026-10-27", "2026-10-28", "2026-10-29", "2026-10-30",
            "2026-10-31", "2026-11-01", "2026-11-02", "2026-11-03"]
    for i, d in enumerate(days):
        for j, region in enumerate(["华东", "华北"]):
            events.append(ev(f"E{d}-{region}", f"{d}T{8 + j:02d}:00:00+08:00",
                             100 + i * 10 + j, region=region, qty=2))
    ing.ingest_batch(conn, events)
    return events


def test_three_level_consistency_and_cross_month_week(conn):
    seed_week_crossing(conn)
    res = agg.run_all(conn)
    assert all(r["status"] == "success" for r in res.values())
    expected = events_totals(conn)
    assert totals(conn, "agg_day", agg.pointer(conn, "day")) == expected
    assert totals(conn, "agg_week", agg.pointer(conn, "week")) == expected
    assert totals(conn, "agg_month", agg.pointer(conn, "month")) == expected

    # 跨月周 W44 是一个桶,含 10 月和 11 月的 7 天
    w44 = conn.execute(
        "SELECT * FROM agg_week WHERE run_id=? AND iso_year=2026 AND iso_week=44 AND region='华东'",
        (agg.pointer(conn, "week"),)).fetchone()
    assert w44["week_start"] == "2026-10-26" and w44["week_end"] == "2026-11-01"
    assert w44["events"] == 7  # 华东每天 1 单 × 7 天
    # 月桶保持纯粹:10 月 6 天、11 月 3 天
    m10 = conn.execute(
        "SELECT events FROM agg_month WHERE run_id=? AND month='2026-10' AND region='华东'",
        (agg.pointer(conn, "month"),)).fetchone()
    m11 = conn.execute(
        "SELECT events FROM agg_month WHERE run_id=? AND month='2026-11' AND region='华东'",
        (agg.pointer(conn, "month"),)).fetchone()
    assert (m10["events"], m11["events"]) == (6, 3)
    # 血缘:week/month run 记录衍生自哪个 day run
    assert res["week"]["day_run_id"] == res["day"]["run_id"]
    assert res["month"]["day_run_id"] == res["day"]["run_id"]
    # 每个成功 run 都有核对记录且一致
    for run in res.values():
        recs = conn.execute("SELECT * FROM agg_run_recon WHERE run_id=?", (run["run_id"],)).fetchall()
        assert recs and len({(r["total_amount_cents"], r["total_qty"], r["total_events"]) for r in recs}) == 1


def test_correction_propagates_and_old_version_queryable(conn):
    seed_week_crossing(conn)
    agg.run_all(conn)
    day_v1, week_v1, month_v1 = (agg.pointer(conn, l) for l in ("day", "week", "month"))
    before = conn.execute(
        "SELECT amount_cents FROM agg_day WHERE run_id=? AND day='2026-10-28' AND region='华东'",
        (day_v1,)).fetchone()["amount_cents"]

    # 迟到 + 更正:10-28 华东一单 120 → 900;另有一单跨月搬移 10-31 → 11-01
    ing.ingest_batch(conn, [
        ev("E2026-10-28-华东", "2026-10-28T08:00:00+08:00", 900, version=2),
        ev("E2026-10-31-华东", "2026-11-01T00:20:00+08:00", 160, version=2),  # 跨月搬移
        ev("LATE-1", "2026-10-28T23:00:00+08:00", 77),
    ])
    res = agg.run_all(conn)
    assert all(r["status"] == "success" for r in res.values())

    # 三级都反映更正后的总额
    expected = events_totals(conn)
    for level, table in (("day", "agg_day"), ("week", "agg_week"), ("month", "agg_month")):
        assert totals(conn, table, agg.pointer(conn, level)) == expected, level

    day_v2 = agg.pointer(conn, "day")
    assert day_v2 != day_v1
    d28 = conn.execute(
        "SELECT amount_cents FROM agg_day WHERE run_id=? AND day='2026-10-28' AND region='华东'",
        (day_v2,)).fetchone()["amount_cents"]
    assert d28 == before - 12000 + 90000 + 7700  # 更正 +8800? 120→900 即 +78000 分,再加迟到 7700
    # 跨月搬移:10-31 华东唯一的单被搬走到 11-01,桶被清空从新快照移除;月桶随之变化
    o31 = conn.execute(
        "SELECT amount_cents, events FROM agg_day WHERE run_id=? AND day='2026-10-31' AND region='华东'",
        (day_v2,)).fetchone()
    n01 = conn.execute(
        "SELECT amount_cents, events FROM agg_day WHERE run_id=? AND day='2026-11-01' AND region='华东'",
        (day_v2,)).fetchone()
    assert o31 is None  # 桶被搬空,不再出现
    assert n01["events"] == 2  # 原 11-01 一单 + 搬来一单
    emptied = conn.execute(
        "SELECT * FROM agg_run_bucket WHERE run_id=? AND level='day' AND bucket='2026-10-31' AND region='华东'",
        (res["day"]["run_id"],)).fetchone()
    assert emptied["old_events"] == 1 and emptied["new_events"] is None  # 审计记录桶被清空

    # 旧版本仍可查询(版本切换)
    old = conn.execute(
        "SELECT amount_cents FROM agg_day WHERE run_id=? AND day='2026-10-28' AND region='华东'",
        (day_v1,)).fetchone()["amount_cents"]
    assert old == before
    # 更正影响审计:脏桶旧值→新值
    deltas = conn.execute(
        "SELECT * FROM agg_run_bucket WHERE run_id=? AND level='day' AND bucket='2026-10-28' AND region='华东'",
        (res["day"]["run_id"],)).fetchall()
    assert len(deltas) == 1
    assert deltas[0]["old_amount_cents"] == before
    assert deltas[0]["new_amount_cents"] == d28


def test_duplicate_backfill_is_noop(conn):
    seed_week_crossing(conn)
    agg.run_all(conn)
    batch = [ev("X1", "2026-11-03T10:00:00+08:00", 10), ev("X2", "2026-11-03T11:00:00+08:00", 20)]
    ing.ingest_batch(conn, batch)
    agg.run_all(conn)
    # 重复回补同一批 → 全部 duplicate
    r = ing.ingest_batch(conn, batch)
    assert r["counts"]["duplicate"] == 2
    # 再跑汇总 → day 空跑 noop,不触碰任何桶,不产生 week/month run
    res = agg.run_all(conn)
    assert res["day"]["status"] == "success" and res["day"]["noop"] == 1
    assert res["day"]["buckets_touched"] == 0
    assert "week" not in res and "month" not in res
    # 总额不变
    assert totals(conn, "agg_day", agg.pointer(conn, "day")) == events_totals(conn)


def test_failure_preserves_previous_version(conn):
    seed_week_crossing(conn)
    agg.run_all(conn)
    day_v1, week_v1 = agg.pointer(conn, "day"), agg.pointer(conn, "week")
    ing.ingest_batch(conn, [ev("N1", "2026-11-04T10:00:00+08:00", 500)])

    # 演练:week 层事务内失败 → 回滚,week 指针不动;day 层不受影响
    res = agg.run_all(conn, simulate_failure="week")
    assert res["day"]["status"] == "success"
    assert res["week"]["status"] == "failed"
    assert "SimulatedFailure" in res["week"]["error"]
    assert agg.pointer(conn, "week") == week_v1
    assert agg.pointer(conn, "day") != day_v1
    # 失败 run 未留下半截数据
    assert conn.execute("SELECT COUNT(*) c FROM agg_week WHERE run_id=?",
                        (res["week"]["run_id"],)).fetchone()["c"] == 0
    # 旧 week 版本查询结果不变
    assert totals(conn, "agg_week", week_v1) == totals(conn, "agg_day", day_v1)
    # 修复后重跑:week 追平到最新 day
    res2 = agg.run_all(conn)
    assert res2["week"]["status"] == "success"
    assert totals(conn, "agg_week", agg.pointer(conn, "week")) == events_totals(conn)


def test_day_level_failure_keeps_everything(conn):
    seed_week_crossing(conn)
    agg.run_all(conn)
    ptrs = {l: agg.pointer(conn, l) for l in agg.LEVELS}
    ing.ingest_batch(conn, [ev("N2", "2026-11-04T10:00:00+08:00", 500)])
    res = agg.run_all(conn, simulate_failure="day")
    assert res["day"]["status"] == "failed"
    assert "week" not in res and "month" not in res
    for level in agg.LEVELS:
        assert agg.pointer(conn, level) == ptrs[level]


def test_timezone_change_full_rebuild(conn):
    seed_week_crossing(conn)
    agg.run_all(conn)
    expected = events_totals(conn)
    # 切到 UTC:本地日整体移动,但三级总额仍与事件层一致
    agg.change_timezone(conn, "UTC")
    assert get_tz(conn) == "UTC"
    for level, table in (("day", "agg_day"), ("week", "agg_week"), ("month", "agg_month")):
        assert totals(conn, table, agg.pointer(conn, level)) == expected, level
    # 上海时区下 10-31 08:00 的事件 = UTC 10-31 00:00,仍在 10-31;
    # 但 11-01 08:00 上海 = 11-01 00:00 UTC。抽查一个跨午夜的:
    row = conn.execute(
        "SELECT day_local FROM fact_events WHERE event_id='E2026-11-01-华东'").fetchone()
    assert row["day_local"] == "2026-11-01"  # 08:00+08:00 = 00:00Z 同日
    # 再切回上海,总额仍一致
    agg.change_timezone(conn, "Asia/Shanghai")
    assert totals(conn, "agg_day", agg.pointer(conn, "day")) == expected


def test_dst_timezone(conn):
    # 夏令时切换日(美国东部 2026-03-08 春季拨快)落桶不错乱
    conn.execute("UPDATE warehouse_config SET tz='America/New_York' WHERE id=1")
    ing.ingest_batch(conn, [
        ev("D1", "2026-03-08T06:30:00Z", 10),   # 01:30 EST → 03-07? 不,EST 是 UTC-5 → 01:30 属 03-08
        ev("D2", "2026-03-08T07:30:00Z", 20),   # 03:30 EDT(跳过了 02:00)
    ])
    res = agg.run_all(conn)
    assert res["day"]["status"] == "success"
    rows = conn.execute("SELECT day, SUM(amount_cents) a FROM agg_day WHERE run_id=? GROUP BY day",
                        (agg.pointer(conn, "day"),)).fetchall()
    assert [(r["day"], r["a"]) for r in rows] == [("2026-03-08", 3000)]
