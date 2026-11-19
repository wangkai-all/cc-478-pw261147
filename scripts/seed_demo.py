#!/usr/bin/env python3
"""演示数据:覆盖跨月周、时区边界、迟到数据、更正传播、重复回补、失败演练。

用法:  python scripts/seed_demo.py [db_path]
默认 db_path = ./data/warehouse.db(存在则先删除重建)。
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from warehouse import aggregate as agg
from warehouse import ingest as ing
from warehouse.db import connect, get_tz, init_db

DB = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "..", "data", "warehouse.db")


def ev(event_id, occurred_at, amount, region="华东", qty=1, version=1):
    return {"event_id": event_id, "record_version": version, "occurred_at": occurred_at,
            "region": region, "amount": amount, "qty": qty}


def show_runs(title, res):
    print(f"\n== {title} ==")
    for level, r in res.items():
        print(f"  {level:5s} run#{r['run_id']:<3d} {r['status']:8s} noop={r['noop']} "
              f"水位线 {r['wm_from']}→{r['wm_to']} 触桶 {r['buckets_touched']} {r.get('error') or ''}")


def main():
    if os.path.exists(DB):
        os.remove(DB)
    os.makedirs(os.path.dirname(os.path.abspath(DB)), exist_ok=True)
    conn = connect(DB)
    init_db(conn, tz="Asia/Shanghai")
    print(f"db = {DB}  tz = {get_tz(conn)}")

    # 批次 1:跨月周 W44(10-26~11-01)+ W45 部分,三个分区,含跨午夜边界事件
    b1 = []
    days = ["2026-10-26", "2026-10-27", "2026-10-28", "2026-10-29", "2026-10-30",
            "2026-10-31", "2026-11-01", "2026-11-02", "2026-11-03"]
    regions = ["华东", "华北", "华南"]
    for i, d in enumerate(days):
        for j, region in enumerate(regions):
            b1.append(ev(f"E{d}-{region}", f"{d}T{8 + j:02d}:00:00+08:00",
                         100 + i * 10 + j * 3, region=region, qty=1 + j))
    # 时区边界:UTC 16:30 = 上海次日 00:30,应落入 11-01
    b1.append(ev("EDGE-1", "2026-10-31T16:30:00Z", 45, region="华东"))
    r = ing.ingest_batch(conn, b1, source="seed", note="批次1:基础数据")
    print(f"\n批次1 #{r['batch_id']}: {r['counts']}")
    show_runs("首次汇总", agg.run_all(conn))

    # 批次 2:迟到 + 更正 + 跨月搬移 + 重复
    b2 = [
        ev("LATE-1", "2026-10-28T23:10:00+08:00", 77, region="华北"),                 # 迟到
        ev("E2026-10-28-华东", "2026-10-28T08:00:00+08:00", 900, version=2),          # 更正金额
        ev("E2026-10-31-华南", "2026-11-01T00:20:00+08:00", 210, version=2),          # 更正:跨月搬移
        ev("E2026-10-26-华东", "2026-10-26T08:00:00+08:00", 100),                     # 重复回补
    ]
    r = ing.ingest_batch(conn, b2, source="seed", note="批次2:迟到+更正+搬移+重复")
    print(f"\n批次2 #{r['batch_id']}: {r['counts']}")
    for it in r["items"]:
        print(f"  {it['event_id']:24s} v{it['record_version']} {it['action']:10s} late={it['is_late']} {it.get('detail') or ''}")
    show_runs("增量汇总(更正沿日→周→月传播)", agg.run_all(conn))

    # 批次 3:整批重复回补 → 全部幂等去重,汇总空跑
    r = ing.ingest_batch(conn, b2, source="seed", note="批次3:重复回补")
    print(f"\n批次3 #{r['batch_id']}: {r['counts']}")
    show_runs("重复回补后汇总(应为空跑)", agg.run_all(conn))

    # 演练:week 层失败 → 上一版保留;随后重跑自动追平
    ing.ingest_batch(conn, [ev("N-1", "2026-11-04T10:00:00+08:00", 500, region="华南")], source="seed")
    show_runs("演练:week 层注入失败", agg.run_all(conn, simulate_failure="week"))
    show_runs("重跑(滞后层自动补跑追平)", agg.run_all(conn))

    print("\n完成。启动控制台:  WAREHOUSE_DB=%s .venv/bin/python -m warehouse.app" % DB)
    print("然后访问 http://127.0.0.1:8765/")


if __name__ == "__main__":
    main()
