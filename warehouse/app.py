"""HTTP 层:汇总控制台页面 + JSON API。

路由:
  GET  /                     控制台页面(层级依赖/更正影响/版本切换/核对值/摄取日志)
  GET  /api/state            全局状态:配置、指针、run 列表、核对、一致性、摄取日志
  GET  /api/agg              某层某版本的汇总数据 + 可切换版本列表
  GET  /api/runs/<id>/impact 某 run 的更正影响(桶级旧值→新值 + 贡献事件)
  POST /api/ingest           摄取事件批次(幂等,支持迟到/更正/重复回补)
  POST /api/aggregate        跑一次增量汇总(可选 full_rebuild / simulate_failure 演练)
  POST /api/config/tz        切换时区并全量重建
"""
from __future__ import annotations

import os
from zoneinfo import ZoneInfo

from flask import Flask, g, jsonify, render_template, request

from . import aggregate as agg
from . import ingest as ing
from .db import connect, get_tz, init_db, utcnow

LEVEL_TABLE = {"day": "agg_day", "week": "agg_week", "month": "agg_month"}
DEFAULT_DB = os.environ.get("WAREHOUSE_DB", os.path.join(os.path.dirname(__file__), "..", "data", "warehouse.db"))


def create_app(db_path: str = DEFAULT_DB) -> Flask:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    app = Flask(__name__)
    conn = connect(db_path)
    init_db(conn)
    conn.close()

    def db():
        if "db" not in g:
            g.db = connect(db_path)
        return g.db

    @app.teardown_appcontext
    def close_db(_exc):
        c = g.pop("db", None)
        if c is not None:
            c.close()

    # ------------------------------------------------------------ 页面

    @app.get("/")
    def index():
        return render_template("index.html")

    # ------------------------------------------------------------ 查询 API

    def _consistency(c):
        max_seq = c.execute("SELECT COALESCE(MAX(ingest_seq),0) s FROM fact_events").fetchone()["s"]
        ev = agg._events_totals(c, max_seq)
        out = {
            "watermark": max_seq,
            "events": {"amount_cents": ev[0], "qty": ev[1], "events": ev[2]},
            "levels": {},
        }
        all_match, same_wm = True, True
        for level in agg.LEVELS:
            pid = agg.pointer(c, level)
            if not pid:
                out["levels"][level] = None
                all_match = same_wm = False
                continue
            run = c.execute("SELECT * FROM agg_run WHERE run_id=?", (pid,)).fetchone()
            t = agg._table_totals(c, LEVEL_TABLE[level], pid)
            out["levels"][level] = {
                "run_id": pid, "wm_to": run["wm_to"],
                "amount_cents": t[0], "qty": t[1], "events": t[2], "match": t == ev,
            }
            all_match = all_match and t == ev
            same_wm = same_wm and run["wm_to"] == max_seq
        out["all_match"] = all_match
        out["same_watermark"] = same_wm
        return out

    @app.get("/api/state")
    def state():
        c = db()
        runs = [dict(r) for r in c.execute("SELECT * FROM agg_run ORDER BY run_id DESC LIMIT 60")]
        recon = {}
        for r in c.execute("SELECT * FROM agg_run_recon ORDER BY id"):
            recon.setdefault(r["run_id"], []).append(dict(r))
        pointers = {}
        for level in agg.LEVELS:
            pid = agg.pointer(c, level)
            pointers[level] = dict(c.execute("SELECT * FROM agg_run WHERE run_id=?", (pid,)).fetchone()) if pid else None
        log = [dict(r) for r in c.execute("SELECT * FROM ingest_log ORDER BY id DESC LIMIT 100")]
        batches = [dict(r) for r in c.execute("SELECT * FROM ingest_batch ORDER BY batch_id DESC LIMIT 20")]
        return jsonify({
            "tz": get_tz(c), "now": utcnow(), "runs": runs, "recon": recon,
            "pointers": pointers, "ingest_log": log, "batches": batches,
            "consistency": _consistency(c),
        })

    @app.get("/api/agg")
    def get_agg():
        c = db()
        level = request.args.get("level", "day")
        if level not in LEVEL_TABLE:
            return jsonify({"error": "level 必须是 day|week|month"}), 400
        run_id = request.args.get("run_id", type=int) or agg.pointer(c, level)
        versions = [dict(r) for r in c.execute(
            "SELECT run_id, wm_to, buckets_touched, noop, finished_at FROM agg_run "
            "WHERE level=? AND status='success' AND noop=0 ORDER BY run_id DESC", (level,))]
        rows, run = [], None
        if run_id:
            run_row = c.execute("SELECT * FROM agg_run WHERE run_id=?", (run_id,)).fetchone()
            if run_row is None:
                return jsonify({"error": "run 不存在"}), 404
            if run_row["noop"]:  # 空跑 run 无快照,回落到当前指针版本
                run_id = agg.pointer(c, level)
                run_row = c.execute("SELECT * FROM agg_run WHERE run_id=?", (run_id,)).fetchone() if run_id else None
            run = dict(run_row) if run_row else None
            if level == "day":
                rows = [dict(r) for r in c.execute(
                    "SELECT day AS bucket, region, amount_cents, qty, events FROM agg_day "
                    "WHERE run_id=? ORDER BY day DESC, region", (run_id,))]
            elif level == "week":
                rows = [dict(r) for r in c.execute(
                    "SELECT iso_year, iso_week, week_start, week_end, region, amount_cents, qty, events "
                    "FROM agg_week WHERE run_id=? ORDER BY iso_year DESC, iso_week DESC, region", (run_id,))]
                for r in rows:
                    r["bucket"] = f"{r['iso_year']}-W{r['iso_week']:02d}"
            else:
                rows = [dict(r) for r in c.execute(
                    "SELECT month AS bucket, region, amount_cents, qty, events FROM agg_month "
                    "WHERE run_id=? ORDER BY month DESC, region", (run_id,))]
        return jsonify({"level": level, "run_id": run_id, "run": run, "versions": versions, "rows": rows})

    @app.get("/api/runs/<int:run_id>/impact")
    def impact(run_id):
        c = db()
        run = c.execute("SELECT * FROM agg_run WHERE run_id=?", (run_id,)).fetchone()
        if not run:
            return jsonify({"error": "run 不存在"}), 404
        deltas = [dict(r) for r in c.execute(
            "SELECT * FROM agg_run_bucket WHERE run_id=? ORDER BY bucket, region", (run_id,))]
        events = [dict(r) for r in c.execute(
            """SELECT fe.ingest_seq, fe.event_id, fe.record_version, fe.day_local, fe.region,
                      fe.amount_cents, fe.qty, fe.ingested_at,
                      (SELECT action FROM ingest_log il WHERE il.event_id=fe.event_id
                        AND il.record_version=fe.record_version AND il.batch_id=fe.batch_id
                        ORDER BY id LIMIT 1) AS action,
                      (SELECT is_late FROM ingest_log il WHERE il.event_id=fe.event_id
                        AND il.record_version=fe.record_version AND il.batch_id=fe.batch_id
                        ORDER BY id LIMIT 1) AS is_late
               FROM fact_events fe
               WHERE fe.ingest_seq > ? AND fe.ingest_seq <= ?
               ORDER BY fe.ingest_seq""",
            (run["wm_from"], run["wm_to"]))]
        return jsonify({"run": dict(run), "deltas": deltas, "events": events})

    # ------------------------------------------------------------ 写入 API

    @app.post("/api/ingest")
    def api_ingest():
        payload = request.get_json(force=True, silent=True) or {}
        events = payload.get("events")
        if not isinstance(events, list) or not events:
            return jsonify({"error": "events 必须是非空数组"}), 400
        result = ing.ingest_batch(db(), events, source=payload.get("source", "api"),
                                  note=payload.get("note"))
        return jsonify(result)

    @app.post("/api/aggregate")
    def api_aggregate():
        payload = request.get_json(force=True, silent=True) or {}
        sim = payload.get("simulate_failure")
        if sim not in (None, "day", "week", "month"):
            return jsonify({"error": "simulate_failure 必须是 day|week|month"}), 400
        result = agg.run_all(db(), full_rebuild=bool(payload.get("full_rebuild")),
                             simulate_failure=sim)
        return jsonify({"runs": result})

    @app.post("/api/config/tz")
    def api_tz():
        payload = request.get_json(force=True, silent=True) or {}
        tz = str(payload.get("tz") or "")
        try:
            ZoneInfo(tz)
        except Exception:
            return jsonify({"error": f"未知时区: {tz}"}), 400
        result = agg.change_timezone(db(), tz)
        return jsonify({"tz": tz, "runs": result})

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", "8765")))
