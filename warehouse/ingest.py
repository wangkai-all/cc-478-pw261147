"""事件摄取:幂等去重、版本更正、迟到检测。

动作分类(写入 ingest_log,全程可审计):
- insert     新事件
- correction 同 event_id 更高 record_version → 新行生效,旧行保留为历史
- duplicate  同 event_id 同版本且内容一致(重复回补)→ 安全空操作
- stale      更低版本(乱序旧数据)→ 拒绝
- conflict   同版本但内容不同(生产端 bug)→ 拒绝
- error      载荷非法
迟到(is_late):事件落入已被当前 day 版本覆盖的日期,提示需要增量回补。
"""
from __future__ import annotations

from . import calendar as cal
from .db import get_tz, utcnow

PAYLOAD_KEYS = ("occurred_at_utc", "region", "courier", "amount_cents", "qty")


def _normalize(raw: dict, tz: str) -> dict:
    event_id = str(raw.get("event_id") or "").strip()
    if not event_id:
        raise ValueError("event_id 不能为空")
    version = int(raw.get("record_version", 1))
    if version < 1:
        raise ValueError("record_version 必须 >= 1")
    if "occurred_at" not in raw:
        raise ValueError("缺少 occurred_at")
    epoch = cal.parse_occurred_at(raw["occurred_at"])
    region = str(raw.get("region") or "未分区").strip()
    amount_cents = int(round(float(raw.get("amount", 0)) * 100))
    qty = int(raw.get("qty", 1))
    return {
        "event_id": event_id,
        "record_version": version,
        "occurred_at_utc": epoch,
        "day_local": cal.epoch_to_day(epoch, tz),
        "region": region,
        "courier": raw.get("courier"),
        "amount_cents": amount_cents,
        "qty": qty,
    }


def _max_aggregated_day(conn) -> str | None:
    row = conn.execute(
        """SELECT MAX(ad.day) AS d FROM agg_day ad
           JOIN agg_pointer p ON p.level = 'day' AND p.run_id = ad.run_id"""
    ).fetchone()
    return row["d"] if row and row["d"] else None


def ingest_batch(conn, events: list[dict], source: str = "api", note: str | None = None) -> dict:
    tz = get_tz(conn)
    cur = conn.execute(
        "INSERT INTO ingest_batch(source, note, submitted_at) VALUES (?, ?, ?)",
        (source, note, utcnow()),
    )
    batch_id = cur.lastrowid
    max_day = _max_aggregated_day(conn)
    counts = {"insert": 0, "correction": 0, "duplicate": 0, "stale": 0, "conflict": 0, "error": 0}
    items = []
    for raw in events:
        try:
            ev = _normalize(raw, tz)
        except (ValueError, TypeError) as e:
            counts["error"] += 1
            conn.execute(
                """INSERT INTO ingest_log(batch_id, event_id, record_version, action, is_late, day_local, detail, created_at)
                   VALUES (?, ?, ?, 'error', 0, NULL, ?, ?)""",
                (batch_id, str(raw.get("event_id") or "?"), 0, str(e), utcnow()),
            )
            items.append({"event_id": raw.get("event_id"), "action": "error", "detail": str(e)})
            continue

        rows = conn.execute(
            "SELECT * FROM fact_events WHERE event_id = ? ORDER BY record_version",
            (ev["event_id"],),
        ).fetchall()
        detail = None
        if not rows:
            action = "insert"
        else:
            latest = rows[-1]
            v = ev["record_version"]
            if v > latest["record_version"]:
                action = "correction"
                detail = f"v{latest['record_version']}→v{v}"
            elif v == latest["record_version"]:
                if all(ev[k] == latest[k] for k in PAYLOAD_KEYS):
                    action = "duplicate"
                else:
                    action = "conflict"
                    detail = "同版本不同内容,已拒绝"
            else:
                action = "stale"
                detail = f"已有更高版本 v{latest['record_version']}"

        is_late = 1 if (max_day and action in ("insert", "correction") and ev["day_local"] <= max_day) else 0
        if action in ("insert", "correction"):
            conn.execute(
                """INSERT INTO fact_events(event_id, record_version, occurred_at_utc, day_local,
                                           region, courier, amount_cents, qty, batch_id, ingested_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (ev["event_id"], ev["record_version"], ev["occurred_at_utc"], ev["day_local"],
                 ev["region"], ev["courier"], ev["amount_cents"], ev["qty"], batch_id, utcnow()),
            )
        conn.execute(
            """INSERT INTO ingest_log(batch_id, event_id, record_version, action, is_late, day_local, detail, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (batch_id, ev["event_id"], ev["record_version"], action, is_late, ev["day_local"], detail, utcnow()),
        )
        counts[action] += 1
        items.append({
            "event_id": ev["event_id"], "record_version": ev["record_version"], "action": action,
            "is_late": bool(is_late), "day_local": ev["day_local"], "detail": detail,
        })
    return {"batch_id": batch_id, "counts": counts, "items": items}
