"""Flask API 冒烟测试:摄取 → 汇总 → 状态/版本/影响/核对/时区。"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from warehouse.app import create_app


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "api.db"))
    app.config["TESTING"] = True
    return app.test_client()


def test_full_flow(client):
    # 摄取
    r = client.post("/api/ingest", json={"events": [
        {"event_id": "A", "occurred_at": "2026-10-31T16:30:00Z", "region": "华东", "amount": 10, "qty": 1},
        {"event_id": "B", "occurred_at": "2026-11-01T09:00:00+08:00", "region": "华北", "amount": 20, "qty": 2},
    ]})
    assert r.status_code == 200
    assert r.get_json()["counts"]["insert"] == 2
    # 汇总
    r = client.post("/api/aggregate", json={})
    runs = r.get_json()["runs"]
    assert all(v["status"] == "success" for v in runs.values())
    # 状态:一致性
    st = client.get("/api/state").get_json()
    assert st["consistency"]["all_match"] is True
    assert st["consistency"]["same_watermark"] is True
    assert st["tz"] == "Asia/Shanghai"
    # UTC 16:30 = 上海次日 00:30 → A 落在 2026-11-01
    day_rows = client.get("/api/agg?level=day").get_json()["rows"]
    a_row = [x for x in day_rows if x["region"] == "华东"][0]
    assert a_row["bucket"] == "2026-11-01" and a_row["amount_cents"] == 1000
    # 版本切换:再摄取并汇总后,旧版本仍可查
    v1 = st["pointers"]["day"]["run_id"]
    client.post("/api/ingest", json={"events": [
        {"event_id": "A", "record_version": 2, "occurred_at": "2026-10-31T16:30:00Z",
         "region": "华东", "amount": 99, "qty": 1}]})
    client.post("/api/aggregate", json={})
    old = client.get(f"/api/agg?level=day&run_id={v1}").get_json()
    assert old["run"]["run_id"] == v1
    assert sum(x["amount_cents"] for x in old["rows"]) == 3000  # 更正前的总额
    # 更正影响
    impact = client.get(f"/api/runs/{old['run']['run_id'] + 1}/impact").get_json()
    assert impact["deltas"], "新 run 应有桶级变更记录"
    # 核对值存在且一致
    st2 = client.get("/api/state").get_json()
    assert st2["consistency"]["all_match"] is True


def test_simulate_failure_and_bad_requests(client):
    client.post("/api/ingest", json={"events": [
        {"event_id": "A", "occurred_at": "2026-11-01T09:00:00+08:00", "amount": 10}]})
    client.post("/api/aggregate", json={})
    ptr_day = client.get("/api/state").get_json()["pointers"]["day"]["run_id"]
    client.post("/api/ingest", json={"events": [
        {"event_id": "C", "occurred_at": "2026-11-02T09:00:00+08:00", "amount": 5}]})
    r = client.post("/api/aggregate", json={"simulate_failure": "day"})
    assert r.get_json()["runs"]["day"]["status"] == "failed"
    # 指针未动,上一版可查
    st = client.get("/api/state").get_json()
    assert st["pointers"]["day"]["run_id"] == ptr_day
    # 非法请求
    assert client.post("/api/ingest", json={"events": []}).status_code == 400
    assert client.post("/api/aggregate", json={"simulate_failure": "year"}).status_code == 400
    assert client.post("/api/config/tz", json={"tz": "Mars/Olympus"}).status_code == 400
    assert client.get("/api/agg?level=year").status_code == 400
    # 时区切换 + 全量重建
    r = client.post("/api/config/tz", json={"tz": "UTC"})
    assert r.status_code == 200
    assert client.get("/api/state").get_json()["tz"] == "UTC"


def test_index_page(client):
    r = client.get("/")
    assert r.status_code == 200
    html = r.get_data(as_text=True)
    for section in ("层级依赖", "更正影响", "核对值", "摄取日志", "一致性总览"):
        assert section in html
