"""端到端自测：修订裁决、三班停机计算、失效重算、并发与原子性。"""
from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient


def _ev(device_id, event_id, revision, ts, status):
    return {
        "device_id": device_id,
        "event_id": event_id,
        "revision": revision,
        "event_time": ts,
        "status": status,
    }


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_file = tmp_path / "test.db"
    monkeypatch.setenv("DOWNTIME_DB_PATH", str(db_file))
    # 直接改写模块级路径（在首次 import 前环境变量也已设置，二者一致），
    # 不做 importlib.reload，避免 main -> service -> db 引用出现新旧模块实例混用
    from app import db as db_mod
    from app import main as main_mod
    from app import service as svc_mod

    monkeypatch.setattr(db_mod, "DB_PATH", db_file)
    db_mod.init_db()
    # raise_server_exceptions=False：未捕获异常以 500 返回，便于断言失败回滚
    with TestClient(main_mod.app, raise_server_exceptions=False) as c:
        yield c, svc_mod, db_mod


# --------------------------------------------------------------------------- #
# 1. 修订号裁决：低修订回退拒收、同版异文拒收、同版同文幂等
# --------------------------------------------------------------------------- #
def test_revision_arbitration(client):
    c, _, _ = client
    body = _ev("D1", "E1", 2, "2026-11-10T03:00:00Z", "STOPPED")
    r = c.post("/events", json=body)
    assert r.status_code == 201 and r.json()["accepted"] is True

    # 低修订回退
    r = c.post("/events", json=_ev("D1", "E1", 1, "2026-11-10T04:00:00Z", "RUNNING"))
    assert r.status_code == 409

    # 同版异文（时刻不同）
    r = c.post("/events", json=_ev("D1", "E1", 2, "2026-11-10T05:00:00Z", "STOPPED"))
    assert r.status_code == 409

    # 同版异文（状态不同）
    r = c.post("/events", json=_ev("D1", "E1", 2, "2026-11-10T03:00:00Z", "RUNNING"))
    assert r.status_code == 409

    # 同版同文幂等
    r = c.post("/events", json=body)
    assert r.status_code == 201
    data = r.json()
    assert data["idempotent"] is True
    assert data["data_version"] == 1  # 幂等不增版

    # 高修订接受且版号增加
    r = c.post("/events", json=_ev("D1", "E1", 3, "2026-11-10T06:00:00Z", "RUNNING"))
    assert r.status_code == 201 and r.json()["data_version"] == 2


# --------------------------------------------------------------------------- #
# 2. 三班停机计算 + 班初未知不充作运行 + 班末沿用最后状态
# --------------------------------------------------------------------------- #
def test_shift_calculation_and_unknown_head(client):
    c, svc, _ = client
    # 02:00 STOPPED，05:00 RUNNING；班初 00:00-02:00 无此前状态 => UNKNOWN
    c.post("/events", json=_ev("D2", "A", 1, "2026-11-10T02:00:00Z", "STOPPED"))
    c.post("/events", json=_ev("D2", "B", 1, "2026-11-10T05:00:00Z", "RUNNING"))

    env = c.get("/devices/D2/reports/2026-11-10").json()
    assert env["status"] == "CURRENT"
    rep = env["report"]
    night, day, evening = rep["shifts"]

    assert night["shift"] == "night"
    assert night["unknown_minutes"] == 120          # 00:00-02:00
    assert night["downtime_minutes"] == 180         # 02:00-05:00
    assert night["running_minutes"] == 180          # 05:00-08:00
    statuses = [(i["status"], i["duration_minutes"]) for i in night["intervals"]]
    assert ("UNKNOWN", 120) in statuses
    assert ("STOPPED", 180) in statuses
    assert ("RUNNING", 180) in statuses

    # 白班/晚班沿用 RUNNING，全天最后状态不再变化
    assert day["running_minutes"] == 480 and day["downtime_minutes"] == 0
    assert evening["running_minutes"] == 480 and evening["downtime_minutes"] == 0

    assert rep["total_downtime_minutes"] == 180
    assert rep["total_unknown_minutes"] == 120
    assert set(rep["used_event_ids"]) == {"A", "B"}

    # 纯函数层再校验：无任何事件时 UNKNOWN 绝不计入运行/停机
    content = svc.build_report_content("D2", "2026-11-10", [], 0)
    assert content["total_downtime_minutes"] == 0
    assert content["total_unknown_minutes"] == 24 * 60
    assert all(s["running_minutes"] == 0 for s in content["shifts"])
    assert all(
        i["status"] == "UNKNOWN"
        for s in content["shifts"]
        for i in s["intervals"]
    )


# --------------------------------------------------------------------------- #
# 3. 跨日状态延续：前一日停机延续到次日
# --------------------------------------------------------------------------- #
def test_cross_day_continuity(client):
    c, _, _ = client
    c.post("/events", json=_ev("D3", "S", 1, "2026-11-09T23:00:00Z", "STOPPED"))
    c.post("/events", json=_ev("D3", "R", 1, "2026-11-10T09:30:00Z", "RUNNING"))

    rep = c.get("/devices/D3/reports/2026-11-10").json()["report"]
    night = rep["shifts"][0]
    day = rep["shifts"][1]
    # 夜班 00:00-08:00 全程停机（前一日 23:00 的状态延续）
    assert night["downtime_minutes"] == 480
    # 白班 08:00-09:30 停机 90 分钟，之后运行
    assert day["downtime_minutes"] == 90
    assert day["running_minutes"] == 390
    assert night["intervals"][0]["source_event_id"] == "S"


# --------------------------------------------------------------------------- #
# 4. 更正旧事件：日报失效 + 重算恢复 + 版号更新
# --------------------------------------------------------------------------- #
def test_invalidation_and_recompute(client):
    c, _, _ = client
    c.post("/events", json=_ev("D4", "E", 1, "2026-11-10T10:00:00Z", "STOPPED"))
    env1 = c.get("/devices/D4/reports/2026-11-10").json()
    assert env1["status"] == "CURRENT"
    assert env1["report"]["total_downtime_minutes"] == 14 * 60  # 10:00-24:00
    assert env1["data_version"] == 1

    # 高修订把停机改到 12:00
    c.post("/events", json=_ev("D4", "E", 2, "2026-11-10T12:00:00Z", "STOPPED"))
    env2 = c.get("/devices/D4/reports/2026-11-10").json()
    assert env2["status"] == "INVALIDATED"
    assert env2["report"] is None
    assert env2["invalidated_reason"] is not None
    assert env2["data_version"] == 1  # 失效行保留旧版号

    # 重算后恢复 CURRENT，版号升到 2，停机 12 小时
    env3 = c.get("/devices/D4/reports/2026-11-10?recompute=true").json()
    assert env3["status"] == "CURRENT"
    assert env3["data_version"] == 2
    assert env3["report"]["total_downtime_minutes"] == 12 * 60

    # 再查直接命中 CURRENT 缓存，不会重复重算
    env4 = c.get("/devices/D4/reports/2026-11-10").json()
    assert env4["status"] == "CURRENT" and env4["generated_at"] == env3["generated_at"]


# --------------------------------------------------------------------------- #
# 5. 事件移出原日期：两日日报都失效
# --------------------------------------------------------------------------- #
def test_event_moved_to_another_date_invalidates_both(client):
    c, _, _ = client
    c.post("/events", json=_ev("D5", "E", 1, "2026-11-10T10:00:00Z", "STOPPED"))
    c.post("/events", json=_ev("D5", "R", 1, "2026-11-10T11:00:00Z", "RUNNING"))
    c.get("/devices/D5/reports/2026-11-10")
    c.get("/devices/D5/reports/2026-11-11")  # 11 日延续 RUNNING

    # 修订 2：事件从 11-10 移到 11-11
    c.post("/events", json=_ev("D5", "E", 2, "2026-11-11T02:00:00Z", "STOPPED"))

    listing = {r["report_date"]: r["status"] for r in c.get("/reports?device_id=D5").json()}
    assert listing["2026-11-10"] == "INVALIDATED"
    assert listing["2026-11-11"] == "INVALIDATED"

    rep10 = c.get("/devices/D5/reports/2026-11-10?recompute=true").json()["report"]
    # 10 日不再有停机：00:00-11:00 无此前状态（UNKNOWN 660 分钟），11:00 起 RUNNING
    assert rep10["total_downtime_minutes"] == 0
    assert rep10["total_unknown_minutes"] == 11 * 60
    assert rep10["shifts"][1]["running_minutes"] == 300      # 11:00-16:00 运行
    assert rep10["shifts"][0]["unknown_minutes"] == 480      # 夜班全程未知
    assert rep10["shifts"][1]["unknown_minutes"] == 180      # 08:00-11:00 未知

    rep11 = c.get("/devices/D5/reports/2026-11-11?recompute=true").json()["report"]
    # 11 日 02:00 起停机并延续到班末
    assert rep11["shifts"][0]["downtime_minutes"] == 360  # 02:00-08:00
    assert rep11["total_downtime_minutes"] == 22 * 60      # 02:00-24:00


# --------------------------------------------------------------------------- #
# 6. 批量提交原子性：冲突整批回滚
# --------------------------------------------------------------------------- #
def test_batch_atomicity(client):
    c, _, _ = client
    ok = [
        _ev("D6", "E1", 1, "2026-11-10T01:00:00Z", "STOPPED"),
        _ev("D6", "E2", 1, "2026-11-10T02:00:00Z", "RUNNING"),
    ]
    r = c.post("/events/batch", json=ok)
    assert r.status_code == 201 and r.json()["accepted_count"] == 2

    bad = [
        _ev("D6", "E3", 1, "2026-11-10T03:00:00Z", "STOPPED"),
        _ev("D6", "E1", 1, "2026-11-10T04:00:00Z", "RUNNING"),  # 同版异文冲突
    ]
    r = c.post("/events/batch", json=bad)
    assert r.status_code == 409
    events = c.get("/devices/D6/events").json()
    assert {e["event_id"] for e in events} == {"E1", "E2"}  # E3 随整批回滚，未落库


# --------------------------------------------------------------------------- #
# 7. 并发更正：同事件并发高修订，无混合版本，日报正确失效
# --------------------------------------------------------------------------- #
def test_concurrent_revisions(client):
    c, svc, db_mod = client
    c.post("/events", json=_ev("D7", "E", 1, "2026-11-10T10:00:00Z", "STOPPED"))
    c.get("/devices/D7/reports/2026-11-10")

    errors = []

    def worker(rev: int):
        try:
            # 时刻全部落在 10 日 10:xx，避免小时越界；时刻各不相同
            r = c.post(
                "/events",
                json=_ev("D7", "E", rev, f"2026-11-10T10:{rev % 60:02d}:00Z",
                         "STOPPED" if rev % 2 else "RUNNING"),
            )
            if r.status_code not in (201, 409):
                errors.append((rev, r.status_code))
        except Exception as exc:  # pragma: no cover
            errors.append((rev, repr(exc)))

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(worker, range(2, 50)))

    assert errors == []
    # 最高修订 49 必然落库，且每个修订最多一行（无同版异文脏数据）
    rows = svc.list_events("D7")
    revs = sorted(r["revision"] for r in rows if r["event_id"] == "E")
    assert revs[0] == 1 and revs[-1] == 49 and len(revs) == len(set(revs))

    # 日报必须已失效；重算后与当前最高修订一致
    env = c.get("/devices/D7/reports/2026-11-10").json()
    assert env["status"] == "INVALIDATED"
    rec = c.get("/devices/D7/reports/2026-11-10?recompute=true").json()
    assert rec["status"] == "CURRENT"
    assert rec["data_version"] == len(revs)  # 每次接受 +1
    assert rec["report"]["used_event_ids"] == ["E"]


# --------------------------------------------------------------------------- #
# 8. 写事务失败不产生半份日报 / 不留失效脏状态
# --------------------------------------------------------------------------- #
def test_write_failure_rolls_back(client, monkeypatch):
    c, svc, db_mod = client
    c.post("/events", json=_ev("D8", "E", 1, "2026-11-10T10:00:00Z", "STOPPED"))
    c.get("/devices/D8/reports/2026-11-10")

    # 注入故障：代理包装连接，让“日报失效 UPDATE”抛错，验证整个写事务回滚
    real_factory = db_mod.connection_factory

    class FailingConnection:
        def __init__(self, *args, **kwargs):
            self._inner = real_factory(*args, **kwargs)

        def execute(self, sql, params=()):
            if isinstance(sql, str) and sql.lstrip().startswith(
                "UPDATE reports SET status='INVALIDATED'"
            ):
                raise sqlite3.OperationalError("injected failure")
            return self._inner.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    def failing_factory(*args, **kwargs):
        return FailingConnection(*args, **kwargs)

    monkeypatch.setattr(db_mod, "connection_factory", failing_factory)
    r = c.post("/events", json=_ev("D8", "E", 2, "2026-11-10T12:00:00Z", "RUNNING"))
    assert r.status_code == 500
    monkeypatch.setattr(db_mod, "connection_factory", real_factory)

    # 修订 2 必须随事务整体回滚
    rows = svc.list_events("D8")
    assert [(x["revision"], x["status"]) for x in rows] == [(1, "STOPPED")]
    # 日报仍是 CURRENT，未出现“事件已改但日报半失效”的混合状态；版号仍为 1
    env = c.get("/devices/D8/reports/2026-11-10").json()
    assert env["status"] == "CURRENT"
    assert env["data_version"] == 1
    assert env["report"]["total_downtime_minutes"] == 14 * 60


# --------------------------------------------------------------------------- #
# 9. 边界：事件恰好落在班次/日期分界点
# --------------------------------------------------------------------------- #
def test_boundary_timestamps(client):
    c, _, _ = client
    c.post("/events", json=_ev("D9", "A", 1, "2026-11-10T00:00:00Z", "STOPPED"))
    c.post("/events", json=_ev("D9", "B", 1, "2026-11-10T08:00:00Z", "RUNNING"))
    c.post("/events", json=_ev("D9", "C", 1, "2026-11-10T16:00:00Z", "STOPPED"))

    rep = c.get("/devices/D9/reports/2026-11-10").json()["report"]
    night, day, evening = rep["shifts"]
    assert night["downtime_minutes"] == 480
    assert day["running_minutes"] == 480
    assert evening["downtime_minutes"] == 480
    # 每班只有一个区间，没有零时长碎片
    assert all(len(s["intervals"]) == 1 for s in rep["shifts"])

    # 次日 00:00 延续晚班停机
    nxt = c.get("/devices/D9/reports/2026-11-11").json()["report"]
    assert nxt["shifts"][0]["downtime_minutes"] == 480


# --------------------------------------------------------------------------- #
# 10. 无任何事件的设备：全天 UNKNOWN
# --------------------------------------------------------------------------- #
def test_no_events_all_unknown(client):
    c, _, _ = client
    rep = c.get("/devices/D10/reports/2026-11-10").json()["report"]
    assert rep["total_downtime_minutes"] == 0
    assert rep["total_unknown_minutes"] == 24 * 60
    assert all(s["unknown_minutes"] == 480 for s in rep["shifts"])
    assert rep["used_event_ids"] == []


# --------------------------------------------------------------------------- #
# 11. 输入校验：无时区时间、非法状态、非法日期
# --------------------------------------------------------------------------- #
def test_input_validation(client):
    c, _, _ = client
    r = c.post("/events", json=_ev("D11", "E", 1, "2026-11-10T10:00:00", "RUNNING"))
    assert r.status_code == 422  # 必须带时区
    r = c.post("/events", json=_ev("D11", "E", 1, "2026-11-10T10:00:00Z", "BROKEN"))
    assert r.status_code == 422
    r = c.get("/devices/D11/reports/not-a-date")
    assert r.status_code == 422


# --------------------------------------------------------------------------- #
# 12. 并发首次生成同一日报：不报错、不产生重复/半份行，结果一致
# --------------------------------------------------------------------------- #
def test_concurrent_first_report_creation(client):
    c, _, _ = client
    c.post("/events", json=_ev("D12", "E", 1, "2026-11-10T10:00:00Z", "STOPPED"))

    results, statuses = [], []

    def worker(_):
        r = c.get("/devices/D12/reports/2026-11-10")
        statuses.append(r.status_code)
        results.append(r.json())

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(worker, range(8)))

    assert set(statuses) == {200}
    assert all(r["status"] == "CURRENT" for r in results)
    assert len({r["generated_at"] for r in results}) == 1
    listing = c.get("/reports?device_id=D12").json()
    assert len(listing) == 1  # 只有一行日报，无重复/半份残留
