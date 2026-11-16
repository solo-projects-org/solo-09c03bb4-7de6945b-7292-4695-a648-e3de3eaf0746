"""独立状态采样接收与按设备 / UTC 日期核验的端到端自测。"""
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


def _smp(device_id, sample_id, ts, status):
    return {
        "device_id": device_id,
        "sample_id": sample_id,
        "sample_time": ts,
        "status": status,
    }


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_file = tmp_path / "samples.db"
    monkeypatch.setenv("DOWNTIME_DB_PATH", str(db_file))
    from app import db as db_mod
    from app import main as main_mod
    from app import service as svc_mod

    monkeypatch.setattr(db_mod, "DB_PATH", db_file)
    db_mod.init_db()
    with TestClient(main_mod.app, raise_server_exceptions=False) as c:
        yield c, svc_mod, db_mod


def _failing_factory(real_factory, should_fail):
    """构造在特定 SQL 上抛错的连接工厂，用于验证事务整体回滚。

    注意：db.get_connection 把 row_factory 设在包装器外层，内层连接必须
    同样设为 sqlite3.Row，否则包装层查询会退化为元组、掩盖真实故障点。
    """

    class FailingConnection:
        def __init__(self, *args, **kwargs):
            self._inner = real_factory(*args, **kwargs)
            self._inner.row_factory = sqlite3.Row

        def execute(self, sql, params=()):
            if isinstance(sql, str) and should_fail(sql.lstrip()):
                raise sqlite3.OperationalError("injected failure")
            return self._inner.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    return lambda *a, **k: FailingConnection(*a, **k)


# --------------------------------------------------------------------------- #
# 1. 采样接收：同号同文幂等、同号异文拒收、按设备隔离、输入校验
# --------------------------------------------------------------------------- #
def test_sample_idempotency_and_conflict(client):
    c, _, _ = client
    body = _smp("S1", "P-1", "2026-11-10T03:00:00Z", "STOPPED")
    r = c.post("/samples", json=body)
    assert r.status_code == 201
    assert r.json()["idempotent"] is False and r.json()["sample_version"] == 1

    # 同号同文重复提交：幂等，不增版
    r = c.post("/samples", json=body)
    assert r.status_code == 201
    assert r.json()["idempotent"] is True and r.json()["sample_version"] == 1

    # 同号异文：状态不同 / 时刻不同都拒收
    r = c.post("/samples", json=_smp("S1", "P-1", "2026-11-10T03:00:00Z", "RUNNING"))
    assert r.status_code == 409
    r = c.post("/samples", json=_smp("S1", "P-1", "2026-11-10T04:00:00Z", "STOPPED"))
    assert r.status_code == 409

    # 版号仍为 1，数据未被异文污染
    rows = c.get("/devices/S1/samples").json()
    assert [(x["sample_id"], x["status"], x["sample_time"]) for x in rows] == [
        ("P-1", "STOPPED", "2026-11-10T03:00:00Z")
    ]

    # 采样号只在设备内唯一：另一设备同号可接收
    r = c.post("/samples", json=_smp("S2", "P-1", "2026-11-10T03:00:00Z", "RUNNING"))
    assert r.status_code == 201 and r.json()["sample_version"] == 1

    # 输入校验：时刻必须带时区、状态枚举、采样号非空
    assert c.post(
        "/samples", json=_smp("S1", "P-2", "2026-11-10T05:00:00", "RUNNING")
    ).status_code == 422
    assert c.post(
        "/samples", json=_smp("S1", "P-2", "2026-11-10T05:00:00Z", "BROKEN")
    ).status_code == 422
    assert c.post(
        "/samples", json=_smp("S1", "", "2026-11-10T05:00:00Z", "RUNNING")
    ).status_code == 422
    assert c.get("/devices/S1/verifications/not-a-date").status_code == 422


# --------------------------------------------------------------------------- #
# 2. 批量采样：整批原子，任一冲突全部回滚
# --------------------------------------------------------------------------- #
def test_batch_samples_atomicity(client):
    c, _, _ = client
    ok = [
        _smp("S3", "A", "2026-11-10T01:00:00Z", "STOPPED"),
        _smp("S3", "B", "2026-11-10T02:00:00Z", "RUNNING"),
    ]
    r = c.post("/samples/batch", json=ok)
    assert r.status_code == 201
    assert r.json() == {"accepted_count": 2, "idempotent_count": 0, "sample_version": 2}

    bad = [
        _smp("S3", "C", "2026-11-10T03:00:00Z", "STOPPED"),
        _smp("S3", "A", "2026-11-10T04:00:00Z", "RUNNING"),  # 同号异文
    ]
    assert c.post("/samples/batch", json=bad).status_code == 409
    rows = c.get("/devices/S3/samples").json()
    assert {x["sample_id"] for x in rows} == {"A", "B"}  # C 随整批回滚

    # 跨设备批量拒收，不部分生效
    mixed = [
        _smp("S3", "D", "2026-11-10T05:00:00Z", "STOPPED"),
        _smp("S4", "E", "2026-11-10T05:00:00Z", "STOPPED"),
    ]
    assert c.post("/samples/batch", json=mixed).status_code == 409
    assert c.get("/devices/S4/samples").json() == []


# --------------------------------------------------------------------------- #
# 3. 核验：按事件时序逐条对照，列出冲突采样号 + 双方状态 + 依据事件号；
#    首次切换前的采样为 UNKNOWN
# --------------------------------------------------------------------------- #
def test_verification_conflicts_and_unknown(client):
    c, _, _ = client
    # 事件时序：06:00 起 RUNNING(E1)，10:00 起 STOPPED(E2)，13:00 起 RUNNING(E3)
    c.post("/events", json=_ev("V1", "E1", 1, "2026-11-10T06:00:00Z", "RUNNING"))
    c.post("/events", json=_ev("V1", "E2", 1, "2026-11-10T10:00:00Z", "STOPPED"))
    c.post("/events", json=_ev("V1", "E3", 1, "2026-11-10T13:00:00Z", "RUNNING"))

    samples = [
        _smp("V1", "S-BEFORE", "2026-11-10T05:00:00Z", "RUNNING"),  # 首次切换前
        _smp("V1", "S-AT", "2026-11-10T06:00:00Z", "RUNNING"),      # 恰在切换点
        _smp("V1", "S-OK1", "2026-11-10T07:00:00Z", "RUNNING"),
        _smp("V1", "S-BAD", "2026-11-10T09:00:00Z", "STOPPED"),     # 事件仍 RUNNING
        _smp("V1", "S-OK2", "2026-11-10T11:00:00Z", "STOPPED"),
        _smp("V1", "S-OK3", "2026-11-10T14:00:00Z", "RUNNING"),
    ]
    for s in samples:
        assert c.post("/samples", json=s).status_code == 201

    env = c.get("/devices/V1/verifications/2026-11-10").json()
    assert env["status"] == "CURRENT"
    assert env["verified"] is True
    assert env["event_version"] == 3
    assert env["sample_version"] == 6
    assert env["sample_count"] == 6
    assert env["conflict_count"] == 1
    assert env["unknown_count"] == 1

    assert [s["sample_id"] for s in env["samples"]] == [
        "S-BEFORE", "S-AT", "S-OK1", "S-BAD", "S-OK2", "S-OK3"
    ]
    by_id = {s["sample_id"]: s for s in env["samples"]}
    assert by_id["S-BEFORE"]["outcome"] == "UNKNOWN"
    assert by_id["S-BEFORE"]["event_status"] == "UNKNOWN"
    assert by_id["S-BEFORE"]["basis_event_id"] is None
    assert by_id["S-AT"]["outcome"] == "MATCH"
    assert by_id["S-AT"]["basis_event_id"] == "E1"

    assert env["conflicts"] == [
        {
            "sample_id": "S-BAD",
            "sample_time": "2026-11-10T09:00:00Z",
            "sample_status": "STOPPED",
            "event_status": "RUNNING",
            "basis_event_id": "E1",
            "basis_event_time": "2026-11-10T06:00:00Z",
        }
    ]


# --------------------------------------------------------------------------- #
# 4. 无采样的日期不发布核验；补采样后才可核验
# --------------------------------------------------------------------------- #
def test_no_samples_never_verified(client):
    c, _, _ = client
    c.post("/samples", json=_smp("V2", "A", "2026-11-10T03:00:00Z", "RUNNING"))
    c.post("/events", json=_ev("V2", "E", 1, "2026-11-10T01:00:00Z", "RUNNING"))

    env = c.get("/devices/V2/verifications/2026-11-11").json()
    assert env["status"] == "NO_SAMPLES"
    assert env["verified"] is False
    assert env["sample_count"] == 0
    assert env["samples"] == [] and env["conflicts"] == []
    assert c.get("/verifications?device_id=V2").json() == []  # 未落“已核验”行

    # 强制重算也不能把无采样日期标为已核验
    env = c.get("/devices/V2/verifications/2026-11-11?recompute=true").json()
    assert env["status"] == "NO_SAMPLES" and env["verified"] is False
    assert c.get("/verifications?device_id=V2").json() == []

    # 补采样后该日核验发布，并保留两路版号
    c.post("/samples", json=_smp("V2", "B", "2026-11-11T03:00:00Z", "RUNNING"))
    env = c.get("/devices/V2/verifications/2026-11-11").json()
    assert env["status"] == "CURRENT" and env["verified"] is True
    assert env["event_version"] == 1 and env["sample_version"] == 2
    assert env["samples"][0]["outcome"] == "MATCH"      # 状态跨日延续
    assert env["samples"][0]["basis_event_id"] == "E"


# --------------------------------------------------------------------------- #
# 5. 采样补传：只失效其 UTC 日期，其他日期继续可用；重算后恢复
# --------------------------------------------------------------------------- #
def test_late_sample_invalidates_only_its_date(client):
    c, _, _ = client
    c.post("/events", json=_ev("V3", "E", 1, "2026-11-09T23:00:00Z", "STOPPED"))
    c.post("/events", json=_ev("V3", "R", 1, "2026-11-10T12:00:00Z", "RUNNING"))
    c.post("/samples", json=_smp("V3", "D10-1", "2026-11-10T01:00:00Z", "STOPPED"))
    c.post("/samples", json=_smp("V3", "D11-1", "2026-11-11T01:00:00Z", "RUNNING"))

    v10 = c.get("/devices/V3/verifications/2026-11-10").json()
    v11 = c.get("/devices/V3/verifications/2026-11-11").json()
    assert v10["status"] == "CURRENT" and v10["sample_version"] == 2
    assert v11["status"] == "CURRENT" and v11["sample_version"] == 2

    # 延迟补传 10 日采样 → 仅 10 日核验失效，11 日继续可用
    r = c.post("/samples", json=_smp("V3", "D10-2", "2026-11-10T10:00:00Z", "STOPPED"))
    assert r.status_code == 201 and r.json()["sample_version"] == 3

    v10_late = c.get("/devices/V3/verifications/2026-11-10").json()
    assert v10_late["status"] == "INVALIDATED"
    assert v10_late["verified"] is False
    assert v10_late["invalidated_reason"] is not None
    assert v10_late["event_version"] == 2      # 失效行保留旧的两路版号
    assert v10_late["sample_version"] == 2
    # 失效后不再发布旧对照明细，避免把过期结论当作当前核验
    assert v10_late["samples"] == [] and v10_late["conflicts"] == []
    assert v10_late["sample_count"] == 0

    v11_after = c.get("/devices/V3/verifications/2026-11-11").json()
    assert v11_after["status"] == "CURRENT"
    assert v11_after["generated_at"] == v11["generated_at"]  # 未受影响，不重算
    assert v11_after["samples"][0]["outcome"] == "MATCH"

    # 重算 10 日：恢复 CURRENT，采样版号升为 3，两条采样都参与
    v10_new = c.get("/devices/V3/verifications/2026-11-10?recompute=true").json()
    assert v10_new["status"] == "CURRENT" and v10_new["verified"] is True
    assert v10_new["sample_version"] == 3 and v10_new["event_version"] == 2
    assert v10_new["sample_count"] == 2
    assert all(s["outcome"] == "MATCH" for s in v10_new["samples"])

    listing = {
        r["check_date"]: r["status"]
        for r in c.get("/verifications?device_id=V3").json()
    }
    assert listing == {"2026-11-10": "CURRENT", "2026-11-11": "CURRENT"}


# --------------------------------------------------------------------------- #
# 6. 事件追溯更正：只失效核验结果真正变化的日期
# --------------------------------------------------------------------------- #
def test_event_correction_invalidates_affected_dates_only(client):
    c, _, _ = client
    c.post("/events", json=_ev("V4", "A", 1, "2026-11-10T08:00:00Z", "RUNNING"))
    c.post("/events", json=_ev("V4", "E", 1, "2026-11-10T10:00:00Z", "STOPPED"))
    c.post("/events", json=_ev("V4", "R", 1, "2026-11-10T23:00:00Z", "RUNNING"))
    c.post("/samples", json=_smp("V4", "D10", "2026-11-10T09:00:00Z", "RUNNING"))
    c.post("/samples", json=_smp("V4", "D11", "2026-11-11T01:00:00Z", "RUNNING"))

    assert c.get("/devices/V4/verifications/2026-11-10").json()["status"] == "CURRENT"
    v11 = c.get("/devices/V4/verifications/2026-11-11").json()
    assert v11["status"] == "CURRENT" and v11["samples"][0]["outcome"] == "MATCH"

    # 把 E 从 10:00 更正到 11:00：10 日的期望状态区间改变，11 日仍由 R 延续
    c.post("/events", json=_ev("V4", "E", 2, "2026-11-10T11:00:00Z", "STOPPED"))

    v10 = c.get("/devices/V4/verifications/2026-11-10").json()
    assert v10["status"] == "INVALIDATED"
    assert v10["event_version"] == 3          # 保留更正前的版号
    v11_after = c.get("/devices/V4/verifications/2026-11-11").json()
    assert v11_after["status"] == "CURRENT"    # 未受影响，继续可用
    assert v11_after["generated_at"] == v11["generated_at"]

    # 重算 10 日：09:00 仍应为 RUNNING(A)，但依据/版号已更新
    v10_new = c.get("/devices/V4/verifications/2026-11-10?recompute=true").json()
    assert v10_new["status"] == "CURRENT"
    assert v10_new["event_version"] == 4      # 三次初始事件 + 一次更正
    assert v10_new["samples"][0]["outcome"] == "MATCH"
    assert v10_new["samples"][0]["basis_event_id"] == "A"


# --------------------------------------------------------------------------- #
# 7. 状态跨日延续：更正延续状态会波及后续日期，直到下一次切换
# --------------------------------------------------------------------------- #
def test_cross_day_correction_invalidates_carry_days(client):
    c, _, _ = client
    c.post("/events", json=_ev("V5", "E", 1, "2026-11-09T20:00:00Z", "STOPPED"))
    c.post("/samples", json=_smp("V5", "D09", "2026-11-09T21:00:00Z", "STOPPED"))
    c.post("/samples", json=_smp("V5", "D10", "2026-11-10T10:00:00Z", "STOPPED"))
    c.post("/samples", json=_smp("V5", "D11", "2026-11-11T10:00:00Z", "STOPPED"))

    for d in ("2026-11-09", "2026-11-10", "2026-11-11"):
        assert c.get(f"/devices/V5/verifications/{d}").json()["status"] == "CURRENT"

    # 更正延续中的状态：09 日 20:00 起改为 RUNNING，持续到后续日期
    c.post("/events", json=_ev("V5", "E", 2, "2026-11-09T20:00:00Z", "RUNNING"))

    states = {
        r["check_date"]: r["status"]
        for r in c.get("/verifications?device_id=V5").json()
    }
    assert states == {
        "2026-11-09": "INVALIDATED",
        "2026-11-10": "INVALIDATED",
        "2026-11-11": "INVALIDATED",
    }

    v10 = c.get("/devices/V5/verifications/2026-11-10?recompute=true").json()
    assert v10["status"] == "CURRENT"
    assert v10["conflicts"] == [
        {
            "sample_id": "D10",
            "sample_time": "2026-11-10T10:00:00Z",
            "sample_status": "STOPPED",
            "event_status": "RUNNING",
            "basis_event_id": "E",
            "basis_event_time": "2026-11-09T20:00:00Z",
        }
    ]


# --------------------------------------------------------------------------- #
# 7b. 仅推移延续状态的事件时刻：涉及依据时刻的日期失效，更早日期不受影响
# --------------------------------------------------------------------------- #
def test_event_time_shift_only_invalidates_from_event_onward(client):
    c, _, _ = client
    c.post("/events", json=_ev("V5B", "E", 1, "2026-11-09T20:00:00Z", "STOPPED"))
    c.post("/samples", json=_smp("V5B", "D08", "2026-11-08T10:00:00Z", "RUNNING"))
    c.post("/samples", json=_smp("V5B", "D09", "2026-11-09T21:00:00Z", "STOPPED"))
    c.post("/samples", json=_smp("V5B", "D10", "2026-11-10T10:00:00Z", "STOPPED"))

    for d in ("2026-11-08", "2026-11-09", "2026-11-10"):
        assert c.get(f"/devices/V5B/verifications/{d}").json()["status"] == "CURRENT"
    v08 = c.get("/devices/V5B/verifications/2026-11-08").json()

    # 状态不变，仅把延续中的停机推迟一小时：08 日无依据事件变化，继续可用
    c.post("/events", json=_ev("V5B", "E", 2, "2026-11-09T21:00:00Z", "STOPPED"))

    v08_after = c.get("/devices/V5B/verifications/2026-11-08").json()
    assert v08_after["status"] == "CURRENT"
    assert v08_after["generated_at"] == v08["generated_at"]
    states = {
        r["check_date"]: r["status"]
        for r in c.get("/verifications?device_id=V5B").json()
    }
    assert states == {
        "2026-11-08": "CURRENT",
        "2026-11-09": "INVALIDATED",
        "2026-11-10": "INVALIDATED",
    }

    # 重算 10 日：状态仍是 STOPPED，但依据事件时刻已更新为更正后的 21:00
    v10 = c.get("/devices/V5B/verifications/2026-11-10?recompute=true").json()
    assert v10["status"] == "CURRENT" and v10["conflict_count"] == 0
    assert v10["samples"][0]["basis_event_id"] == "E"
    assert v10["samples"][0]["basis_event_time"] == "2026-11-09T21:00:00Z"


# --------------------------------------------------------------------------- #
# 7c. 事件跨日移动：原日期的后续采样依据改变，新旧日期核验都失效
# --------------------------------------------------------------------------- #
def test_event_moved_across_days_invalidates_both(client):
    c, _, _ = client
    c.post("/events", json=_ev("V5C", "A", 1, "2026-11-10T10:00:00Z", "RUNNING"))
    c.post("/events", json=_ev("V5C", "B", 1, "2026-11-10T12:00:00Z", "STOPPED"))
    c.post("/samples", json=_smp("V5C", "D10", "2026-11-10T13:00:00Z", "STOPPED"))
    c.post("/samples", json=_smp("V5C", "D11", "2026-11-11T03:00:00Z", "STOPPED"))

    v10 = c.get("/devices/V5C/verifications/2026-11-10").json()
    v11 = c.get("/devices/V5C/verifications/2026-11-11").json()
    assert v10["conflict_count"] == 0 and v10["samples"][0]["basis_event_id"] == "B"
    assert v11["conflict_count"] == 0 and v11["samples"][0]["basis_event_id"] == "B"

    # 把 B 从 10 日 12:00 移到 11 日 02:00（状态不变）
    c.post("/events", json=_ev("V5C", "B", 2, "2026-11-11T02:00:00Z", "STOPPED"))

    states = {
        r["check_date"]: r["status"]
        for r in c.get("/verifications?device_id=V5C").json()
    }
    assert states == {"2026-11-10": "INVALIDATED", "2026-11-11": "INVALIDATED"}

    # 10 日 13:00 的采样现在应以 A（RUNNING）为依据 → 冲突
    new10 = c.get("/devices/V5C/verifications/2026-11-10?recompute=true").json()
    assert new10["status"] == "CURRENT"
    assert new10["conflict_count"] == 1
    assert new10["conflicts"][0]["basis_event_id"] == "A"
    assert new10["conflicts"][0]["event_status"] == "RUNNING"
    assert new10["conflicts"][0]["sample_status"] == "STOPPED"

    # 11 日 03:00 仍由 B 决定，但依据时刻已更新
    new11 = c.get("/devices/V5C/verifications/2026-11-11?recompute=true").json()
    assert new11["status"] == "CURRENT" and new11["conflict_count"] == 0
    assert new11["samples"][0]["basis_event_time"] == "2026-11-11T02:00:00Z"


# --------------------------------------------------------------------------- #
# 8. 采样时刻按 UTC 归日（带偏移的输入统一换算）
# --------------------------------------------------------------------------- #
def test_sample_time_normalized_to_utc_date(client):
    c, _, _ = client
    # 2026-11-11T02:00+08:00 == 2026-11-10T18:00Z
    r = c.post("/samples", json=_smp("V6", "TZ", "2026-11-11T02:00:00+08:00", "RUNNING"))
    assert r.status_code == 201
    rows = c.get("/devices/V6/samples").json()
    assert rows[0]["sample_time"] == "2026-11-10T18:00:00Z"
    assert rows[0]["sample_date"] == "2026-11-10"

    assert c.get("/devices/V6/verifications/2026-11-10").json()["sample_count"] == 1
    assert c.get("/devices/V6/verifications/2026-11-11").json()["status"] == "NO_SAMPLES"


# --------------------------------------------------------------------------- #
# 9. 采样写入失败：采样、失效标记、采样版号整体回滚，核验不被半失效
# --------------------------------------------------------------------------- #
def test_sample_write_failure_rolls_back(client, monkeypatch):
    c, _, db_mod = client
    c.post("/events", json=_ev("V7", "E", 1, "2026-11-10T01:00:00Z", "RUNNING"))
    c.post("/samples", json=_smp("V7", "A", "2026-11-10T03:00:00Z", "RUNNING"))
    before = c.get("/devices/V7/verifications/2026-11-10").json()
    assert before["status"] == "CURRENT" and before["sample_version"] == 1

    real_factory = db_mod.connection_factory
    monkeypatch.setattr(
        db_mod,
        "connection_factory",
        _failing_factory(
            real_factory, lambda sql: sql.startswith("INSERT INTO sample_versions")
        ),
    )
    r = c.post("/samples", json=_smp("V7", "B", "2026-11-10T05:00:00Z", "STOPPED"))
    assert r.status_code == 500
    monkeypatch.setattr(db_mod, "connection_factory", real_factory)

    # 采样 B 未落库、核验仍是原 CURRENT（未留下“已失效”半状态）
    assert [s["sample_id"] for s in c.get("/devices/V7/samples").json()] == ["A"]
    after = c.get("/devices/V7/verifications/2026-11-10").json()
    assert after["status"] == "CURRENT"
    assert after["generated_at"] == before["generated_at"]
    assert after["sample_version"] == 1 and after["sample_count"] == 1


# --------------------------------------------------------------------------- #
# 10. 事件写入失败：事件、核验失效、事件版号整体回滚
# --------------------------------------------------------------------------- #
def test_event_write_failure_keeps_verification_current(client, monkeypatch):
    c, _, db_mod = client
    c.post("/events", json=_ev("V8", "E", 1, "2026-11-10T10:00:00Z", "STOPPED"))
    c.post("/samples", json=_smp("V8", "A", "2026-11-10T11:00:00Z", "STOPPED"))
    before = c.get("/devices/V8/verifications/2026-11-10").json()
    assert before["status"] == "CURRENT" and before["event_version"] == 1

    real_factory = db_mod.connection_factory
    monkeypatch.setattr(
        db_mod,
        "connection_factory",
        _failing_factory(
            real_factory,
            lambda sql: sql.startswith("UPDATE sample_checks SET status='INVALIDATED'"),
        ),
    )
    r = c.post("/events", json=_ev("V8", "E", 2, "2026-11-10T12:00:00Z", "RUNNING"))
    assert r.status_code == 500
    monkeypatch.setattr(db_mod, "connection_factory", real_factory)

    # 修订 2 未落库，核验仍为 CURRENT 且版号未变
    assert [(e["revision"], e["status"]) for e in c.get("/devices/V8/events").json()] == [
        (1, "STOPPED")
    ]
    after = c.get("/devices/V8/verifications/2026-11-10").json()
    assert after["status"] == "CURRENT"
    assert after["event_version"] == 1
    assert after["generated_at"] == before["generated_at"]
    assert after["samples"][0]["event_status"] == "STOPPED"


# --------------------------------------------------------------------------- #
# 11. 重算失败：旧的 INVALIDATED 行保持原样，不发布半份核验
# --------------------------------------------------------------------------- #
def test_recompute_failure_keeps_invalidated_row(client, monkeypatch):
    c, _, db_mod = client
    c.post("/events", json=_ev("V9", "E", 1, "2026-11-10T01:00:00Z", "RUNNING"))
    c.post("/samples", json=_smp("V9", "A", "2026-11-10T03:00:00Z", "RUNNING"))
    assert c.get("/devices/V9/verifications/2026-11-10").json()["status"] == "CURRENT"
    c.post("/samples", json=_smp("V9", "B", "2026-11-10T05:00:00Z", "STOPPED"))
    assert c.get("/devices/V9/verifications/2026-11-10").json()["status"] == "INVALIDATED"

    real_factory = db_mod.connection_factory
    monkeypatch.setattr(
        db_mod,
        "connection_factory",
        _failing_factory(
            real_factory, lambda sql: sql.startswith("INSERT INTO sample_checks")
        ),
    )
    r = c.get("/devices/V9/verifications/2026-11-10?recompute=true")
    assert r.status_code == 500
    monkeypatch.setattr(db_mod, "connection_factory", real_factory)

    mid = c.get("/devices/V9/verifications/2026-11-10").json()
    assert mid["status"] == "INVALIDATED" and mid["verified"] is False

    final = c.get("/devices/V9/verifications/2026-11-10?recompute=true").json()
    assert final["status"] == "CURRENT" and final["sample_count"] == 2
    assert final["sample_version"] == 2


# --------------------------------------------------------------------------- #
# 12. 两路写入与重核交错：最终核验版本一致、无重复采样、无半份核验
# --------------------------------------------------------------------------- #
def test_concurrent_samples_and_recompute(client):
    c, svc, _ = client
    c.post("/events", json=_ev("VC", "E", 1, "2026-11-10T00:30:00Z", "RUNNING"))
    c.get("/devices/VC/verifications/2026-11-10")

    errors = []

    def add_sample(i: int):
        r = c.post(
            "/samples",
            json=_smp(
                "VC",
                f"S{i:02d}",
                f"2026-11-10T{i % 20 + 1:02d}:{i % 60:02d}:00Z",
                "RUNNING" if i % 2 == 0 else "STOPPED",
            ),
        )
        if r.status_code not in (201, 409):
            errors.append(("sample", i, r.status_code))

    def recheck(_: int):
        r = c.get("/devices/VC/verifications/2026-11-10?recompute=true")
        if r.status_code != 200:
            errors.append(("recheck", r.status_code))
            return
        env = r.json()
        # 任何时刻读到的核验都必须是完整的一份：两路版号一致且逐条齐全
        if env["status"] == "CURRENT" and env["sample_count"] != len(env["samples"]):
            errors.append(("half", env))

    with ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(add_sample, range(40)))
        list(pool.map(recheck, range(40)))

    assert errors == []
    rows = c.get("/devices/VC/samples").json()
    assert len(rows) == 40
    assert len({r["sample_id"] for r in rows}) == 40

    final = c.get("/devices/VC/verifications/2026-11-10?recompute=true").json()
    assert final["status"] == "CURRENT"
    assert final["sample_count"] == 40
    assert final["sample_version"] == 40
    assert final["event_version"] == 1
    assert final["conflict_count"] + final["unknown_count"] + sum(
        1 for s in final["samples"] if s["outcome"] == "MATCH"
    ) == 40

    listing = c.get("/verifications?device_id=VC").json()
    assert len(listing) == 1 and listing[0]["status"] == "CURRENT"


# --------------------------------------------------------------------------- #
# 13. Swagger / OpenAPI 与既有日报接口
# --------------------------------------------------------------------------- #
def test_openapi_exposes_sample_endpoints_and_reports_kept(client):
    c, _, _ = client
    spec = c.get("/openapi.json").json()
    paths = spec["paths"]
    for path in (
        "/samples",
        "/samples/batch",
        "/devices/{device_id}/samples",
        "/devices/{device_id}/verifications/{check_date}",
        "/verifications",
    ):
        assert path in paths
    for path in (
        "/events",
        "/events/batch",
        "/devices/{device_id}/events",
        "/devices/{device_id}/reports/{report_date}",
        "/reports",
        "/health",
    ):
        assert path in paths
    assert spec["info"]["version"] == "1.1.0"
    assert "SampleIn" in spec["components"]["schemas"]
    assert "VerificationEnvelope" in spec["components"]["schemas"]
    assert c.get("/docs").status_code == 200

    # 旧日报接口与新采样/核验接口可同时工作
    c.post("/events", json=_ev("VW", "E", 1, "2026-11-10T10:00:00Z", "STOPPED"))
    assert c.get("/devices/VW/reports/2026-11-10").json()["status"] == "CURRENT"
    c.post("/samples", json=_smp("VW", "A", "2026-11-10T11:00:00Z", "STOPPED"))
    assert c.get("/devices/VW/verifications/2026-11-10").json()["verified"] is True
