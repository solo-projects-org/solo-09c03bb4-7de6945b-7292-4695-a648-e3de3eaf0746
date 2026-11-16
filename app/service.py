"""核心业务逻辑：事件修订裁决、日报计算、失效与重算。

时间一律使用 UTC。班制：夜班 00:00–08:00、白班 08:00–16:00、晚班 16:00–24:00。
"""
from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone
from typing import Any

from . import db

UTC = timezone.utc
SHIFT_BOUNDS = (0, 8, 16, 24)
SHIFT_NAMES = ("night", "day", "evening")


class ConflictError(Exception):
    """修订号回退或同版异文，映射为 HTTP 409。"""


# --------------------------------------------------------------------------- #
# 时间辅助
# --------------------------------------------------------------------------- #
def to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        # 无显式时区的输入按 UTC 解释（生产入口由 Pydantic 强制要求带时区）
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def iso(dt: datetime) -> str:
    """统一以 UTC ISO-8601（Z 结尾）落库。"""
    return to_utc(dt).isoformat().replace("+00:00", "Z")


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(UTC)


# --------------------------------------------------------------------------- #
# 事件接收（单条 / 批量，全部在一个写事务内）
# --------------------------------------------------------------------------- #
def _get_existing_revision(
    conn, device_id: str, event_id: str, revision: int
) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT event_time, status FROM event_revisions "
        "WHERE device_id=? AND event_id=? AND revision=?",
        (device_id, event_id, revision),
    ).fetchone()
    return dict(row) if row else None


def _ingest_one(conn, payload: dict[str, Any]) -> bool:
    """在已开启的事务中写入一条事件修订。

    返回 True 表示新接受，False 表示同版同文幂等。
    低修订回退 / 同版异文抛 ConflictError，事务由外层回滚。
    """
    device_id = payload["device_id"]
    event_id = payload["event_id"]
    revision = payload["revision"]
    event_time = to_utc(payload["event_time"])
    status = payload["status"].value if hasattr(payload["status"], "value") else payload["status"]

    max_row = conn.execute(
        "SELECT MAX(revision) AS max_revision FROM event_revisions "
        "WHERE device_id=? AND event_id=?",
        (device_id, event_id),
    ).fetchone()
    max_revision = max_row["max_revision"]
    if max_revision is not None and revision < max_revision:
        raise ConflictError(
            f"事件 {event_id} 修订号 {revision} 低于当前最高修订 {max_revision}，拒绝回退"
        )

    existing = _get_existing_revision(conn, device_id, event_id, revision)
    if existing is not None:
        if existing["event_time"] == iso(event_time) and existing["status"] == status:
            return False  # 同版同文：幂等，不重复计数、不改变状态
        raise ConflictError(
            f"事件 {event_id} 修订号 {revision} 已存在但内容不同（同版异文），拒绝"
        )

    # 取更正前该事件的最早/当前时刻，用于圈定受影响的日报日期
    old_dates = [
        r["event_date"]
        for r in conn.execute(
            "SELECT DISTINCT substr(event_time, 1, 10) AS event_date "
            "FROM event_revisions WHERE device_id=? AND event_id=?",
            (device_id, event_id),
        ).fetchall()
    ]

    conn.execute(
        "INSERT INTO event_revisions "
        "(device_id, event_id, revision, event_time, status, received_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (device_id, event_id, revision, iso(event_time), status, iso(datetime.now(UTC))),
    )

    new_d = event_time.date().isoformat()
    affected = sorted(set(old_dates) | {new_d})
    if affected:
        start_d, end_d = affected[0], affected[-1]
        conn.execute(
            "UPDATE reports SET status='INVALIDATED', "
            "invalidated_at=?, invalidated_reason=? "
            "WHERE device_id=? AND status='CURRENT' "
            "AND report_date BETWEEN ? AND ?",
            (
                iso(datetime.now(UTC)),
                f"事件 {event_id} 修订 {revision} 更新/更正",
                device_id,
                start_d,
                end_d,
            ),
        )

    # 设备数据版号 +1（幂等重复提交不增版）
    conn.execute(
        "INSERT INTO device_versions(device_id, current_version) VALUES(?, 1) "
        "ON CONFLICT(device_id) DO UPDATE SET current_version = current_version + 1",
        (device_id,),
    )
    return True


def submit_event(payload: dict[str, Any]) -> dict[str, Any]:
    conn = db.get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        is_new = _ingest_one(conn, payload)
        version = conn.execute(
            "SELECT current_version FROM device_versions WHERE device_id=?",
            (payload["device_id"],),
        ).fetchone()["current_version"]
        conn.commit()
        return {
            "accepted": True,
            "idempotent": not is_new,
            "device_id": payload["device_id"],
            "event_id": payload["event_id"],
            "revision": payload["revision"],
            "data_version": version,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def submit_events_batch(payloads: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """批量提交：整批一个事务，任一冲突全部回滚，杜绝部分生效。"""
    if not payloads:
        raise ConflictError("批量提交不能为空")
    devices = {p["device_id"] for p in payloads}
    if len(devices) > 1:
        raise ConflictError("批量提交仅限同一设备的事件，以保证单设备版号一致")
    conn = db.get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        accepted = 0
        idempotent = 0
        for p in payloads:
            if _ingest_one(conn, p):
                accepted += 1
            else:
                idempotent += 1
        device_id = payloads[0]["device_id"]
        version = conn.execute(
            "SELECT current_version FROM device_versions WHERE device_id=?",
            (device_id,),
        ).fetchone()["current_version"]
        conn.commit()
        return {
            "accepted_count": accepted,
            "idempotent_count": idempotent,
            "data_version": version,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# 事件查询
# --------------------------------------------------------------------------- #
def list_events(device_id: str) -> list[dict[str, Any]]:
    conn = db.get_connection()
    try:
        rows = conn.execute(
            "SELECT device_id, event_id, revision, event_time, status, received_at "
            "FROM event_revisions WHERE device_id=? "
            "ORDER BY event_time, event_id, revision",
            (device_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def latest_event_rows(conn, device_id: str) -> list[Any]:
    return conn.execute(
        "SELECT event_id, revision, event_time, status FROM latest_events "
        "WHERE device_id=? ORDER BY event_time, event_id",
        (device_id,),
    ).fetchall()


# --------------------------------------------------------------------------- #
# 日报计算（纯函数，便于单测）
# --------------------------------------------------------------------------- #
def _day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return start, start + timedelta(days=1)


def build_report_content(
    device_id: str, report_date: str, latest: Sequence[Any], data_version: int
) -> dict[str, Any]:
    """依据每个事件的当前生效修订计算某设备某日的三班停机报告。

    latest: 每行含 event_id/revision/event_time/status，按 (event_time, event_id) 排序。
    """
    day = date.fromisoformat(report_date)
    day_start, day_end = _day_bounds(day)

    # 班初之前的最后状态；没有则为 UNKNOWN（不充作运行）
    prior = [r for r in latest if parse_iso(r["event_time"]) <= day_start]
    current_status: str | None = prior[-1]["status"] if prior else None
    current_source: str | None = prior[-1]["event_id"] if prior else None

    # 日内切换（含恰好 00:00:00 的切换；跨日事件已在 latest 全集中排序）
    day_events = [
        r
        for r in latest
        if day_start <= parse_iso(r["event_time"]) < day_end
    ]

    # 构造整日状态片段
    segments: list[dict[str, Any]] = []
    seg_start = day_start
    if current_status is not None:
        segments.append(
            {"start": seg_start, "end": None, "status": current_status,
             "source_event_id": current_source}
        )
    for ev in day_events:
        ev_time = parse_iso(ev["event_time"])
        if segments and segments[-1]["end"] is None:
            segments[-1]["end"] = ev_time
        elif ev_time > seg_start:
            # 班初无此前状态：未知片段
            segments.append(
                {"start": seg_start, "end": ev_time, "status": "UNKNOWN",
                 "source_event_id": None}
            )
        segments.append(
            {"start": ev_time, "end": None, "status": ev["status"],
             "source_event_id": ev["event_id"]}
        )
        current_status = ev["status"]
        current_source = ev["event_id"]
        seg_start = ev_time
    if segments and segments[-1]["end"] is None:
        segments[-1]["end"] = day_end  # 班末沿用最后状态
    elif not segments:
        # 全天没有任何已知状态
        segments.append(
            {"start": day_start, "end": day_end, "status": "UNKNOWN",
             "source_event_id": None}
        )

    def split_by_shifts(seg: dict[str, Any]) -> list[dict[str, Any]]:
        out = []
        for idx, (h0, h1) in enumerate(zip(SHIFT_BOUNDS[:-1], SHIFT_BOUNDS[1:])):
            s0 = day_start + timedelta(hours=h0)
            s1 = day_start + timedelta(hours=h1)
            lo = max(seg["start"], s0)
            hi = min(seg["end"], s1)
            if lo < hi:
                out.append(
                    {
                        "shift_index": idx,
                        "start": lo,
                        "end": hi,
                        "status": seg["status"],
                        "source_event_id": seg["source_event_id"],
                    }
                )
        return out

    shifts: list[dict[str, Any]] = []
    used_event_ids: set[str] = set()
    total_down = total_unknown = 0

    for idx, name in enumerate(SHIFT_NAMES):
        s0 = day_start + timedelta(hours=SHIFT_BOUNDS[idx])
        s1 = day_start + timedelta(hours=SHIFT_BOUNDS[idx + 1])
        intervals: list[dict[str, Any]] = []
        down_m = unknown_m = running_m = 0
        for seg in segments:
            for piece in split_by_shifts(seg):
                if piece["shift_index"] != idx:
                    continue
                minutes = int((piece["end"] - piece["start"]).total_seconds() // 60)
                interval = {
                    "start": iso(piece["start"]),
                    "end": iso(piece["end"]),
                    "duration_minutes": minutes,
                    "status": piece["status"],
                    "source_event_id": piece["source_event_id"],
                }
                intervals.append(interval)
                if piece["status"] == "STOPPED":
                    down_m += minutes
                elif piece["status"] == "RUNNING":
                    running_m += minutes
                else:
                    unknown_m += minutes
                if piece["source_event_id"]:
                    used_event_ids.add(piece["source_event_id"])
        total_down += down_m
        total_unknown += unknown_m
        shifts.append(
            {
                "shift": name,
                "shift_start": iso(s0),
                "shift_end": iso(s1),
                "downtime_minutes": down_m,
                "unknown_minutes": unknown_m,
                "running_minutes": running_m,
                "intervals": intervals,
            }
        )

    return {
        "device_id": device_id,
        "report_date": report_date,
        "data_version": data_version,
        "shifts": shifts,
        "total_downtime_minutes": total_down,
        "total_unknown_minutes": total_unknown,
        "used_event_ids": sorted(used_event_ids),
    }


# --------------------------------------------------------------------------- #
# 日报查询 / 重算
# --------------------------------------------------------------------------- #
def _current_version(conn, device_id: str) -> int:
    row = conn.execute(
        "SELECT current_version FROM device_versions WHERE device_id=?",
        (device_id,),
    ).fetchone()
    return row["current_version"] if row else 0


def get_or_create_report(device_id: str, report_date: str) -> dict[str, Any]:
    """返回日报信封。CURRENT 直接给缓存；缺失则重算落库；INVALIDATED 不自动重算。

    并发首次生成时，多个请求可能在任一提交前都读到“无行”。它们在
    ``BEGIN IMMEDIATE`` 处排队拿写锁；拿到锁后在事务内**复查**：
    若已存在同版 CURRENT 行则直接复用，否则才重算，避免冗余重算与
    排队期间事件更新造成的版本错配，绝不暴露半份日报。
    """
    date.fromisoformat(report_date)  # 校验日期格式
    conn = db.get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM reports WHERE device_id=? AND report_date=?",
            (device_id, report_date),
        ).fetchone()
        if row is not None:
            return _envelope(row)

        conn.execute("BEGIN IMMEDIATE")
        try:
            # 拿锁后复查：并发对手可能已提交，或期间又有事件更正
            row = conn.execute(
                "SELECT * FROM reports WHERE device_id=? AND report_date=?",
                (device_id, report_date),
            ).fetchone()
            version = _current_version(conn, device_id)
            if row is not None and row["status"] == "CURRENT" and row["data_version"] == version:
                conn.rollback()  # 无事可写，仅释放写锁
                return _envelope(row)
            envelope = _recompute_and_store(conn, device_id, report_date)
            conn.commit()
            return envelope
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()


def recompute_report(device_id: str, report_date: str) -> dict[str, Any]:
    """显式重算：在一个写事务内替换旧行，任何失败回滚，不暴露半份日报。"""
    date.fromisoformat(report_date)
    conn = db.get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        return _recompute_and_store(conn, device_id, report_date, do_commit=True)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _recompute_and_store(conn, device_id: str, report_date: str, do_commit: bool = False):
    version = _current_version(conn, device_id)
    latest = latest_event_rows(conn, device_id)
    content = build_report_content(device_id, report_date, latest, version)
    now = iso(datetime.now(UTC))
    # 同主键覆盖：旧的 INVALIDATED 行被新的 CURRENT 行整体替换，杜绝混合版本
    conn.execute(
        "INSERT INTO reports(device_id, report_date, data_version, status, content, "
        "generated_at, invalidated_at, invalidated_reason) "
        "VALUES(?, ?, ?, 'CURRENT', ?, ?, NULL, NULL) "
        "ON CONFLICT(device_id, report_date) DO UPDATE SET "
        "data_version=excluded.data_version, status='CURRENT', content=excluded.content, "
        "generated_at=excluded.generated_at, invalidated_at=NULL, invalidated_reason=NULL",
        (device_id, report_date, version, json.dumps(content, ensure_ascii=False), now),
    )
    if do_commit:
        conn.commit()
    row = conn.execute(
        "SELECT * FROM reports WHERE device_id=? AND report_date=?",
        (device_id, report_date),
    ).fetchone()
    return _envelope(row)


def _envelope(row) -> dict[str, Any]:
    if row["status"] == "CURRENT":
        report = json.loads(row["content"])
    else:
        report = None
    return {
        "device_id": row["device_id"],
        "report_date": row["report_date"],
        "status": row["status"],
        "data_version": row["data_version"],
        "generated_at": row["generated_at"],
        "invalidated_at": row["invalidated_at"],
        "invalidated_reason": row["invalidated_reason"],
        "report": report,
    }


def list_reports(device_id: str | None = None) -> list[dict[str, Any]]:
    conn = db.get_connection()
    try:
        if device_id:
            rows = conn.execute(
                "SELECT * FROM reports WHERE device_id=? ORDER BY report_date",
                (device_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM reports ORDER BY device_id, report_date"
            ).fetchall()
        return [_envelope(r) for r in rows]
    finally:
        conn.close()
