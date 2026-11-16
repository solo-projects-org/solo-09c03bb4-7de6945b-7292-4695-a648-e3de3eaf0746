"""FastAPI 入口：网关状态切换事件接收与 UTC 三班停机日报查询。

启动后访问 Swagger：http://localhost:8000/docs
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse

from . import service
from .db import init_db
from .models import (
    BatchEventResult,
    EventAck,
    EventIn,
    EventRecord,
    ReportEnvelope,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="设备网关停机日报服务",
    version="1.0.0",
    description=(
        "接收设备网关运行/停机切换事件（含修订号裁决），按 UTC 00:00/08:00/16:00 "
        "三班生成停机区间日报；更正旧事件时受影响日报自动失效，重算后恢复。"
    ),
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")


@app.get("/health", tags=["系统"])
def health():
    return {"status": "ok"}


@app.post("/events", response_model=EventAck, tags=["事件"], status_code=201)
def submit_event(event: EventIn):
    """提交一条设备状态切换事件。

    - 高修订可更改同一事件的时刻与状态；
    - 低修订回退、同版异文返回 409；
    - 同版同文重复提交幂等（idempotent=true）。
    """
    try:
        return service.submit_event(event.model_dump())
    except service.ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/events/batch", response_model=BatchEventResult, tags=["事件"], status_code=201)
def submit_events_batch(events: list[EventIn]):
    """批量提交：整批同一事务，任一冲突全部回滚。"""
    try:
        return service.submit_events_batch([e.model_dump() for e in events])
    except service.ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/devices/{device_id}/events", response_model=list[EventRecord], tags=["事件"])
def list_device_events(device_id: str):
    """查看某设备全部事件修订流水。"""
    return service.list_events(device_id)


@app.get(
    "/devices/{device_id}/reports/{report_date}",
    response_model=ReportEnvelope,
    tags=["日报"],
)
def get_report(
    device_id: str,
    report_date: date,
    recompute: bool = Query(
        False, description="true 时强制按当前数据版号重算（失效日报恢复为 CURRENT）"
    ),
):
    """按设备 + UTC 日期获取三班停机日报。

    状态 INVALIDATED 表示该日报依据的事件已被更正，需要重算；
    重算后 status 恢复为 CURRENT，data_version 更新为最新版号。
    """
    day = report_date.isoformat()
    try:
        if recompute:
            return service.recompute_report(device_id, day)
        return service.get_or_create_report(device_id, day)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/reports", response_model=list[ReportEnvelope], tags=["日报"])
def list_all_reports(
    device_id: str | None = Query(None, description="可选，按设备过滤"),
):
    """列出已生成日报（含 CURRENT / INVALIDATED 全部状态）。"""
    return service.list_reports(device_id)
