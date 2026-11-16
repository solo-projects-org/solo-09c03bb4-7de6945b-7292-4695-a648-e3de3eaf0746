"""Pydantic 请求/响应模型（Swagger 文档即由这些模型生成）。"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class EventStatus(str, Enum):
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"


class EventIn(BaseModel):
    device_id: str = Field(..., min_length=1, description="设备号")
    event_id: str = Field(..., min_length=1, description="事件号")
    revision: int = Field(..., ge=1, description="修订号，越大越新")
    event_time: datetime = Field(..., description="UTC 切换时刻（必须带时区的 ISO-8601，建议以 Z 结尾）")
    status: EventStatus = Field(..., description="本次切换后的运行状态")

    @field_validator("event_time")
    @classmethod
    def _require_timezone(cls, v: datetime) -> datetime:
        if v.tzinfo is None or v.utcoffset() is None:
            raise ValueError("event_time 必须带时区信息（UTC，例如 2026-11-10T10:00:00Z）")
        return v


class EventRecord(EventIn):
    received_at: datetime


class EventAck(BaseModel):
    accepted: bool
    idempotent: bool = Field(..., description="同版同文重复提交时为 true")
    device_id: str
    event_id: str
    revision: int
    data_version: int = Field(..., description="提交后该设备的数据版号")


class BatchEventResult(BaseModel):
    accepted_count: int
    idempotent_count: int
    data_version: int


# --------------------------------------------------------------------------- #
# 独立状态采样
# --------------------------------------------------------------------------- #
class SampleIn(BaseModel):
    device_id: str = Field(..., min_length=1, description="设备号")
    sample_id: str = Field(..., min_length=1, description="采样号（同一设备内唯一）")
    sample_time: datetime = Field(
        ..., description="采样时刻（必须带时区的 ISO-8601，内部统一转 UTC）"
    )
    status: EventStatus = Field(..., description="采样到的运行状态")

    @field_validator("sample_time")
    @classmethod
    def _require_timezone(cls, v: datetime) -> datetime:
        if v.tzinfo is None or v.utcoffset() is None:
            raise ValueError("sample_time 必须带时区信息（UTC，例如 2026-11-10T10:00:00Z）")
        return v


class SampleRecord(BaseModel):
    device_id: str
    sample_id: str
    sample_time: datetime
    sample_date: str
    status: EventStatus
    received_at: datetime


class SampleAck(BaseModel):
    accepted: bool
    idempotent: bool = Field(..., description="同号同文重复提交时为 true")
    device_id: str
    sample_id: str
    sample_version: int = Field(..., description="提交后该设备的采样数据版号")


class BatchSampleResult(BaseModel):
    accepted_count: int
    idempotent_count: int
    sample_version: int


# --------------------------------------------------------------------------- #
# 采样核验
# --------------------------------------------------------------------------- #
class SampleConflict(BaseModel):
    sample_id: str
    sample_time: datetime
    sample_status: Literal["RUNNING", "STOPPED"]
    event_status: Literal["RUNNING", "STOPPED"]
    basis_event_id: str = Field(..., description="判定所依据的生效切换事件号")
    basis_event_time: datetime


class SampleCheckResult(BaseModel):
    sample_id: str
    sample_time: datetime
    sample_status: Literal["RUNNING", "STOPPED"]
    event_status: Literal["RUNNING", "STOPPED", "UNKNOWN"]
    basis_event_id: str | None = Field(None, description="首次切换前无依据事件，为 null")
    basis_event_time: datetime | None = None
    outcome: Literal["MATCH", "CONFLICT", "UNKNOWN"]


class VerificationEnvelope(BaseModel):
    device_id: str
    check_date: str = Field(..., description="UTC 日期 YYYY-MM-DD")
    status: Literal["CURRENT", "INVALIDATED", "NO_SAMPLES"] = Field(
        ...,
        description=(
            "CURRENT=已发布核验；INVALIDATED=依据的采样或事件已变更，需重算；"
            "NO_SAMPLES=该日期无采样，不发布核验（verified=false）"
        ),
    )
    verified: bool = Field(..., description="仅 CURRENT 且有采样时为 true")
    event_version: int = Field(..., description="核验所依据的事件数据版号")
    sample_version: int = Field(..., description="核验所依据的采样数据版号")
    sample_count: int
    conflict_count: int
    unknown_count: int
    conflicts: list[SampleConflict] = Field(
        default_factory=list, description="采样状态与生效事件状态冲突的明细"
    )
    samples: list[SampleCheckResult] = Field(
        default_factory=list, description="逐条采样对照结果（按采样时刻排序）"
    )
    generated_at: datetime | None = None
    invalidated_at: datetime | None = None
    invalidated_reason: str | None = None


class IntervalOut(BaseModel):
    start: datetime
    end: datetime
    duration_minutes: int
    status: Literal["RUNNING", "STOPPED", "UNKNOWN"]
    source_event_id: str | None = Field(None, description="该状态由哪个事件切换而来")


class ShiftOut(BaseModel):
    shift: Literal["night", "day", "evening"]
    shift_start: datetime
    shift_end: datetime
    downtime_minutes: int
    unknown_minutes: int
    running_minutes: int
    intervals: list[IntervalOut]


class ReportContent(BaseModel):
    device_id: str
    report_date: str
    data_version: int
    shifts: list[ShiftOut]
    total_downtime_minutes: int
    total_unknown_minutes: int
    used_event_ids: list[str]


class ReportEnvelope(BaseModel):
    device_id: str
    report_date: str
    status: Literal["CURRENT", "INVALIDATED"]
    data_version: int
    generated_at: datetime
    invalidated_at: datetime | None = None
    invalidated_reason: str | None = None
    report: ReportContent | None = Field(None, description="CURRENT 时为重算结果；INVALIDATED 时为 null")
