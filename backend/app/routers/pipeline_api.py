"""交接流水线接口：规则发布、事件接入、输出放行与交接点推进。

这些固定路径必须先于 /{module}/{entry_id} 动态路由注册，否则会被当成模块名。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services import pipeline

router = APIRouter(prefix="/api", tags=["交接流水线"])


class IngestPayload(BaseModel):
    dataflow: str | None = None
    source: str | None = None
    business_key: str
    offset: int
    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    raw_log: str | None = None


class HandoverPayload(BaseModel):
    """从 CDC 编排入口送入一条数据流事件。"""

    cdc_entry_id: int | None = None
    dataflow: str | None = None
    business_key: str = "7"
    offset: int
    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)


@router.post("/flows/stream-handover")
def trigger_stream_handover(payload: HandoverPayload) -> dict[str, Any]:
    """编排入口：把事件送入指定数据流的交接流水线。"""
    dataflow = payload.dataflow
    if not dataflow and payload.cdc_entry_id is not None:
        from app.store import store
        entry = store.find("cdc_source", payload.cdc_entry_id)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"CDC源端 {payload.cdc_entry_id} 不存在")
        dataflow = pipeline.dataflow_for_source(str(entry.get("源端编号", "")))
    dataflow = dataflow or "order_events_cdc"
    result = pipeline.ingest_event(
        dataflow=dataflow,
        business_key=payload.business_key,
        offset=payload.offset,
        operation=payload.event_type,
        payload=payload.payload,
    )
    return {"dataflow": dataflow, **result}


@router.get("/flows/list")
def list_flows() -> dict[str, Any]:
    """列出所有可编排流链路（固定路径，需先于动态模块路由注册）。"""
    from app.services.orchestrator import FLOWS
    return {"flows": list(FLOWS.keys()), "count": len(FLOWS)}


@router.get("/stream-handover/{dataflow}")
def read_stream_handover(dataflow: str) -> dict[str, Any]:
    """事件转换工作台核对视图：版本、接纳、输出与消费进度。"""
    return pipeline.snapshot(dataflow)


@router.post("/parse-rules/{entry_id}/publish")
def publish_parse_rule(entry_id: int) -> dict[str, Any]:
    from app.services.binlog_parse import BinlogParseService
    entry, message = BinlogParseService().publish_rule(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=message)
    return {"ok": True, "message": message, "entry": entry}


@router.post("/parse-versions/{entry_id}/fill-columns")
def fill_version_columns(entry_id: int) -> dict[str, Any]:
    version, message = pipeline.fill_columns(entry_id)
    if version is None:
        raise HTTPException(status_code=404, detail=message)
    return {"ok": True, "message": message, "entry": version}


@router.post("/event-ingest/{entry_id}/retry")
def retry_ingest(entry_id: int) -> dict[str, Any]:
    entry, result = pipeline.retry_ingest(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="接入事件不存在")
    return result


@router.post("/event-ingest/{entry_id}/resume")
def resume_ingest(entry_id: int) -> dict[str, Any]:
    entry, result = pipeline.resume_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="接入事件不存在")
    return result


@router.post("/event-output/{entry_id}/release")
def release_output(entry_id: int) -> dict[str, Any]:
    entry, result = pipeline.release_output(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="转换输出不存在")
    return result


@router.post("/handover/{entry_id}/advance")
def advance_handover(entry_id: int) -> dict[str, Any]:
    entry, result = pipeline.advance_handover(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="交接点不存在")
    return result


@router.get("/batch_consume/{entry_id}/trace")
def trace_batch_consume(entry_id: int) -> dict[str, Any]:
    """由关联/去重消费记录反查转换输出、接入事件与原始日志。"""
    trace = pipeline.trace_consumption(entry_id)
    if trace is None:
        raise HTTPException(status_code=404, detail="消费记录不存在")
    return trace
