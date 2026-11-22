"""事件接入服务：按源日志位点绑定规则版本，核对字段后决定接纳或挂起。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services import pipeline


class EventIngestService(BaseModuleService):
    MODULE = "event_ingest"
    ENTITY = "接入事件"
    OPERATION_TERM = "接入"
    LIST_FIELDS = [
        "事件编号", "数据流", "业务键", "源端位点", "事件类型",
        "规则版本", "列版本", "字段核对", "原日志位置", "待核原因",
    ]
    REQUIRED_FIELDS = ["数据流", "业务键", "源端位点", "事件类型"]
    STATUS_ORDER = ["已接纳", "挂起待核", "已转换"]
    ACTION_RULES = {"重试挂起批次": "已接纳", "从位点续做": "已接纳"}
    NEGATIVE_ACTIONS = []

    def ingest(self, values: dict[str, Any]) -> dict[str, Any]:
        dataflow = str(values.get("数据流") or pipeline.dataflow_for_source(str(values.get("源端", ""))))
        offset = pipeline.parse_offset(values.get("源端位点")) or 0
        payload = values.get("payload") or {}
        return pipeline.ingest_event(
            dataflow=dataflow,
            business_key=str(values.get("业务键", "")),
            offset=offset,
            operation=str(values.get("事件类型", "")),
            payload=payload if isinstance(payload, dict) else {},
            raw_log=values.get("原始日志"),
        )

    def retry(self, entry_id: int) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        return pipeline.retry_ingest(entry_id)

    def resume(self, entry_id: int) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        return pipeline.resume_entry(entry_id)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "重试挂起批次":
            entry, result = self.retry(entry_id)
            return entry, str(result.get("message", "重试完成"))
        if action == "从位点续做":
            entry, result = self.resume(entry_id)
            return entry, str(result.get("message", "续做完成"))
        return super().run_action(entry_id, action)
