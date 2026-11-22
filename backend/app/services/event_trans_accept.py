"""转换字段接纳服务：记录每个位点的事件类型、规则版本与列版本核对结果。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services import pipeline


class EventTransAcceptService(BaseModuleService):
    MODULE = "event_trans_accept"
    ENTITY = "接纳结果"
    OPERATION_TERM = "接纳"
    LIST_FIELDS = [
        "接纳编号", "数据流", "业务键", "源端位点", "事件类型",
        "规则版本", "列版本", "接纳结论", "待核原因",
    ]
    REQUIRED_FIELDS = ["数据流", "业务键", "源端位点", "事件类型"]
    STATUS_ORDER = ["已通过", "暂缓", "已转换"]
    ACTION_RULES = {"重试接纳": "已通过"}
    NEGATIVE_ACTIONS = []

    def retry(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        result = pipeline.retry_accept(entry_id)
        entry = self.get_entry(entry_id)
        return entry, str(result.get("message", "重试完成"))

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "重试接纳":
            return self.retry(entry_id)
        return super().run_action(entry_id, action)
