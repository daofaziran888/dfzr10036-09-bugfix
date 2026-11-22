"""转换输出服务：维护发布顺序，确保前序位点未放行时不进入关联与去重。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services import pipeline


class EventOutputService(BaseModuleService):
    MODULE = "event_output"
    ENTITY = "转换输出"
    OPERATION_TERM = "发布"
    LIST_FIELDS = [
        "输出编号", "数据流", "业务键", "源端位点", "事件类型",
        "规则版本", "列版本", "原日志位置", "消费阻塞原因", "输出说明",
    ]
    REQUIRED_FIELDS = ["数据流", "源端位点", "事件类型"]
    STATUS_ORDER = ["待放行", "待消费", "已消费", "已回滚"]
    ACTION_RULES = {"放行输出": "待消费", "标记回滚": "已回滚"}
    NEGATIVE_ACTIONS = ["标记回滚"]

    def release(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry, result = pipeline.release_output(entry_id)
        return entry, str(result.get("message", "放行完成"))

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "放行输出":
            return self.release(entry_id)
        return super().run_action(entry_id, action)
