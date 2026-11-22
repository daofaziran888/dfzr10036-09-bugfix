"""批次消费服务：关联与去重按位点顺序消费转换输出，并保留原日志追溯。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services import pipeline


class BatchConsumeService(BaseModuleService):
    MODULE = "batch_consume"
    ENTITY = "消费记录"
    OPERATION_TERM = "消费"
    LIST_FIELDS = [
        "消费编号", "数据流", "消费阶段", "源端位点", "输出编号",
        "事件类型", "规则版本", "列版本", "消费结果", "追溯日志位置",
    ]
    REQUIRED_FIELDS = ["数据流", "消费阶段", "源端位点", "输出编号"]
    STATUS_ORDER = ["已消费", "待消费"]
    ACTION_RULES = {"补消费": "已消费", "反查原日志": "已消费"}
    NEGATIVE_ACTIONS = []

    def replay(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry, result = pipeline.replay_consume(entry_id)
        return entry, str(result.get("message", "补消费完成"))

    def trace(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = self.get_entry(entry_id)
        trace = pipeline.trace_consumption(entry_id)
        if entry is None or trace is None:
            return None, f"消费记录 {entry_id} 不存在"
        return entry, (
            f"消费记录 {entry.get('消费编号')} 可反查原日志 {trace['raw_log'] or trace['trace_location']}"
        )

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "补消费":
            return self.replay(entry_id)
        if action == "反查原日志":
            return self.trace(entry_id)
        return super().run_action(entry_id, action)
