"""交接点服务：记录新旧规则批次在同一源端的冲突、续做位点和放行进度。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services import pipeline


class HandoverPointService(BaseModuleService):
    MODULE = "handover_point"
    ENTITY = "交接点"
    OPERATION_TERM = "推进"
    LIST_FIELDS = ["交接编号", "数据流", "源端", "规则版本", "位点范围", "前序位点", "续做位点", "冲突说明"]
    REQUIRED_FIELDS = ["数据流", "源端"]
    STATUS_ORDER = ["冲突", "推进中", "已完成"]
    ACTION_RULES = {"推进交接": "推进中", "完成交接": "已完成"}
    NEGATIVE_ACTIONS = []

    def advance(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry, result = pipeline.advance_handover(entry_id)
        return entry, str(result.get("message", "推进完成"))

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action in ("推进交接", "完成交接"):
            return self.advance(entry_id)
        return super().run_action(entry_id, action)
