"""规则版本服务：把事件过滤与字段映射作为一个整体发布，并同步到下游转换管道。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services import pipeline


class ParseVersionService(BaseModuleService):
    MODULE = "parse_version"
    ENTITY = "规则版本"
    OPERATION_TERM = "发布"
    LIST_FIELDS = ["版本编号", "规则编号", "源端", "数据流", "规则版本", "事件过滤", "字段映射", "列映射", "列版本"]
    REQUIRED_FIELDS = ["规则编号", "源端", "事件过滤", "字段映射"]
    STATUS_ORDER = ["草稿", "已发布", "待补齐", "已归档"]
    ACTION_RULES = {"发布版本": "已发布", "补齐列定义": "已发布", "归档版本": "已归档"}
    NEGATIVE_ACTIONS = []

    def publish(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"规则版本 {entry_id} 不存在或已归档"
        source = str(entry.get("源端", ""))
        dataflow = str(entry.get("数据流") or pipeline.dataflow_for_source(source))
        offset = pipeline.parse_offset(entry.get("_range_start", 0)) or 0
        version = pipeline.publish_version(
            rule_number=str(entry.get("规则编号", "")),
            source=source,
            dataflow=dataflow,
            event_filter=str(entry.get("事件过滤", "")),
            field_mapping=str(entry.get("字段映射", "")),
            range_start=offset,
        )
        return version, (
            f"规则版本 {version.get('规则版本')} 已发布（{version.get('事件过滤')}），"
            f"事件转换已同步为同一版本，状态：{version.get('status')}"
        )

    def fill_columns(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        return pipeline.fill_columns(entry_id)

    def archive(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        return pipeline.archive_version(entry_id)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "发布版本":
            return self.publish(entry_id)
        if action == "补齐列定义":
            return self.fill_columns(entry_id)
        if action == "归档版本":
            return self.archive(entry_id)
        return super().run_action(entry_id, action)

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        by_dataflow: dict[str, dict[str, int]] = {}
        for row in self.list_entries(size=1_000_000)[0]:
            dataflow = str(row.get("数据流", ""))
            bucket = by_dataflow.setdefault(dataflow, {"versions": 0, "待补齐": 0})
            bucket["versions"] += 1
            if row.get("status") == "待补齐":
                bucket["待补齐"] += 1
        snapshot["by_dataflow"] = by_dataflow
        return snapshot
