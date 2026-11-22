"""Binlog解析服务：启动解析后会生产事件并推动下游event_trans转换管道。"""
from __future__ import annotations

from typing import Any

from app.services import pipeline
from app.services.base import BaseModuleService
from app.store import store


class BinlogParseService(BaseModuleService):
    MODULE = "binlog_parse"
    ENTITY = "解析规则"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["规则编号", "源端", "日志格式", "事件过滤", "字段映射", "DDL处理", "大事务拆分", "解析速率"]
    REQUIRED_FIELDS = ["规则编号", "源端", "日志格式"]
    STATUS_ORDER = ["草稿", "解析中", "速率下降", "解析中断"]
    ACTION_RULES = {"启动解析": "解析中", "调整速率": "速率下降", "标记中断": "解析中断", "发布规则": "解析中"}
    NEGATIVE_ACTIONS = []

    def publish_rule(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """发布解析规则：事件过滤与字段映射属于同一规则版本，整体替换下游条件。

        数据流链路: binlog_parse(规则版本) -> parse_version -> event_trans
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"解析规则 {entry_id} 不存在或已归档"
        source = str(entry.get("源端", ""))
        dataflow = pipeline.dataflow_for_source(source)
        # 新版本只覆盖尚未接入的位点，已绑定旧版本的事件继续按旧版本判定。
        known = [
            int(pipeline.parse_offset(row.get("源端位点", "0")) or 0)
            for row in store.rows("event_ingest")
            if str(row.get("数据流", "")) == dataflow
        ]
        range_start = max(pipeline.RESUME_OFFSET.get(dataflow, 0), (max(known) + 1) if known else 0)
        field_mapping = str(entry.get("字段映射", ""))
        version = pipeline.publish_version(
            rule_number=str(entry.get("规则编号", "")),
            source=source,
            dataflow=dataflow,
            event_filter=str(entry.get("事件过滤", "")),
            field_mapping=field_mapping,
            range_start=range_start,
        )
        entry["status"] = "解析中"
        entry["pending"] = True
        entry["abnormal"] = version.get("status") == "待补齐"
        entry["当前版本"] = str(version.get("规则版本", ""))
        trans = pipeline.sync_rule_and_pipeline(version)
        message = (
            f"解析规则 {entry.get('规则编号')} 已发布为 {version.get('规则版本')}"
            f"（{version.get('事件过滤')}）；事件转换管道"
            f"{trans.get('管道编号') if trans else '未创建'} 已同步，状态 {version.get('status')}"
        )
        return entry, message


    def trigger_downstream_event_trans(self, entry_id: int) -> dict[str, Any] | None:
        """binlog解析变为「解析中」后，级联创建 event_trans 转换管道。

        数据流链路: binlog_parse -> event_trans
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "解析中":
            return None

        trans_rows = store.rows("event_trans")
        existing = [r for r in trans_rows if str(r.get("源端", "")) == str(entry.get("源端", ""))]
        if existing:
            existing[0]["status"] = "转换中"
            existing[0]["pending"] = True
            existing[0]["abnormal"] = False
            existing[0]["最近吞吐"] = "12.5k eps"
            return existing[0]

        new_id = max((int(r.get("id", 0)) for r in trans_rows), default=0) + 1
        downstream = {
            "id": new_id,
            "管道编号": f"EVEN-{new_id:04d}",
            "源端": str(entry.get("源端", "")),
            "事件类型": str(entry.get("事件过滤", "INSERT,UPDATE,DELETE")),
            "转换函数": "flatten_nested_json, cast_timestamp, normalize_pk",
            "输出格式": "Avro",
            "失败策略": "写入死信队列",
            "转换顺序": "先扁平后类型转换",
            "最近吞吐": "12.5k eps",
            "status": "转换中",
            "pending": True,
            "abnormal": False,
        }
        trans_rows.append(downstream)
        return downstream

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "发布规则":
            return self.publish_rule(entry_id)
        entry, message = super().run_action(entry_id, action)
        if entry is not None and action == "启动解析":
            self.trigger_downstream_event_trans(entry_id)
        return entry, message

    def propagate_rate_adjustment(self) -> dict[str, Any]:
        """解析速率下降时，将上游速率事件传播到下游。

        数据流链路: binlog_parse(速率下降) -> event_trans(过载) -> lag_monitor(延迟上升)
        """
        rows = store.rows(self.MODULE)
        slow_rows = [r for r in rows if r.get("status") == "速率下降"]
        result = {"affected_downstream": 0}

        for slow in slow_rows:
            tag = str(slow.get("源端", ""))
            # 传播到事件转换
            for tr in store.rows("event_trans"):
                if str(tr.get("源端", "")) == tag and tr.get("status") == "转换中":
                    tr["status"] = "转换过载"
                    tr["pending"] = True
                    tr["abnormal"] = True
                    result["affected_downstream"] += 1
            # 传播到延迟监控
            for lm in store.rows("lag_monitor"):
                if str(lm.get("数据流", "")) == tag and lm.get("status") == "正常":
                    lm["status"] = "延迟偏高"
                    lm["pending"] = True
                    lm["abnormal"] = True
                    result["affected_downstream"] += 1

        return result