"""事件转换服务：转换完成后触发维表关联和Dedup去重。"""
from __future__ import annotations

from typing import Any

from app.services import pipeline
from app.services.base import BaseModuleService
from app.store import store


class EventTransService(BaseModuleService):
    MODULE = "event_trans"
    ENTITY = "转换管道"
    OPERATION_TERM = "转换"
    LIST_FIELDS = [
        "管道编号", "源端", "事件类型", "转换函数", "输出格式", "失败策略",
        "转换顺序", "列版本", "当前事件", "待核原因", "最近吞吐",
    ]
    REQUIRED_FIELDS = ["管道编号", "源端", "事件类型"]
    STATUS_ORDER = ["草稿", "转换中", "转换过载", "已停止"]
    ACTION_RULES = {
        "启动转换": "转换中",
        "扩容处理": "转换过载",
        "停止管道": "已停止",
        "核对接纳结果": "转换中",
        "重试挂起批次": "转换中",
        "放行输出": "转换中",
        "从位点续做": "转换中",
    }
    NEGATIVE_ACTIONS = []

    def acceptance_result(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """按转换字段接纳结果核对事件类型与列版本。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"转换管道 {entry_id} 不存在或已归档"
        dataflow = pipeline.dataflow_for_source(str(entry.get("源端", "")))
        view = pipeline.snapshot(dataflow)
        pending = view["pending_events"]
        if pending:
            lines = [
                f"{item.get('事件编号')} 位点{item.get('源端位点')} {item.get('事件类型')}"
                f"（{item.get('规则版本')}）：{item.get('待核原因')}"
                for item in pending
            ]
            entry["待核原因"] = "；".join(
                f"{item.get('源端位点')} {item.get('待核原因')}" for item in pending
            )
            entry["pending"] = True
            entry["abnormal"] = True
            return entry, f"待核原因：{' | '.join(lines)}"
        entry["待核原因"] = ""
        entry["abnormal"] = False
        return entry, (
            f"转换字段接纳结果：已接纳 {view['stage']['accepted']} 条，"
            f"列版本 {entry.get('列版本', '未绑定')}，无待核位点"
        )

    def retry_pending(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """重试该源端全部挂起批次；补齐列定义后仍只生成一份输出。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"转换管道 {entry_id} 不存在或已归档"
        dataflow = pipeline.dataflow_for_source(str(entry.get("源端", "")))
        hung = [
            row for row in store.rows("event_ingest")
            if str(row.get("数据流", "")) == dataflow and row.get("status") == "挂起待核"
        ]
        if not hung:
            return entry, "当前没有挂起批次，无需重试"
        messages: list[str] = []
        for row in sorted(hung, key=lambda item: int(pipeline.parse_offset(item.get("源端位点", "0")) or 0)):
            _, result = pipeline.retry_ingest(int(row["id"]))
            messages.append(f"位点{row.get('源端位点')}：{result.get('message', '')}")
        self.acceptance_result(entry_id)
        return entry, " | ".join(messages)

    def release_pending(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """推进交接点，按源日志位点顺序放行转换输出。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"转换管道 {entry_id} 不存在或已归档"
        dataflow = pipeline.dataflow_for_source(str(entry.get("源端", "")))
        handover = pipeline.find_handover(dataflow)
        if handover is None:
            return entry, "未找到交接点，无法放行"
        handover, result = pipeline.advance_handover(int(handover["id"]))
        return entry, str(result.get("message", "放行完成"))

    def resume_from_offset(self, entry_id: int, offset: int | None) -> tuple[dict[str, Any] | None, str]:
        """从可辨识的源日志位置续做；未指定时沿用交接点记录的续做位点。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"转换管道 {entry_id} 不存在或已归档"
        dataflow = pipeline.dataflow_for_source(str(entry.get("源端", "")))
        handover = pipeline.find_handover(dataflow)
        start = offset
        if start is None and handover is not None:
            start = pipeline.parse_offset(handover.get("续做位点"))
        if start is None:
            start = pipeline.RESUME_OFFSET.get(dataflow, 0)
        result = pipeline.resume_from(dataflow, int(start))
        return entry, str(result.get("message", "续做完成"))

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "核对接纳结果":
            return self.acceptance_result(entry_id)
        if action == "重试挂起批次":
            return self.retry_pending(entry_id)
        if action == "放行输出":
            return self.release_pending(entry_id)
        if action == "从位点续做":
            return self.resume_from_offset(entry_id, None)
        entry, message = super().run_action(entry_id, action)
        if entry is not None and action == "启动转换":
            self.trigger_dim_join_and_dedup(entry_id)
        return entry, message


    def trigger_dim_join_and_dedup(self, entry_id: int) -> dict[str, Any]:
        """事件转换管道启动后，级联创建维表关联和去重窗口。

        数据流链路: event_trans -> dim_join + dedup_stream
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "转换中":
            return None

        tag = str(entry.get("源端", ""))
        results: dict[str, Any] = {"dim_join": None, "dedup": None}

        # 创建/恢复 dim_join 维表关联
        dim_rows = store.rows("dim_join")
        has_dim = [r for r in dim_rows if str(r.get("事实表", "")) == tag]
        if has_dim:
            has_dim[0]["status"] = "关联中"
            has_dim[0]["pending"] = True
            has_dim[0]["abnormal"] = False
            has_dim[0]["命中率"] = "87.3%"
            results["dim_join"] = has_dim[0]
        else:
            new_dim_id = max((int(r.get("id", 0)) for r in dim_rows), default=0) + 1
            dim_config = {
                "id": new_dim_id,
                "配置编号": f"DIM_-{new_dim_id:04d}",
                "事实表": tag,
                "维度表": "dim_customer_profile",
                "关联键": "customer_id",
                "关联类型": "LEFT JOIN",
                "缺失策略": "填充默认值",
                "缓存TTL": "600s",
                "命中率": "87.3%",
                "status": "关联中",
                "pending": True,
                "abnormal": False,
            }
            dim_rows.append(dim_config)
            results["dim_join"] = dim_config

        # 创建/恢复 dedup_stream 去重窗口
        dedup_rows = store.rows("dedup_stream")
        has_dedup = [r for r in dedup_rows if str(r.get("数据流", "")) == tag]
        if has_dedup:
            has_dedup[0]["status"] = "去重中"
            has_dedup[0]["pending"] = True
            has_dedup[0]["abnormal"] = False
            results["dedup"] = has_dedup[0]
        else:
            new_dedup_id = max((int(r.get("id", 0)) for r in dedup_rows), default=0) + 1
            dedup_config = {
                "id": new_dedup_id,
                "窗口编号": f"DEDU-{new_dedup_id:04d}",
                "数据流": tag,
                "去重键": "event_id+timestamp",
                "窗口宽度": "10min",
                "去重算法": "BloomFilter",
                "误杀率": "0.01%",
                "内存占用": "128MB",
                "最近命中": "1420次/h",
                "status": "去重中",
                "pending": True,
                "abnormal": False,
            }
            dedup_rows.append(dedup_config)
            results["dedup"] = dedup_config

        return results