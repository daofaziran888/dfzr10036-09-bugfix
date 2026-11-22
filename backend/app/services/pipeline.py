"""事件交接流水线：规则版本发布、字段接纳、转换输出发布与有序消费。

这里承载跨模块状态机，供 parse_version / event_ingest / event_trans_accept /
event_output / batch_consume / handover_point 以及编排层共同调用。

核心约束：
1. 列映射与事件过滤属于同一个规则版本，发布时整体替换，不能残留旧条件。
2. 进入转换的事件只按其自身绑定的规则版本判定 INSERT / DELETE 与列版本。
3. 字段未核实的输入暂缓消费，并明确列出缺少的列定义。
4. 转换输出按源日志位点有序交接；前序位点未放行时，后续输出只落输出表，
   不进入维表关联与去重。
5. 重试幂等：同一数据流、同一位点只生成一份输出、只消费一次。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

LOG_PREFIX = "binlog.001234"
CONSUMER_STAGES = ("维表关联", "流去重")
ACTIVE_VERSION_STATUS = ("已发布", "待补齐")

DEFAULT_COLUMNS: dict[str, dict[str, str]] = {
    "INSERT": {
        "order_id": "BIGINT",
        "user_id": "BIGINT",
        "amount": "DECIMAL(18,2)",
        "status": "VARCHAR(32)",
        "event_time": "TIMESTAMP",
    },
    "UPDATE": {
        "order_id": "BIGINT",
        "user_id": "BIGINT",
        "amount": "DECIMAL(18,2)",
        "status": "VARCHAR(32)",
        "event_time": "TIMESTAMP",
    },
    "REPLACE": {
        "order_id": "BIGINT",
        "user_id": "BIGINT",
        "amount": "DECIMAL(18,2)",
        "status": "VARCHAR(32)",
        "event_time": "TIMESTAMP",
    },
    "DELETE": {
        "order_id": "BIGINT",
        "status": "VARCHAR(32)",
        "event_time": "TIMESTAMP",
        "op": "VARCHAR(16)",
    },
}

REQUIRED_COLUMNS: dict[str, list[str]] = {
    "INSERT": ["order_id", "user_id", "amount", "status", "event_time"],
    "UPDATE": ["order_id", "user_id", "amount", "status", "event_time"],
    "REPLACE": ["order_id", "user_id", "amount", "status", "event_time"],
    "DELETE": ["order_id", "status", "event_time"],
}

# 同一进程内保存“可辨识的源日志位置”，支持中断后从位点续做。
SOURCE_LOG: dict[str, list[dict[str, Any]]] = {}
RESUME_OFFSET: dict[str, int] = {}


# ---------------------------------------------------------------------------
# 基础工具
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _next_id(module: str) -> int:
    rows = store.rows(module)
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def log_location(offset: int) -> str:
    return f"{LOG_PREFIX}:{offset}"


def parse_offset(text: Any) -> int | None:
    raw = str(text or "").strip()
    if ":" in raw:
        raw = raw.rsplit(":", 1)[-1]
    try:
        return int(raw)
    except ValueError:
        return None


def _filter_tokens(version: dict[str, Any]) -> list[str]:
    raw = str(version.get("事件过滤", ""))
    tokens = [token.strip().upper() for token in raw.replace("，", ",").split(",")]
    return [token for token in tokens if token]


def _columns_of(version: dict[str, Any], operation: str) -> dict[str, str]:
    columns = version.get("_columns") or {}
    return {str(name): str(kind) for name, kind in (columns.get(operation) or {}).items()}


def _required_of(version: dict[str, Any], operation: str) -> list[str]:
    required = version.get("_required") or {}
    names = required.get(operation)
    if names:
        return [str(name) for name in names]
    return list(_columns_of(version, operation))


def _missing_columns(version: dict[str, Any], operation: str) -> list[str]:
    columns = _columns_of(version, operation)
    return [name for name in _required_of(version, operation) if name not in columns]


def _format_missing(missing: list[str]) -> str:
    return "、".join(missing) if missing else "无"


def describe_columns(version: dict[str, Any]) -> str:
    parts: list[str] = []
    for operation in _filter_tokens(version):
        columns = _columns_of(version, operation)
        missing = _missing_columns(version, operation)
        if missing:
            parts.append(f"{operation}(待补齐: {_format_missing(missing)})")
        else:
            body = ", ".join(f"{name}:{kind}" for name, kind in columns.items())
            parts.append(f"{operation}({body})")
    return " | ".join(parts)


def dataflow_for_source(source: str) -> str:
    """由 CDC 源端的监听表推导数据流名称，保持与示例数据一致。"""
    for row in store.rows("cdc_source"):
        if str(row.get("源端编号", "")) == source:
            table = str(row.get("监听表") or "order_events")
            return table if table.endswith("_cdc") else f"{table}_cdc"
    return "order_events_cdc"


# ---------------------------------------------------------------------------
# 规则版本
# ---------------------------------------------------------------------------

def _versions(dataflow: str | None = None) -> list[dict[str, Any]]:
    rows = store.rows("parse_version")
    if dataflow is None:
        return list(rows)
    return [row for row in rows if str(row.get("数据流", "")) == dataflow]


def resolve_version(dataflow: str, offset: int) -> dict[str, Any] | None:
    """按源日志位点解析生效规则版本；位点必须落在版本批次范围内。"""
    candidates = [
        row for row in _versions(dataflow)
        if row.get("status") in ACTIVE_VERSION_STATUS
        and int(row.get("_range_start", 0)) <= offset
    ]
    if not candidates:
        return None
    # 批次起点更晚的版本优先，同一批内版本号更大的优先。
    return max(candidates, key=lambda row: (int(row.get("_range_start", 0)), str(row.get("规则版本", ""))))


def _create_version(
    *,
    rule_number: str,
    source: str,
    dataflow: str,
    event_filter: str,
    field_mapping: str,
    columns: dict[str, dict[str, str]],
    required: dict[str, list[str]],
    range_start: int,
    status: str,
) -> dict[str, Any]:
    rows = store.rows("parse_version")
    same_source = [row for row in rows if str(row.get("源端", "")) == source]
    version_code = f"v{len(same_source) + 1}"
    new_id = _next_id("parse_version")
    version = {
        "id": new_id,
        "status": status,
        "pending": status != "已归档",
        "abnormal": status == "待补齐",
        "版本编号": f"PV-{new_id:04d}",
        "规则编号": rule_number,
        "源端": source,
        "数据流": dataflow,
        "规则版本": version_code,
        "事件过滤": event_filter,
        "字段映射": field_mapping,
        "列版本": f"COL-{version_code}",
        "_range_start": range_start,
        "_columns": {op: dict(cols) for op, cols in columns.items()},
        "_required": {op: list(names) for op, names in required.items()},
        "_published_at": _now(),
    }
    version["列映射"] = describe_columns(version)
    rows.append(version)
    return version


def ensure_insert_version(
    *, rule_number: str, source: str, dataflow: str, field_mapping: str
) -> dict[str, Any]:
    """确保旧批 INSERT 规则版本存在；它绑定位点 40 及以前的日志。"""
    for row in _versions(dataflow):
        if str(row.get("规则版本", "")) == "v1":
            return row
    columns = {op: dict(DEFAULT_COLUMNS[op]) for op in ("INSERT", "UPDATE")}
    required = {op: list(REQUIRED_COLUMNS[op]) for op in ("INSERT", "UPDATE")}
    return _create_version(
        rule_number=rule_number,
        source=source,
        dataflow=dataflow,
        event_filter="INSERT,UPDATE",
        field_mapping=field_mapping,
        columns=columns,
        required=required,
        range_start=0,
        status="已发布",
    )


def publish_version(
    *,
    rule_number: str,
    source: str,
    dataflow: str,
    event_filter: str,
    field_mapping: str,
    range_start: int,
    columns: dict[str, dict[str, str]] | None = None,
    required: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """发布新规则版本：过滤条件与列映射作为一个整体生效并同步下游。"""
    tokens = [token.strip().upper() for token in event_filter.replace("，", ",").split(",") if token.strip()]
    columns = columns or {op: dict(DEFAULT_COLUMNS.get(op, {})) for op in tokens}
    required = required or {op: list(REQUIRED_COLUMNS.get(op, [])) for op in tokens}
    version = _create_version(
        rule_number=rule_number,
        source=source,
        dataflow=dataflow,
        event_filter=",".join(tokens),
        field_mapping=field_mapping,
        columns=columns,
        required=required,
        range_start=range_start,
        status="已发布",
    )
    refresh_version(version)
    sync_rule_and_pipeline(version)
    # 先登记交接点，再判断新批是否因缺列定义而挂起。
    ensure_handover(source, dataflow, version)
    return version


def _version_status(version: dict[str, Any]) -> str:
    for operation in _filter_tokens(version):
        if _missing_columns(version, operation):
            return "待补齐"
    return "已发布"


def refresh_version(version: dict[str, Any]) -> dict[str, Any]:
    version["status"] = _version_status(version)
    version["pending"] = version["status"] != "已归档"
    version["abnormal"] = version["status"] == "待补齐"
    version["列映射"] = describe_columns(version)
    return version


def fill_columns(version_id: int) -> tuple[dict[str, Any] | None, str]:
    """补齐版本内每个过滤动作的必要列定义，列映射仍归属同一规则版本。"""
    version = store.find("parse_version", version_id)
    if version is None:
        return None, f"规则版本 {version_id} 不存在或已归档"
    filled: list[str] = []
    columns = version.setdefault("_columns", {})
    required = version.setdefault("_required", {})
    for operation in _filter_tokens(version):
        defaults = DEFAULT_COLUMNS.get(operation)
        if not defaults:
            continue
        current = columns.setdefault(operation, {})
        for name, kind in defaults.items():
            if name not in current:
                current[name] = kind
                filled.append(f"{operation}.{name}")
        if not required.get(operation):
            required[operation] = list(defaults)
    refresh_version(version)
    # 列定义补齐后，挂起事件仍保持挂起，等待显式重试。
    for ingest in store.rows("event_ingest"):
        if ingest.get("status") != "挂起待核":
            continue
        if str(ingest.get("规则版本", "")) != str(version.get("规则版本", "")):
            continue
        if not _missing_columns(version, str(ingest.get("事件类型", "")).upper()):
            ingest["待核原因"] = f"列定义已补齐，可重试位点 {ingest.get('源端位点', '')}"
            for accept in store.rows("event_trans_accept"):
                if str(accept.get("源端位点", "")) == str(ingest.get("源端位点", "")):
                    accept["待核原因"] = ingest["待核原因"]
    sync_rule_and_pipeline(version, update_rule=False)
    ensure_handover(str(version.get("源端", "")), str(version.get("数据流", "")), version)
    note = "已补齐列定义" + (f"：{_format_missing(filled)}" if filled else "（无需补充）")
    if version["status"] == "待补齐":
        remaining = []
        for operation in _filter_tokens(version):
            remaining.extend(_missing_columns(version, operation))
        return version, f"{note}；仍缺少列定义：{_format_missing(remaining)}"
    return version, f"{note}；版本 {version.get('规则版本')} 已可接纳事件，请重试挂起批次"


def archive_version(version_id: int) -> tuple[dict[str, Any] | None, str]:
    version = store.find("parse_version", version_id)
    if version is None:
        return None, f"规则版本 {version_id} 不存在"
    version["status"] = "已归档"
    version["pending"] = False
    version["abnormal"] = False
    return version, f"规则版本 {version.get('版本编号')} 已归档，历史输出保持可追溯"


# ---------------------------------------------------------------------------
# 事件接入与转换字段接纳
# ---------------------------------------------------------------------------

def _upsert_ingest(
    *,
    dataflow: str,
    business_key: str,
    offset: int,
    operation: str,
    version: dict[str, Any] | None,
    payload: dict[str, Any],
    raw_log: str,
    status: str,
    check_result: str,
    reason: str,
) -> dict[str, Any]:
    location = log_location(offset)
    rows = store.rows("event_ingest")
    row = next(
        (item for item in rows if str(item.get("数据流", "")) == dataflow and str(item.get("源端位点", "")) == location),
        None,
    )
    if row is None:
        new_id = _next_id("event_ingest")
        row = {
            "id": new_id,
            "事件编号": f"INGT-{new_id:04d}",
            "数据流": dataflow,
        }
        rows.append(row)
    row.update({
        "业务键": business_key,
        "源端位点": location,
        "事件类型": operation,
        "规则版本": str(version.get("规则版本", "")) if version else "",
        "列版本": str(version.get("列版本", "")) if version else "",
        "字段核对": check_result,
        "原日志位置": location,
        "待核原因": reason,
        "status": status,
        "pending": status != "已转换",
        "abnormal": status == "挂起待核",
        "_payload": dict(payload),
        "_raw_log": raw_log,
    })
    return row


def _upsert_accept(
    *,
    dataflow: str,
    business_key: str,
    offset: int,
    operation: str,
    version: dict[str, Any] | None,
    status: str,
    conclusion: str,
    reason: str,
) -> dict[str, Any]:
    location = log_location(offset)
    rows = store.rows("event_trans_accept")
    row = next(
        (item for item in rows if str(item.get("数据流", "")) == dataflow and str(item.get("源端位点", "")) == location),
        None,
    )
    if row is None:
        new_id = _next_id("event_trans_accept")
        row = {
            "id": new_id,
            "接纳编号": f"ACPT-{new_id:04d}",
            "数据流": dataflow,
        }
        rows.append(row)
    row.update({
        "业务键": business_key,
        "源端位点": location,
        "事件类型": operation,
        "规则版本": str(version.get("规则版本", "")) if version else "",
        "列版本": str(version.get("列版本", "")) if version else "",
        "接纳结论": conclusion,
        "待核原因": reason,
        "status": status,
        "pending": status != "已转换",
        "abnormal": status == "暂缓",
    })
    return row


def ingest_event(
    *,
    dataflow: str,
    business_key: str,
    offset: int,
    operation: str,
    payload: dict[str, Any],
    raw_log: str | None = None,
) -> dict[str, Any]:
    """按事件位点绑定规则版本，完成字段接纳；未核实的输入挂起不消费。"""
    operation = operation.upper()
    raw_log = raw_log or f"[{log_location(offset)}] stream={dataflow} op={operation} pk={business_key}"
    # 同一位点重复投递视为同一事件：沿用已登记内容并补齐本轮携带的字段。
    source_events = SOURCE_LOG.setdefault(dataflow, [])
    logged = next((item for item in source_events if int(item.get("offset", -1)) == offset), None)
    if logged is not None:
        merged = dict(logged.get("payload") or {})
        merged.update({key: value for key, value in payload.items() if value is not None and str(value).strip() != ""})
        payload = merged
        raw_log = str(logged.get("raw_log") or raw_log)
        operation = str(logged.get("event_type") or operation).upper()
        business_key = str(logged.get("business_key") or business_key)
    else:
        source_events.append({
            "offset": offset,
            "event_type": operation,
            "business_key": business_key,
            "payload": dict(payload),
            "raw_log": raw_log,
        })

    version = resolve_version(dataflow, offset)
    if version is None:
        ingest = _upsert_ingest(
            dataflow=dataflow, business_key=business_key, offset=offset, operation=operation,
            version=None, payload=payload, raw_log=raw_log,
            status="挂起待核", check_result="未通过",
            reason=f"未找到覆盖位点 {log_location(offset)} 的规则版本",
        )
        return {"ok": False, "event": ingest, "missing_columns": [], "message": ingest["待核原因"]}

    tokens = _filter_tokens(version)
    missing = _missing_columns(version, operation)
    unverified = [
        name for name in _required_of(version, operation)
        if payload.get(name) is None or str(payload.get(name)).strip() == ""
    ]

    if operation not in tokens:
        reason = f"事件类型 {operation} 不在规则版本 {version.get('规则版本')} 的过滤条件（{version.get('事件过滤')}）内"
    elif missing:
        reason = (
            f"投递失败：规则版本 {version.get('规则版本')}（{operation}）缺少列定义："
            f"{_format_missing(missing)}，事件暂缓消费"
        )
    elif unverified:
        reason = f"字段未核实：{_format_missing(unverified)} 在入站事件中没有值，事件暂缓消费"
    else:
        reason = ""

    if reason:
        ingest = _upsert_ingest(
            dataflow=dataflow, business_key=business_key, offset=offset, operation=operation,
            version=version, payload=payload, raw_log=raw_log,
            status="挂起待核", check_result="待核实", reason=reason,
        )
        accept = _upsert_accept(
            dataflow=dataflow, business_key=business_key, offset=offset, operation=operation,
            version=version, status="暂缓", conclusion="待核", reason=reason,
        )
        _sync_pipeline_for_source(str(version.get("源端", "")), dataflow)
        return {
            "ok": False, "event": ingest, "acceptance": accept,
            "missing_columns": missing, "message": reason,
            "rule_version": version.get("规则版本"), "column_version": version.get("列版本"),
        }

    ingest = _upsert_ingest(
        dataflow=dataflow, business_key=business_key, offset=offset, operation=operation,
        version=version, payload=payload, raw_log=raw_log,
        status="已接纳", check_result="通过", reason="",
    )
    accept = _upsert_accept(
        dataflow=dataflow, business_key=business_key, offset=offset, operation=operation,
        version=version, status="已通过", conclusion="已通过", reason="",
    )
    output, created = _create_output(
        dataflow=dataflow, business_key=business_key, offset=offset,
        operation=operation, version=version, raw_log=raw_log,
    )
    if not created and output.get("status") == "已消费":
        ingest["status"] = "已转换"
        ingest["pending"] = False
        accept["status"] = "已转换"
        accept["pending"] = False
    _sync_pipeline_for_source(str(version.get("源端", "")), dataflow)
    message = (
        f"事件位点 {log_location(offset)} 已按规则版本 {version.get('规则版本')}"
        f"（{operation} / {version.get('列版本')}）接纳"
    )
    if not created:
        message += "；转换输出已存在，重试不重复生效"
    return {
        "ok": True, "event": ingest, "acceptance": accept, "output": output,
        "created": created, "duplicate": not created,
        "rule_version": version.get("规则版本"), "column_version": version.get("列版本"),
        "message": message,
    }


def retry_ingest(entry_id: int) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """重试挂起批次：仍缺列定义时继续挂起，补齐后只生成一份输出。"""
    ingest = store.find("event_ingest", entry_id)
    if ingest is None:
        return None, {"ok": False, "message": f"接入事件 {entry_id} 不存在"}
    result = ingest_event(
        dataflow=str(ingest.get("数据流", "")),
        business_key=str(ingest.get("业务键", "")),
        offset=int(parse_offset(ingest.get("源端位点", "0")) or 0),
        operation=str(ingest.get("事件类型", "")),
        payload=dict(ingest.get("_payload") or {}),
        raw_log=str(ingest.get("_raw_log", "")),
    )
    return ingest, result


def retry_accept(accept_id: int) -> dict[str, Any]:
    accept = store.find("event_trans_accept", accept_id)
    if accept is None:
        return {"ok": False, "message": f"接纳结果 {accept_id} 不存在"}
    ingest = next(
        (row for row in store.rows("event_ingest")
         if str(row.get("数据流", "")) == str(accept.get("数据流", ""))
         and str(row.get("源端位点", "")) == str(accept.get("源端位点", ""))),
        None,
    )
    if ingest is None:
        return {"ok": False, "message": "找不到对应的接入事件，无法重试"}
    _, result = retry_ingest(int(ingest["id"]))
    return result


# ---------------------------------------------------------------------------
# 转换输出
# ---------------------------------------------------------------------------

def _outputs(dataflow: str) -> list[dict[str, Any]]:
    return [
        row for row in store.rows("event_output")
        if str(row.get("数据流", "")) == dataflow
    ]


def _ensure_downstream_pipeline(dataflow: str) -> None:
    """确保关联与去重窗口存在，消费记录会写在这两个阶段下。"""
    if not any(str(row.get("事实表", "")) == dataflow for row in store.rows("dim_join")):
        new_id = _next_id("dim_join")
        store.rows("dim_join").append({
            "id": new_id,
            "配置编号": f"DIMJ-{new_id:04d}",
            "事实表": dataflow,
            "维度表": "dim_customer_profile",
            "关联键": "customer_id",
            "关联类型": "LEFT JOIN",
            "缺失策略": "填充默认画像",
            "缓存TTL": "300s",
            "命中率": "92.5%",
            "status": "关联中",
            "pending": True,
            "abnormal": False,
        })
    if not any(str(row.get("数据流", "")) == dataflow for row in store.rows("dedup_stream")):
        new_id = _next_id("dedup_stream")
        store.rows("dedup_stream").append({
            "id": new_id,
            "窗口编号": f"DED-{new_id:04d}",
            "数据流": dataflow,
            "去重键": "order_id+event_time",
            "窗口宽度": "10min",
            "去重算法": "BloomFilter-误杀率0.01%",
            "误杀率": "0.008%",
            "内存占用": "128MB",
            "最近命中": "0次/h",
            "status": "去重中",
            "pending": True,
            "abnormal": False,
        })


def _create_output(
    *,
    dataflow: str,
    business_key: str,
    offset: int,
    operation: str,
    version: dict[str, Any],
    raw_log: str,
) -> tuple[dict[str, Any], bool]:
    location = log_location(offset)
    existing = next((row for row in _outputs(dataflow) if str(row.get("源端位点", "")) == location), None)
    if existing is not None:
        _ensure_downstream_pipeline(dataflow)
        return existing, False
    _ensure_downstream_pipeline(dataflow)

    earlier = [
        row for row in _outputs(dataflow)
        if int(parse_offset(row.get("源端位点", "0")) or 0) < offset
        and row.get("status") != "已消费"
    ]
    handover = find_handover(dataflow)
    handover_conflict = bool(
        handover and handover.get("status") == "冲突" and int(handover.get("_range_start", 0)) > offset
    )

    new_id = _next_id("event_output")
    output = {
        "id": new_id,
        "status": "待消费",
        "pending": True,
        "abnormal": False,
        "输出编号": f"OUTP-{new_id:04d}",
        "数据流": dataflow,
        "业务键": business_key,
        "源端位点": location,
        "事件类型": operation,
        "规则版本": str(version.get("规则版本", "")),
        "列版本": str(version.get("列版本", "")),
        "原日志位置": location,
        "原始日志": raw_log,
        "消费阻塞原因": "",
        "输出说明": f"转换字段接纳结果已确认：{operation} / {version.get('列版本')}",
    }
    if earlier:
        earliest = min(int(parse_offset(row.get("源端位点", "0")) or 0) for row in earlier)
        output["status"] = "待消费"
        output["消费阻塞原因"] = f"前序位点 {log_location(earliest)} 未放行/未消费，暂不进入关联与去重"
        output["输出说明"] = "输出已生成，等待按位点顺序消费"
    elif handover_conflict:
        output["status"] = "待放行"
        output["消费阻塞原因"] = (
            str(handover.get("冲突说明", "交接点冲突，旧批输出暂缓发布"))
            if handover else "交接点冲突"
        )
        output["输出说明"] = "已生成但暂停在转换输出发布前"
    else:
        output["status"] = "待消费"
    store.rows("event_output").append(output)
    if not earlier and not handover_conflict:
        drain_outputs(dataflow)
    return output, True


def drain_outputs(dataflow: str) -> dict[str, Any]:
    """按源日志位点顺序消费输出；重试幂等，同一阶段同一位点只消费一次。"""
    outputs = sorted(_outputs(dataflow), key=lambda row: int(parse_offset(row.get("源端位点", "0")) or 0))
    consumed_now: list[str] = []
    for output in outputs:
        if output.get("status") == "已消费":
            continue
        offset = int(parse_offset(output.get("源端位点", "0")) or 0)
        earlier = [
            row for row in outputs
            if int(parse_offset(row.get("源端位点", "0")) or 0) < offset
            and row.get("status") != "已消费"
        ]
        if earlier:
            earliest = min(int(parse_offset(row.get("源端位点", "0")) or 0) for row in earlier)
            output["status"] = "待消费"
            output["pending"] = True
            output["消费阻塞原因"] = f"前序位点 {log_location(earliest)} 未放行/未消费，暂不进入关联与去重"
            continue
        if output.get("status") == "待放行":
            output["pending"] = True
            output["消费阻塞原因"] = "转换输出尚未放行"
            continue
        _consume_output(output)
        consumed_now.append(str(output.get("源端位点", "")))
    _refresh_handover_after_drain(dataflow)
    return {"consumed": consumed_now}


def _consume_output(output: dict[str, Any]) -> None:
    dataflow = str(output.get("数据流", ""))
    location = str(output.get("源端位点", ""))
    for stage in CONSUMER_STAGES:
        rows = store.rows("batch_consume")
        exists = next(
            (row for row in rows
             if str(row.get("数据流", "")) == dataflow
             and str(row.get("消费阶段", "")) == stage
             and str(row.get("源端位点", "")) == location),
            None,
        )
        if exists is not None:
            continue
        new_id = _next_id("batch_consume")
        rows.append({
            "id": new_id,
            "status": "已消费",
            "pending": False,
            "abnormal": False,
            "消费编号": f"CONS-{new_id:04d}",
            "数据流": dataflow,
            "消费阶段": stage,
            "源端位点": location,
            "输出编号": str(output.get("输出编号", "")),
            "事件类型": str(output.get("事件类型", "")),
            "规则版本": str(output.get("规则版本", "")),
            "消费结果": f"已消费 {output.get('事件类型')} / {output.get('规则版本')}",
            "追溯日志位置": location,
            "原始日志": str(output.get("原始日志", "")),
            "列版本": str(output.get("列版本", "")),
        })
    output["status"] = "已消费"
    output["pending"] = False
    output["abnormal"] = False
    output["消费阻塞原因"] = ""
    output["输出说明"] = "已按位点顺序进入维表关联与流去重"
    _mark_event_done(dataflow, location)
    _write_audit(
        dataflow=dataflow,
        operation=f"按位点消费-{stage}-{output.get('事件类型')}-{output.get('规则版本')}",
        conclusion=f"位点 {location} 已进入{stage}，可反查原日志",
    )


def _mark_event_done(dataflow: str, location: str) -> None:
    for ingest in store.rows("event_ingest"):
        if str(ingest.get("数据流", "")) == dataflow and str(ingest.get("源端位点", "")) == location:
            ingest["status"] = "已转换"
            ingest["pending"] = False
            ingest["abnormal"] = False
    for accept in store.rows("event_trans_accept"):
        if str(accept.get("数据流", "")) == dataflow and str(accept.get("源端位点", "")) == location:
            accept["status"] = "已转换"
            accept["pending"] = False
            accept["abnormal"] = False


def release_output(output_id: int) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    output = store.find("event_output", output_id)
    if output is None:
        return None, {"ok": False, "message": f"转换输出 {output_id} 不存在"}
    dataflow = str(output.get("数据流", ""))
    if output.get("status") == "已消费":
        return output, {"ok": True, "message": f"位点 {output.get('源端位点')} 已消费，重试不重复生效", "consumed": []}
    if output.get("status") == "待放行":
        output["status"] = "待消费"
        output["消费阻塞原因"] = ""
        output["输出说明"] = "已放行，等待按位点顺序消费"
    drained = drain_outputs(dataflow)
    return output, {
        "ok": True,
        "message": f"已放行位点 {output.get('源端位点')}，按位点顺序消费：{_format_missing(drained['consumed'])}",
        "consumed": drained["consumed"],
    }


def replay_consume(entry_id: int) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    consume = store.find("batch_consume", entry_id)
    if consume is None:
        return None, {"ok": False, "message": f"消费记录 {entry_id} 不存在"}
    dataflow = str(consume.get("数据流", ""))
    drained = drain_outputs(dataflow)
    message = f"消费记录 {consume.get('消费编号')} 已存在，重放幂等"
    if drained["consumed"]:
        message += f"；本次补消费位点：{_format_missing(drained['consumed'])}"
    return consume, {"ok": True, "message": message, "consumed": drained["consumed"]}


# ---------------------------------------------------------------------------
# 交接点
# ---------------------------------------------------------------------------

def _handover_rows() -> list[dict[str, Any]]:
    return store.rows("handover_point")


def find_handover(dataflow: str) -> dict[str, Any] | None:
    return next(
        (row for row in _handover_rows() if str(row.get("数据流", "")) == dataflow),
        None,
    )


def ensure_handover(source: str, dataflow: str, version: dict[str, Any]) -> dict[str, Any]:
    row = find_handover(dataflow)
    pending_outputs = [
        item for item in _outputs(dataflow)
        if item.get("status") in ("待放行", "待消费")
    ]
    old_versions = [
        item for item in _versions(dataflow)
        if str(item.get("规则版本", "")) != str(version.get("规则版本", ""))
        and item.get("status") != "已归档"
    ]
    missing: list[str] = []
    for operation in _filter_tokens(version):
        missing.extend(_missing_columns(version, operation))
    counts = "、".join(
        f"位点{log_location(int(parse_offset(item.get('源端位点', '0')) or 0))}"
        for item in sorted(pending_outputs, key=lambda item: int(parse_offset(item.get("源端位点", "0")) or 0))
    )
    if row is None:
        new_id = _next_id("handover_point")
        row = {
            "id": new_id,
            "交接编号": f"HAND-{new_id:04d}",
            "数据流": dataflow,
            "源端": source,
        }
        _handover_rows().append(row)
    row.update({
        "规则版本": f"{old_versions[0].get('规则版本') if old_versions else 'v1'}→{version.get('规则版本')}",
        "位点范围": f"≤{int(version.get('_range_start', 0)) - 1} | ≥{version.get('_range_start', 0)}",
        "前序位点": log_location(int(version.get("_range_start", 0)) - 1) if int(version.get("_range_start", 0)) > 0 else "无",
        "续做位点": counts or log_location(int(version.get("_range_start", 0))),
    })
    if missing:
        row["status"] = "冲突"
        row["pending"] = True
        row["abnormal"] = True
        row["冲突说明"] = (
            f"旧批仍有数据待转换（{counts or '位点在途'}），"
            f"新批 {version.get('规则版本')} 挂在缺少必要映射：{_format_missing(missing)}"
        )
    elif pending_outputs:
        row["status"] = "冲突"
        row["pending"] = True
        row["abnormal"] = True
        row["冲突说明"] = (
            f"旧批输出 {counts} 等待放行，新批 {version.get('规则版本')} 已补齐列定义"
        )
    else:
        row["status"] = "就绪"
        row["pending"] = False
        row["abnormal"] = False
        row["冲突说明"] = f"新批 {version.get('规则版本')} 列定义完整，等待位点进入"
    row["_version_id"] = version.get("id")
    row["_range_start"] = version.get("_range_start")
    _write_audit(
        dataflow=dataflow,
        operation=f"规则版本发布-{version.get('规则版本')}-{version.get('事件过滤')}",
        conclusion=str(row.get("冲突说明", "")),
        abnormal=bool(row.get("abnormal")),
    )
    return row


def _refresh_handover_after_drain(dataflow: str) -> None:
    row = find_handover(dataflow)
    if row is None:
        return
    pending_outputs = [item for item in _outputs(dataflow) if item.get("status") != "已消费"]
    all_outputs = _outputs(dataflow)
    if not pending_outputs and all_outputs:
        row["status"] = "已完成"
        row["pending"] = False
        row["abnormal"] = False
        row["冲突说明"] = "已按源日志位点顺序完成交接，旧批输出保持可追溯"
        max_offset = max(int(parse_offset(item.get("源端位点", "0")) or 0) for item in all_outputs)
        row["续做位点"] = log_location(max_offset + 1)
        RESUME_OFFSET[dataflow] = max_offset + 1
    else:
        earliest = min(
            (item for item in all_outputs if item.get("status") != "已消费"),
            key=lambda item: int(parse_offset(item.get("源端位点", "0")) or 0),
            default=None,
        )
        row["status"] = "冲突"
        row["pending"] = True
        row["abnormal"] = True
        if earliest is not None:
            row["续做位点"] = str(earliest.get("源端位点", ""))


def advance_handover(handover_id: int) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    row = store.find("handover_point", handover_id)
    if row is None:
        return None, {"ok": False, "message": f"交接点 {handover_id} 不存在"}
    dataflow = str(row.get("数据流", ""))
    pending = sorted(
        (item for item in _outputs(dataflow) if item.get("status") == "待放行"),
        key=lambda item: int(parse_offset(item.get("源端位点", "0")) or 0),
    )
    released: list[str] = []
    for output in pending:
        output["status"] = "待消费"
        output["消费阻塞原因"] = ""
        released.append(str(output.get("源端位点", "")))
    drained = drain_outputs(dataflow)
    # 推进交接：把已就绪的新批正式标记为推进中，全部消费完成后由 drain 收敛为已完成。
    if row.get("status") != "已完成":
        row["status"] = "已完成" if not _outputs(dataflow) or all(
            item.get("status") == "已消费" for item in _outputs(dataflow)
        ) else "推进中"
        row["pending"] = row["status"] != "已完成"
        row["abnormal"] = False
    return row, {
        "ok": True,
        "message": (
            f"交接点已推进；放行 {_format_missing(released)}，消费 {_format_missing(drained['consumed'])}"
        ),
        "released": released,
        "consumed": drained["consumed"],
    }


# ---------------------------------------------------------------------------
# 中断续做
# ---------------------------------------------------------------------------

def resume_from(dataflow: str, offset: int) -> dict[str, Any]:
    """从可辨识的源日志位点续做；已接纳且已生成输出的事件不再重复处理。"""
    ingest_by_offset = {
        int(parse_offset(row.get("源端位点", "0")) or 0): row
        for row in store.rows("event_ingest")
        if str(row.get("数据流", "")) == dataflow
    }
    events = [
        item for item in SOURCE_LOG.get(dataflow, [])
        if int(item.get("offset", -1)) >= offset
    ]
    events.sort(key=lambda item: int(item.get("offset", 0)))
    results: list[dict[str, Any]] = []
    skipped = 0
    for item in events:
        event_offset = int(item.get("offset", 0))
        known = ingest_by_offset.get(event_offset)
        if known is not None and known.get("status") == "已转换":
            skipped += 1
            continue
        results.append(ingest_event(
            dataflow=dataflow,
            business_key=str(item.get("business_key", "")),
            offset=event_offset,
            operation=str(item.get("event_type", "")),
            payload=dict(item.get("payload") or {}),
            raw_log=str(item.get("raw_log", "")),
        ))
    resume_at = max((int(item.get("offset", offset)) for item in events), default=offset - 1) + 1
    RESUME_OFFSET[dataflow] = resume_at
    row = find_handover(dataflow)
    if row is not None:
        row["续做位点"] = log_location(resume_at)
    succeeded = [item for item in results if item.get("ok")]
    return {
        "ok": True,
        "message": (
            f"从 {log_location(offset)} 续做，处理 {len(results)} 条，"
            f"成功 {len(succeeded)} 条，跳过已完成 {skipped} 条，续做位点 {log_location(resume_at)}"
        ),
        "resume_from": log_location(offset),
        "resume_at": log_location(resume_at),
        "results": results,
    }


def resume_entry(entry_id: int) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    ingest = store.find("event_ingest", entry_id)
    if ingest is None:
        return None, {"ok": False, "message": f"接入事件 {entry_id} 不存在"}
    dataflow = str(ingest.get("数据流", ""))
    offset = int(parse_offset(ingest.get("源端位点", "0")) or 0)
    handover = find_handover(dataflow)
    start = RESUME_OFFSET.get(dataflow, offset)
    if handover is not None and handover.get("续做位点"):
        start = min(start, int(parse_offset(handover.get("续做位点")) or start))
    result = resume_from(dataflow, start)
    return ingest, result


# ---------------------------------------------------------------------------
# 下游同步与快照
# ---------------------------------------------------------------------------

def _write_audit(
    *, dataflow: str, operation: str, conclusion: str, operator: str = "stream_handover", abnormal: bool = False
) -> dict[str, Any]:
    rows = store.rows("stream_audit")
    new_id = _next_id("stream_audit")
    row = {
        "id": new_id,
        "status": "已记录",
        "pending": True,
        "abnormal": abnormal,
        "记录编号": f"SA-{new_id:04d}",
        "数据流": dataflow,
        "操作类型": operation,
        "影响表": dataflow.replace("_cdc", ""),
        "操作人": operator,
        "操作时间": _now(),
        "审计结论": conclusion,
        "合规标记": "合规",
    }
    rows.append(row)
    return row


def trace_consumption(consume_id: int) -> dict[str, Any] | None:
    """由消费记录反查转换输出、接入事件与原始日志。"""
    consume = store.find("batch_consume", consume_id)
    if consume is None:
        return None
    output = next(
        (row for row in store.rows("event_output")
         if str(row.get("输出编号", "")) == str(consume.get("输出编号", ""))),
        None,
    )
    ingest = next(
        (row for row in store.rows("event_ingest")
         if str(row.get("数据流", "")) == str(consume.get("数据流", ""))
         and str(row.get("源端位点", "")) == str(consume.get("源端位点", ""))),
        None,
    )
    return {
        "consumption": {key: value for key, value in consume.items() if not key.startswith("_")},
        "output": ({key: value for key, value in output.items() if not key.startswith("_")} if output else None),
        "ingest": ({key: value for key, value in ingest.items() if not key.startswith("_")} if ingest else None),
        "raw_log": str(consume.get("原始日志") or (ingest or {}).get("_raw_log") or ""),
        "trace_location": str(consume.get("追溯日志位置", "")),
    }


def _sync_pipeline_for_source(source: str, dataflow: str) -> dict[str, Any] | None:
    versions = [
        row for row in _versions(dataflow)
        if row.get("status") in ACTIVE_VERSION_STATUS
    ]
    if not versions:
        return None
    active = max(versions, key=lambda row: (int(row.get("_range_start", 0)), str(row.get("规则版本", ""))))
    return sync_rule_and_pipeline(active, update_rule=False)


def sync_rule_and_pipeline(version: dict[str, Any], *, update_rule: bool = True) -> dict[str, Any] | None:
    """把规则版本整体同步到解析规则和已激活的事件转换管道。

    事件类型直接覆盖为新版本的过滤条件，确保不会残留上一版本的 INSERT 条件。
    """
    source = str(version.get("源端", ""))
    dataflow = str(version.get("数据流", ""))
    if update_rule:
        for rule in store.rows("binlog_parse"):
            if str(rule.get("源端", "")) == source:
                rule["事件过滤"] = str(version.get("事件过滤", ""))
                rule["字段映射"] = str(version.get("字段映射", ""))
                rule["status"] = "解析中"
                rule["pending"] = True
                rule["abnormal"] = version.get("status") == "待补齐"

    tokens = _filter_tokens(version)
    functions = ",".join(f"map_{token.lower()}_columns" for token in tokens) + ",normalize_pk"
    pending_reason = _pipeline_pending_reason(dataflow)
    row = next(
        (item for item in store.rows("event_trans") if str(item.get("源端", "")) == source),
        None,
    )
    if row is None:
        new_id = _next_id("event_trans")
        row = {
            "id": new_id,
            "管道编号": f"EVEN-{new_id:04d}",
            "源端": source,
            "输出格式": "Avro",
            "失败策略": "写入死信队列",
            "转换顺序": "先字段接纳后类型转换",
            "最近吞吐": "0 eps",
        }
        store.rows("event_trans").append(row)
    latest_event = _latest_ingest(dataflow)
    row.update({
        "事件类型": str(version.get("事件过滤", "")),
        "转换函数": functions,
        "列版本": str(version.get("列版本", "")),
        "当前事件": (
            f"{latest_event.get('事件编号')} 位点{latest_event.get('源端位点')} "
            f"{latest_event.get('事件类型')} {latest_event.get('规则版本')}"
            if latest_event else "无"
        ),
        "待核原因": pending_reason,
        "status": row.get("status") if row.get("status") == "已停止" else "转换中",
        "pending": True,
        "abnormal": bool(pending_reason),
    })
    return row


def _latest_ingest(dataflow: str) -> dict[str, Any] | None:
    rows = [
        row for row in store.rows("event_ingest")
        if str(row.get("数据流", "")) == dataflow
    ]
    if not rows:
        return None
    return max(rows, key=lambda row: int(parse_offset(row.get("源端位点", "0")) or 0))


def _pipeline_pending_reason(dataflow: str) -> str:
    reasons = [
        str(row.get("待核原因", ""))
        for row in store.rows("event_ingest")
        if str(row.get("数据流", "")) == dataflow and row.get("status") == "挂起待核"
    ]
    return "；".join(reason for reason in reasons if reason)


def _public(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: value for key, value in row.items() if not key.startswith("_")}


def snapshot(dataflow: str) -> dict[str, Any]:
    """返回某一数据流在交接流水线上的完整视图，便于工作台核对。"""
    versions = sorted(_versions(dataflow), key=lambda row: str(row.get("规则版本", "")))
    events = sorted(
        (row for row in store.rows("event_ingest") if str(row.get("数据流", "")) == dataflow),
        key=lambda row: int(parse_offset(row.get("源端位点", "0")) or 0),
    )
    acceptance = sorted(
        (row for row in store.rows("event_trans_accept") if str(row.get("数据流", "")) == dataflow),
        key=lambda row: int(parse_offset(row.get("源端位点", "0")) or 0),
    )
    outputs = sorted(
        _outputs(dataflow),
        key=lambda row: int(parse_offset(row.get("源端位点", "0")) or 0),
    )
    consumptions = sorted(
        (row for row in store.rows("batch_consume") if str(row.get("数据流", "")) == dataflow),
        key=lambda row: (int(parse_offset(row.get("源端位点", "0")) or 0), str(row.get("消费阶段", ""))),
    )
    pending_events = [row for row in events if row.get("status") == "挂起待核"]
    pending_reason = "；".join(str(row.get("待核原因", "")) for row in pending_events)
    handover = find_handover(dataflow)
    pipeline = next(
        (row for row in store.rows("event_trans")
         if str(row.get("源端", "")) == (str(handover.get("源端", "")) if handover else "")),
        None,
    )
    return {
        "dataflow": dataflow,
        "versions": [_public(row) for row in versions],
        "events": [_public(row) for row in events],
        "acceptance": [_public(row) for row in acceptance],
        "outputs": [_public(row) for row in outputs],
        "consumptions": [_public(row) for row in consumptions],
        "pending_events": [_public(row) for row in pending_events],
        "pending_reason": pending_reason,
        "handover": _public(handover),
        "pipeline": _public(pipeline),
        "resume_from": log_location(RESUME_OFFSET.get(dataflow, 0)),
        "stage": {
            "accepted": sum(1 for row in events if row.get("status") in ("已接纳", "已转换")),
            "hung": len(pending_events),
            "published_outputs": [str(row.get("源端位点", "")) for row in outputs if row.get("status") != "待放行"],
            "consumed_outputs": [str(row.get("源端位点", "")) for row in outputs if row.get("status") == "已消费"],
            "dim_join_consumed": [
                str(row.get("源端位点", "")) for row in consumptions if row.get("消费阶段") == "维表关联"
            ],
            "dedup_consumed": [
                str(row.get("源端位点", "")) for row in consumptions if row.get("消费阶段") == "流去重"
            ],
        },
    }
