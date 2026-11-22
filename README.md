# 流变工坊 · 实时CDC与流式数据同步平台

面向数据库CDC事件捕获、变更日志解析、流式数据转换、维表关联补全、水位线管理、乱序容忍、结果落湖归档与端到端延监控的实时CDC与流式同步平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| CDC数据源 | `cdc_source` | 链路源端 | 源端编号、数据库类型、连接地址 |
| 日志解析 | `binlog_parse` | 解析规则 | 规则编号、源端、日志格式 |
| 事件转换 | `event_trans` | 转换管道 | 管道编号、源端、事件类型 |
| 维表关联 | `dim_join` | 关联配置 | 配置编号、事实表、维度表 |
| 水位管理 | `watermark_mgr` | 水位线 | 水位编号、数据流、当前位点 |
| 乱序处理 | `out_of_order` | 乱序策略 | 策略编号、数据流、时间字段 |
| 结果落盘 | `sink_writer` | 落盘管道 | 管道编号、数据流、目标载体 |
| 延迟监控 | `lag_monitor` | 延迟指标 | 指标编号、数据流、源端位点 |
| DDL处理 | `ddl_handle` | DDL适配 | 适配编号、源端、DDL类型 |
| 断点恢复 | `checkpoint` | 检查点 | 检查点编号、数据流、保存位点 |
| Schema演进 | `schema_evo` | 演进记录 | 记录编号、数据流、变更列 |
| 流去重 | `dedup_stream` | 去重窗口 | 窗口编号、数据流、去重键 |
| 湖仓提交 | `iceberg_commit` | 提交快照 | 快照编号、目标表、数据文件 |
| 小文件合并 | `compaction` | 合并任务 | 任务编号、目标表、文件数量 |
| 快照过期 | `expire_snap` | 过期策略 | 策略编号、目标表、保留数量 |
| 流聚合 | `metric_aggr` | 聚合窗口 | 窗口编号、数据流、聚合键 |
| 增量回填 | `backfill_run` | 回填任务 | 任务编号、目标表、起始位点 |
| 告警规则 | `alert_rule` | 告警策略 | 策略编号、监控指标、告警条件 |
| 数据比对 | `data_diff` | 比对任务 | 任务编号、源端、目标端 |
| 编排调度 | `task_schedule` | 调度计划 | 计划编号、关联链路、Cron表达式 |
| 流审计 | `stream_audit` | 审计记录 | 记录编号、数据流、操作类型 |
| 健康报告 | `stream_health` | 健康评估 | 评估编号、数据流、健康分数 |
| 规则版本 | `parse_version` | 规则版本 | 版本编号、事件过滤、字段映射、列映射 |
| 交接点 | `handover_point` | 交接点 | 交接编号、规则版本、位点范围、续做位点 |
| 事件接入 | `event_ingest` | 接入事件 | 事件编号、源端位点、事件类型、待核原因 |
| 转换字段接纳 | `event_trans_accept` | 接纳结果 | 接纳编号、事件类型、规则版本、列版本 |
| 转换输出 | `event_output` | 转换输出 | 输出编号、源端位点、消费阻塞原因 |
| 批次消费 | `batch_consume` | 消费记录 | 消费编号、消费阶段、追溯日志位置 |

## 规则版本与事件交接

日志解析启用新配置时，事件过滤与字段映射作为一个整体发布成规则版本
（`parse_version`）。发布动作会把 `event_trans` 的事件类型直接覆盖为新版本的过滤条件，
不再残留上一版本的 INSERT 条件。

交接链路的固定接口（先于 `/{module}/{entry_id}` 动态路由注册）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/parse-rules/{id}/publish` | 发布解析规则并整体同步事件转换 |
| POST | `/api/parse-versions/{id}/fill-columns` | 补齐同一规则版本内的列定义 |
| POST | `/api/event-ingest/{id}/retry` | 重试挂起批次（缺列定义时继续挂起） |
| POST | `/api/event-ingest/{id}/resume` | 从可辨识的源日志位点续做 |
| POST | `/api/event-output/{id}/release` | 放行转换输出并按位点顺序消费 |
| POST | `/api/handover/{id}/advance` | 推进交接点，按序放行旧批输出 |
| GET | `/api/stream-handover/{dataflow}` | 工作台核对视图（版本/接纳/输出/消费） |
| POST | `/api/flows/stream-handover` | CDC 编排入口，送入数据流事件 |
| GET | `/api/batch_consume/{id}/trace` | 由消费记录反查转换输出与原日志 |

约定：

- 每条事件按自身位点绑定规则版本，删除还是插入只由该版本判定。
- 字段未核实或版本缺列定义时事件挂起，投递失败原因会点名缺少的列。
- 转换输出按源日志位点有序交接；前序位点未放行时，后续输出只落输出表，
  不进入维表关联与流去重。
- 重试幂等：同一数据流同一位点只生成一份输出、每个阶段只消费一次。
- 已生成的旧批输出保留原日志位置与原始日志，消费记录可逐条反查。

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
