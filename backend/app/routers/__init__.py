"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import pipeline_api, resource

# 固定路径的交接流水线路由必须先注册，避免被 /api/{module}/{entry_id} 抢先匹配。
ROUTERS = [pipeline_api, resource]
