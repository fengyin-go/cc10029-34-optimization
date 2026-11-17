"""业务模块路由汇总。

这里统一按别名导入再暴露 ROUTERS：模块名有可能和内置名撞车（某个业务模块就叫 dict、list
这种名字时），按名字直接 import 会把内置类型覆盖掉，函数注解在运行时求值就会报
'module' object is not subscriptable。
"""
from __future__ import annotations

from app.routers import targetsite as router_targetsite
from app.routers import selector as router_selector
from app.routers import crawler as router_crawler
from app.routers import stockdataset as router_stockdataset
from app.routers import proxy as router_proxy
from app.routers import antiblock as router_antiblock
from app.routers import schedule as router_schedule
from app.routers import extractor as router_extractor
from app.routers import validate as router_validate
from app.routers import retry as router_retry
from app.routers import storage as router_storage
from app.routers import dedup as router_dedup
from app.routers import quality as router_quality
from app.routers import monitor as router_monitor
from app.routers import alert as router_alert
from app.routers import apigate as router_apigate
from app.routers import archive as router_archive
from app.routers import template as router_template
from app.routers import flowline as router_flowline

ROUTERS = [router_targetsite, router_selector, router_stockdataset, router_crawler, router_proxy, router_antiblock, router_schedule, router_extractor, router_validate, router_retry, router_storage, router_dedup, router_quality, router_monitor, router_alert, router_apigate, router_archive, router_template, router_flowline]
