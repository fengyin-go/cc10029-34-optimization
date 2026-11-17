# 智能爬虫采集平台

面向网页爬取、API接入、增量抓取、反爬对抗与采集质量校验的数据采集平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   ├── app/store.py          内存数据仓库与示例数据
│   ├── app/persistence.py    爬虫任务存量数据集 SQLite 持久层
│   └── data/                 运行期落库目录（*.db，已 gitignore）
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
| 采集目标 | `targetsite` | 目标站点 | 站点编号、站点名称、入口URL |
| 抽取规则 | `selector` | 选择器规则 | 规则编号、归属站点、列表选择器 |
| 爬虫引擎 | `crawler` | 爬虫任务 | 任务编号、任务名称、关联站点 |
| 代理池 | `proxy` | 代理节点 | 节点编号、IP地址、端口号 |
| 反爬对抗 | `antiblock` | 反爬策略 | 策略编号、策略名称、触发条件 |
| 定时调度 | `schedule` | 调度计划 | 计划编号、关联任务、Cron表达式 |
| 数据提取 | `extractor` | 提取模板 | 模板编号、模板名称、输入URL |
| 采集校验 | `validate` | 校验规则 | 规则编号、校验字段、期望格式 |
| 失败重试 | `retry` | 重试策略 | 策略编号、适用任务、最大重试 |
| 存储落地 | `storage` | 存储配置 | 配置编号、存储类型、连接信息 |
| 重复去重 | `dedup` | 去重策略 | 策略编号、业务键列、去重算法 |
| 质量报告 | `quality` | 质量指标 | 指标编号、指标名称、统计口径 |
| 运行监控 | `monitor` | 监控大盘 | 大盘编号、关联任务、采集速率 |
| 告警中心 | `alert` | 告警规则 | 规则编号、监控对象、告警条件 |
| 采集API | `apigate` | API配置 | 配置编号、端点路径、请求方法 |
| 历史归档 | `archive` | 归档策略 | 策略编号、数据来源、归档周期 |
| 模板市场 | `template` | 采集模板 | 模板编号、模板名称、适用站点 |
| 采集流程 | `flowline` | 流程编排 | 流程编号、流程名称、步骤序列 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 爬虫任务存量数据集（重启可连续追踪）

爬虫任务和下发批次历史上只在进程内存中，服务重启即丢。现在爬虫引擎调用链下新增
「爬虫任务存量数据集」能力，落到本地 SQLite（`backend/data/crawler_stock.db`，
可用环境变量 `CRAWLER_STOCK_DB` 覆盖路径，仅依赖标准库）。

- 存量记录沿用既有字段名（任务编号、下发批次等），新增字段只追加：任务版本、归属、
  来源、保留来源、数据集批次、内容指纹。
- 任务版本、归属、已下发批次三者同时齐全的存量记录才能进入「已完成」迁移范围，
  缺任一字段标记为「校验未通过」。
- 迁移支持中断续跑：重跑只处理待迁移/迁移中记录；记录编号由 SQLite 自增主键固化，
  已校验任务不会因重跑改变编号或重复写入。
- 外部送交按业务内容指纹判重，连续/跨批次重复送交全库只保留一份。
- 上线流水线顺序：依赖核对 → 构建环境检查 → 数据库检查 → 流水线发布；
  旧内存任务与新记录发生版本冲突时必须显式指定保留来源，发布后切换下游当前指针，
  下游只采用该来源的完整存量数据集。
- 采集目标读取当前指针数据集生成「目标站点执行快照」，快照引用迁移后的任务归属、
  任务版本与已下发批次；重复生成按指纹拒绝，只保留一份。

接口（均在爬虫引擎调用链下）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/crawler/stock-datasets/migration` | 内存存量迁移/断点续跑（`batch_size` 可模拟中断） |
| POST | `/api/crawler/stock-datasets/submissions` | 外部存量数据集送交（指纹判重） |
| GET | `/api/crawler/stock-datasets` | 指针、迁移批次、存量记录、流水线总览 |
| GET | `/api/crawler/stock-datasets/{batch}` | 单批次明细与版本冲突 |
| POST | `/api/crawler/stock-datasets/{batch}/precheck` | 依赖/构建环境/数据库三项检查 |
| POST | `/api/crawler/stock-datasets/{batch}/release` | 流水线发布并切换当前指针 |
| GET | `/api/targetsite/snapshots` | 目标站点执行快照清单 |
| POST | `/api/targetsite/snapshots/generate` | 基于当前指针数据集生成快照 |

自测（无需额外依赖）：

```bash
cd backend
.venv/bin/python -I tests/test_stockdataset_manual.py   # 7 个领域场景
.venv/bin/python -I tests/test_stockdataset_api.py      # HTTP 接口冒烟（自动起停服务）
```

领域：数据工程
子方向：数据采集、爬虫与RPA
