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

## 流程执行数据快照

采集流程（`flowline`）执行时会按「步骤序列」逐级生成步骤回执，全部步骤完成后
落一份流程执行数据快照；站点采集、字段提取、结果落地三类环节共享同一执行版本。

| 接口 | 说明 |
| --- | --- |
| `POST /api/flowline/{id}/run` | 执行流程，返回流程执行数据快照（`resume_from` 续跑、`fail_at_step` 模拟中断） |
| `GET /api/flowline/runs` | 流程运行清单，含中止状态与失败处理结果 |
| `GET /api/flowline/snapshots/{运行编号}` | 单份快照，步骤回执按序号排列 |
| `GET /api/flowline/snapshots/{运行编号}/receipts` | 选中运行查看步骤回执与输出目标 |
| `GET /api/flowline/snapshots/{运行编号}/validation` | 快照校验结论（回执完整、顺序连续、版本一致） |
| `POST /api/flowline/snapshots/{运行编号}/land` | 消费快照生成落地批次回执，并把校验结论同步到存储台账 |
| `GET /api/flowline/landings` | 落地批次回执清单，引用步骤输出与流程版本 |
| `GET /api/storage/landing-ledger` | 存储落地业务台账中同步的校验结论 |

语义约定：

- 每份步骤完成凭据与对应提取输出一起落库，下一步只读取已完成的上一份回执；
  未完成时返回「不能读取中间数据」。
- 中断（步骤失败）保留已落地回执，下一次只能从未完成步骤继续，已落地结果不会被覆盖。
- 快照组装/落库失败按事务整体回退，流程状态回退，落地批次不会读到半份数据。
- 同一执行版本再次送交只保留一份快照，后续请求读取已存在结果（`重复送交: true`）。
- 流程版本变化会另起一份快照，旧版本结果保留。
- 采集流程状态按「草稿 → 待校验 → 已启用 → 已停用」单向流转，不能回到旧环节。

前端「采集流程」页底部可选定流程运行，查看步骤回执、输出目标、失败处理结果，
并核对恢复执行后的回执数量。后端单测：`cd backend && .venv/bin/python -m unittest tests.test_flowrun -v`。

领域：数据工程
子方向：数据采集、爬虫与RPA
