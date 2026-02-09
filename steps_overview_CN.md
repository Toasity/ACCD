Step 1：项目骨架与环境

做什么

建仓库、放入目录结构（README / docs / db/init / src / scripts / reports）

写 docker-compose.yml（至少 Postgres + 一个 app 容器或本机运行脚本也行）

写 .env.example（DB_URL、COINMETRICS_API_KEY、资产列表、时间范围）

产出

docker compose up 能启动 Postgres

README 有最基本启动说明

Step 2：数据库分层与初始化 SQL
# 项目步骤总览（中文版）

下面是分步任务与预期产出，便于按 Sprint 推进与交付验收。

## Step 1：项目骨架与环境

### 做什么

- 建仓库并放入目录结构：`README`、`docs/`、`db/init/`、`src/`、`scripts/`、`reports/`
- 编写 `docker-compose.yml`（至少包含 Postgres 和一个 app 容器，或在本机运行脚本也可）
- 编写 `.env.example`（包含 `DB_URL`、`COINMETRICS_API_KEY`、资产列表、时间范围）

### 产出

- `docker compose up` 能启动 Postgres
- `README` 包含基本启动说明

## Step 2：数据库分层与初始化 SQL

### 做什么

- 在 `db/init/` 下编写初始化 SQL：创建 `raw`、`processed` schema
- 设计最小表结构：
	- `raw.api_responses`（通用：`endpoint`、`params`、`fetched_at`、`payload_json`）
	- `processed.metrics_long`（长表：`asset`, `metric`, `ts`, `value`, `freq`, `source_version`）

### 产出

- 数据库启动后自动建表（使用 `docker-entrypoint-initdb.d`）
- 可使用 `psql` 查看表结构

## Step 3：CoinMetrics API Client（熟悉 API 的核心）

### 做什么

- 实现 `src/coinmetrics/client.py`：请求、分页、重试、限流、日志
- 实现 `src/coinmetrics/endpoints.py`：封装 catalog / timeseries 的调用
- 编写 `scripts/00_api_explore.py`：
	- 拉取资产列表
	- 拉取感兴趣的指标样例与时间段
	- 生成覆盖情况摘要并写入 `docs/01_api-notes.md`

### 产出

- `python scripts/00_api_explore.py` 能跑通
- `docs/` 中记录使用的 endpoint、返回字段、限制和样例响应（课程 Task 1）

## Step 4：Extract → Raw 入库（ETL 的 E）

### 做什么

- 实现 `src/etl/extract.py`：按配置（资产 × 指标 × 时间范围）批量拉取
- 将每次请求结果写入 `raw.api_responses`（原样保存 JSON + 元信息）
- 做最小校验：HTTP 状态、空响应、字段缺失计数

### 产出

- 运行 `python scripts/10_etl_run.py --stage extract` 后，`raw` 表有数据
- 能追溯并复查任意一条原始返回

## Step 5：Transform（清洗、标准化、对齐）

### 做什么

- 实现 `src/etl/transform.py`：解析 `raw` payload 并统一成长表格式
- 定义缺失值标注/过滤规则（记录 missingness）
- 实现至少一种标准化策略（例如 `log1p` 或 `z-score`，并可配置）
- 实现时间对齐（例如日度到周度或固定频率）

### 产出

- 提供从 `raw` 到标准长表的转换函数
- 输出中间统计（缺失率、覆盖起止年、异常值数量）以支持 Task 2

## Step 6：Load → Processed 入库（ETL 的 L + 幂等）

### 做什么

- 实现 `src/etl/load.py`：将数据写入 `processed.metrics_long`
- 实现幂等写入（主键 `asset, metric, ts, freq` 或使用 upsert）
- 保证重复运行 ETL 不会重复插入

### 产出

- 完整运行 `python scripts/10_etl_run.py` 后 `processed` 表稳定可查询
- 再次运行不会导致行数重复（或仅发生增量更新）

## Step 7：统计分析与报告生成（Task 2）

### 做什么

- 实现 `src/analysis/profiling.py` 并添加 `scripts/20_profile_run.py`
- 生成到 `reports/profiling/` 的产物，至少包括：
	- `coverage`：每个资产×指标的起止日期与总点数
	- `missingness`：缺失率分布（按资产/按指标）
	- `distribution`：直方图/箱线图（至少对 2–3 个指标）
	- `comparability`：标准化前后对比

### 产出

- `reports/final_report.md`（或一组表 + 图）
- 为课程“Analyze the statistics of the data”提供直接证据

## Step 8：指标用途评估（Task 4）

### 做什么

- 在 `reports/final_report.md` 中写结构化结论：
	- 哪些指标覆盖好、稳定、适合横向比较
	- 哪些指标噪声大或资产特异性强
	- 数据限制（例如某些资产早期严重缺失）
- 可选扩展：`src/analysis/lifecycle_hmm.py`，使用多指标进行 HMM 状态解释（说明为解释性扩展）

### 产出

- 一份关于各指标可用性的结论清单（课程 Task 4）

## Step 9：Docker 化一键复现（课程硬要求）

### 做什么

- `docker-compose.yml` 至少包含：
	- `db`（Postgres）
	- `app`（用于运行 ETL 与 profiling 的 Python 环境）
- 在 `README` 中说明：

```bash
docker compose up
docker compose run app python scripts/10_etl_run.py
docker compose run app python scripts/20_profile_run.py
```

### 产出

- 能在新机器上一键启动并跑出报告（复现实验环境）

## Step 10：Scrum 文档（课程硬要求）

### 做什么

- 在 `docs/05_sprints.md` 中写 4 个 Sprint：每个包含目标、任务与 DoD（Definition of Done）
- 可选：附上燃尽图或看板截图（若使用 GitLab/GitHub Project）

### 产出

- 提供课程要求的“scrum based methods”证据
