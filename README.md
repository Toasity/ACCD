# 基于 CoinMetrics 加密货币数据的 ETL、统计分析与生命周期解释

本项目基于 CoinMetrics 提供的公开 REST API，构建一个完整的数据工程与统计分析流程，用于分析加密货币指标在长期历史数据（约 12 年）中的分布特征、可用性与解释边界。

# 基于 CoinMetrics 的 ETL 与统计分析（README）

本项目通过 CoinMetrics REST API 构建一个可复现的 ETL 与分析流水线，用于系统性评估加密货币指标的数据质量、覆盖度与解释边界（非价格预测）。

### Quickstart（最小运行）

1. 复制示例配置并填写 `COINMETRICS_API_KEY`：

```bash
cp .env.example .env
# 编辑 .env，填写 COINMETRICS_API_KEY
```

2. 启动数据库（初始化 SQL 会在 `db/init/` 执行）：

```bash
docker compose up -d db
```

3. 占位命令（将在后续 Step 实现完整脚本）：

```bash
python scripts/00_api_explore.py  # placeholder — 脚本将在后续步骤实现
```

以上为最小复现流程说明，完整 ETL/analysis 功能在 Step 2/3 中实现。

## 项目结构（概览）

```
crypto-coinmetrics-etl/
├─ README.md
├─ project_overview.md
├─ docker-compose.yml
├─ .env.example
|
├─ docs/                  # API 探索、ETL 设计、Scrum 记录
├─ db/init/               # PostgreSQL 初始化 SQL
├─ src/                   # 核心代码（API / ETL / Analysis）
├─ scripts/               # 一键执行脚本
├─ reports/               # 统计分析结果与最终报告
└─ tests/                 #（可选）关键组件测试
```

## 技术栈

- 数据来源：CoinMetrics REST API
- 数据库：PostgreSQL
- ETL & 分析：Python
- 容器化：Docker / Docker Compose
- 开发方法：Scrum（Sprint 迭代）

## 环境准备

### 前置条件

- Docker ≥ 20.x
- Docker Compose ≥ 2.x

### 配置环境变量

复制示例配置并填写：

```bash
cp .env.example .env
```

需要配置的关键环境变量：`COINMETRICS_API_KEY`、`POSTGRES_DB`、`POSTGRES_USER`、`POSTGRES_PASSWORD`，以及（可选）分析用的资产列表与时间范围。

## 启动数据库

启动 PostgreSQL（会自动执行 `db/init/` 中的初始化脚本）：

```bash
docker compose up -d db
```

启动后，初始化脚本应会创建 `raw`、`processed` schema 及必要表/视图。

## 执行流程（Step-by-step）

### Step 1：CoinMetrics API 探索

运行：

```bash
python scripts/00_api_explore.py
```

输出会写入 `docs/01_api-notes.md`，记录使用的 endpoint、返回字段、限制与样例响应。

### Step 2：运行 ETL（Extract → Transform → Load）

运行：

```bash
python scripts/10_etl_run.py
```

流程包括：从 CoinMetrics 拉取数据、将原始响应写入 `raw.api_responses`、执行清洗与标准化、时间对齐，并将结果写入 `processed.metrics_long`。ETL 支持幂等/增量更新。

### Step 3：统计分析与数据剖析

运行：

```bash
python scripts/20_profile_run.py
```

分析输出位于：

- `reports/profiling/tables/`
- `reports/profiling/figures/`

包括时间覆盖、缺失率、指标分布与跨资产可比性分析。

### Step 4：指标用途评估与解释

分析结论写入：`reports/final_report.md`，讨论哪些指标适合横向比较、哪些指标噪声大或资产特异性强及数据限制。

### Step 5（可选）：生命周期解释（HMM）

运行：

```bash
python scripts/30_hmm_run.py
```

该扩展使用 HMM 对资产状态进行解释性分析（用于研究用途，不作为预测模型）。

## Docker 一键复现

完整流程可在容器中运行：

```bash
docker compose up
```

或在 app 容器中单独运行任务：

```bash
docker compose run app python scripts/10_etl_run.py
docker compose run app python scripts/20_profile_run.py
```

建议在 `README` 和 `docker-compose.yml` 中明确映射卷与环境变量，确保在新机器上一键复现。

## Scrum 开发计划

计划采用 4 个 Sprint：

- Sprint 1：API 探索与原始数据获取
- Sprint 2：ETL 流程实现
- Sprint 3：统计分析与数据剖析
- Sprint 4：指标评估与 Docker 化交付

详细 Sprint 记录请见 `docs/05_sprints.md`。

## 项目成果与交付

- 可复现的 CoinMetrics 数据分析系统
- 完整的 ETL 与本地数据仓库方案
- 系统性的数据质量与分布分析报告
- 指标在解释资产生命周期方面的评估结论

## 备注

本项目聚焦数据工程与统计可解释性，所有分析均为研究/教学用途，不构成投资建议。
