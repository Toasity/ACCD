# CoinMetrics API 熟悉记录（Sprint 1 / Task 1）

## 1. 使用的 API 版本与 Base URL

- **Base URL**：`https://community-api.coinmetrics.io/v4`
- **调用方式**：HTTP GET（`requests.Session`）
- **认证**：通过环境变量 `COINMETRICS_API_KEY`（注意：不要在日志中输出密钥）

## 2. 本项目调用的端点（Endpoints）

### 2.1 Catalog：资产目录

- **Path**：`/catalog/assets`
- **目的**：验证 API 连通性并获取可用资产集合（用于选择分析资产）
- **参数**：本项目当前不传分页参数（保持最小请求）

**证据**：每次运行 `extract` 会将响应写入 `raw.api_responses`，可验证：

```bash
docker exec -it coinmetrics-db psql -U coinmetrics -d coinmetrics -c \
  "SELECT id, endpoint, status_code FROM raw.api_responses WHERE endpoint='catalog/assets' ORDER BY id DESC LIMIT 3;"
```

### 2.2 Timeseries：资产指标时间序列

- **Path**：`/timeseries/asset-metrics`
- **目的**：拉取指定资产（例如 `btc`）的多指标时间序列（例如 `PriceUSD`, `TxCnt`）

**常用参数**：

- `assets`：逗号分隔字符串，例如 `btc` 或 `btc,eth`
- `metrics`：逗号分隔字符串，例如 `PriceUSD,TxCnt`
- `frequency`：例如 `1d`（日频）
- `start_time`：ISO8601，例如 `2013-01-01T00:00:00Z`
- `end_time`：ISO8601，例如 `2015-12-31T00:00:00Z`

**踩坑记录（重要）**

- 早期请求曾包含 `limit` 参数，API 返回 400 并给出错误：

  `unsupported_parameter: Unsupported parameter 'limit'.`

- 修复方式：移除 `limit` 参数后请求成功（HTTP 200）。

本项目会把该 400 错误响应也写入 `raw.api_responses`，便于审计与调试。

**证据**：

```bash
docker exec -it coinmetrics-db psql -U coinmetrics -d coinmetrics -c \
  "SELECT id, endpoint, status_code FROM raw.api_responses WHERE endpoint='timeseries/asset-metrics' ORDER BY id DESC LIMIT 5;"
```

## 3. 响应格式观察（简要）

- `/timeseries/asset-metrics` 的响应体包含 `data` 数组；每个元素至少包含 `time` 字段。
- 指标通常以列形式出现（例如 `PriceUSD`、`TxCnt`），即每个 `data` 元素为一行、列出多个指标。
- 在 `transform` 阶段，本项目将“列式指标”展开为规范化的长表：

```
processed.metrics_long(
  asset, metric, ts, freq, value, is_missing, source_endpoint, ingested_at
)
```

## 4. 最小复现命令

1) 拉取真实 API 并写入 `raw`：

```bash
docker compose run --rm app python scripts/10_etl_run.py --stage extract
```

2) `raw` -> `processed`：

```bash
docker compose run --rm app python scripts/10_etl_run.py --stage load
```

3) 统计分析与生成最终报告：

```bash
docker compose run --rm app python scripts/20_profile_run.py
```
