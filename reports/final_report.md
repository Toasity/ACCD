# 最终报告

## 项目概述

本项目基于 CoinMetrics 数据构建 ETL 流程并在本地 Postgres 中存储原始与处理后数据，通过统计分析评估指标的覆盖与缺失特性。
已接入真实 CoinMetrics API（catalog/assets 与 timeseries/asset-metrics），并完成 raw→processed→profiling。


## 数据覆盖

- 资产数（unique assets）：**1**
- 指标数（unique metrics）：**2**
- 频率（freq）列表：**1d**
- 总点数（n_points 总和）：**2190**
- 时间范围：**2013-01-01 00:00:00+00:00** ~ **2015-12-31 00:00:00+00:00**


## 缺失情况

- 缺失率平均值：**0.0000**
- 缺失率最大值：**0.0000**


### 缺失率最高的前 5 条（若不足则全部）：

| asset | metric | freq | missing_rate |
| --- | --- | --- | --- |
| btc | PriceUSD | 1d | 0.0 |
| btc | TxCnt | 1d | 0.0 |


## 时间覆盖与结构分析（Sprint 2）

- coverage_structure 总行数：**2**
- coverage_ratio 平均值：**1.0000**
- coverage_ratio 最小值：**1.0000**


### coverage_ratio 最低的前 5 条（若不足则全部）：

| asset | metric | freq | span_days | n_points | expected_points | coverage_ratio |
| --- | --- | --- | --- | --- | --- | --- |
| btc | PriceUSD | 1d | 1095 | 1095 | 1095 | 1.0 |
| btc | TxCnt | 1d | 1095 | 1095 | 1095 | 1.0 |


## 指标用途评估（最小结论）

- 覆盖点数较多的指标/资产通常更适合用于跨资产比较。
- 低缺失率意味着该指标更稳定，适合横向比较与解释性分析。
- 报告为描述性与解释性分析，不构成预测或投资建议。
