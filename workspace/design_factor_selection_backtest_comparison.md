# 因子选股约束的统一成本与成交回测比较设计

## 1. 背景

行业暴露审计、残差化对照和比例配额对照已经证明：改变选股规则会明显改变目标组合，但暴露改善本身不能说明收益、回撤或交易成本改善。下一步需要在完全相同的市场输入、调仓规则、成交价格和成本参数下，把这些目标送入正式日频回测引擎，形成可审计的研究比较。

本功能是研究诊断，不修改任何正式策略的默认参数、因子权重或目标生成逻辑，也不把比较结果自动写回训练配置。

## 2. 目标与非目标

### 2.1 目标

1. 固定一个 `factor-composite-experiment` 回测配置和一个明确的评估区间。
2. 从同一份点时因子候选和同一份原始综合评分生成七组目标：
   - `baseline`：当前策略的原始综合评分；
   - `neutralized_industry`、`neutralized_size`、`neutralized_industry_size`：行业、规模、行业×规模残差化；
   - `industry_quota`、`size_quota`、`industry_size_quota`：比例行业、规模、行业×规模配额。
3. 复用同一个 `PreparedMarketData`、基准行情和 `DailyBacktestEngine` 成交口径，比较收益、回撤、波动、夏普、换手、交易成本和成交异常。
4. 输出标准人读报告及逐日、逐笔、逐目标审计文件，明确评估区间是否由调用方锁定为样本外区间。

### 2.2 非目标

- 不把残差化或配额接入正式策略注册表。
- 不在本功能中训练因子权重、选择参数或进行任何新的超参数搜索。
- 不把同一评估区间的比较结果解释为因果结论；样本外结论必须由调用方提供预先锁定、未参与方案选择的日期区间，并在报告中如实标注。
- 不实现组合优化、交易容量模型、整手约束、涨跌停撮合或分钟级成交。

## 3. 设计

### 3.1 数据和点时边界

命令只支持 `factor-composite-experiment`。它按现有策略接口加载因子输入，使用财务 `pub_date`/指标可用日期的既有 ASOF 逻辑计算因子，再按原有缩尾、方向排名和综合权重得到候选评分。行业由 `industry_classification_sw` 按信号日 `effective_date` ASOF 对齐，规模使用 `v_daily_valuation.market_cap`。

七种目标共享完全相同的候选池、因子评分、持仓数和信号日：

| 目标 | 选股方法 | 控制变量缺失时 |
|:---|:---|:---|
| baseline | 原始 `score` 降序 | 原逻辑保留 |
| neutralized_industry | 对行业哑变量回归取残差 | 该信号日不回退，记录失败 |
| neutralized_size | 对 `log(market_cap)` 回归取残差 | 该信号日不回退，记录失败 |
| neutralized_industry_size | 对行业哑变量和 `log(market_cap)` 回归取残差 | 该信号日不回退，记录失败 |
| industry_quota | 按有效行业候选数量分配 Hamilton 配额，组内按原始分数选取 | 有效候选不足持仓数时不回退 |
| size_quota | 按有效规模分组候选数量分配 Hamilton 配额，组内按原始分数选取 | 有效候选不足持仓数时不回退 |
| industry_size_quota | 按行业×规模组候选数量分配 Hamilton 配额，组内按原始分数选取 | 有效候选不足持仓数时不回退 |

控制变量缺失不会用未来日期、横截面均值或基准目标补齐。没有目标的失败信号日由引擎自然跳过，报告保留失败原因。

### 3.2 统一回测口径

回测阶段对七组目标使用同一个 `DailyBacktestEngine`：

- 信号日收盘生成目标，下一实际交易日开盘执行；
- 使用同一份区间行情和预构建市场日历、价格映射；
- 使用同一基准 ETF、初始资金、手续费和滑点；
- 使用后复权开收盘计算持仓收益；
- 使用引擎现有的停牌阻塞、退市清算和现金处理规则。

报告的换手只统计 `BUY`/`SELL` 的单边名义金额，交易成本直接汇总成交记录 `cost`；`DELIST`、`SKIP_REBALANCE` 分别单独计数，不混入换手。

为获得真正样本外比较，调用方应把评估日期设置为已经锁定且未用于选方案的测试区间，例如稳健 Walk-forward 方案的某个测试段。若省略日期覆盖参数，报告会明确标记为“配置原始区间”，不得自动称为样本外结果。

### 3.3 命令和参数

```bash
uv run main.py evaluate-factor-selection-variants \
  --backtest-config config/backtest/factor_experiment_value_growth.toml \
  --evaluation-start-date 2022-07-01 \
  --evaluation-end-date 2024-06-30
```

`--evaluation-start-date` 与 `--evaluation-end-date` 必须成对出现；不提供时使用回测配置原始区间。`--quantile-count` 默认 5，只影响规模分组和行业/规模配额，不改变因子排名。

### 3.4 输出

结果默认写入 `workspace/factor_selection_comparison/<配置名>_<时间戳>/`：

- `summary.md`：评估范围、口径、覆盖失败、各变体指标和相对基准差异；
- `parameters.json`：原始配置、实际评估配置、数据源、行业快照和口径；
- `selection_comparison.csv`：收益、风险、换手、成本、成交异常和相对基准差异；
- `selection_daily_nav.csv`：每个变体逐交易日净值、现金、持仓市值和基准净值；
- `selection_trades.csv`：每个变体逐笔成交及成本；
- `selection_targets.csv`：每个变体逐信号日目标；
- `selection_coverage.csv`：候选数、有效控制变量数、控制变量覆盖率、目标数、失败状态和失败原因；
- `selection_target_overlap.csv`：各变体与 baseline 的逐信号日目标重合。

报告不只保留收益最高的变体，所有变体和基准都完整输出，避免选择性报告。

## 4. 验收标准

- 纯逻辑测试覆盖日期覆盖校验、七组目标生成、控制变量缺失/不足持仓处理、稳定排序、目标重合、成本/换手汇总和报告写出。
- 集成路径验证所有变体使用同一预构建市场数据，且每个变体均经过正式 `DailyBacktestEngine`。
- 使用 reversal 和 value/growth 两个现有因子配置分别运行锁定测试区间，报告中的成本参数、调仓日历和基准一致。
- 通过 Ruff 检查、格式检查、全量测试和 Review Gate 后提交。

## 5. 解释边界

该比较回答的是“在同一成交与成本口径下，选股约束造成了什么收益和交易影响”，不回答“约束一定带来因果收益提升”。如果某变体覆盖不足、交易被跳过或测试区间过短，必须以覆盖和失败明细为准，不得用基准目标替代后再宣称完整比较。

## 6. 状态

已完成（代码、测试、两套真实锁定区间报告、文档和 Review Gate 已通过；提交 `3f1bbcb`）。
