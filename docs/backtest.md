# 日频股票回测

## 1. 定位与范围

`backtest/` 是基于统一视图的 A 股日频、长仓、横截面选股回测模块。当前内置 `quality-value-recovery`、`price-momentum`、`multi-factor-quality-value-momentum` 与 `factor-composite-experiment` 四个策略，统一使用月度调仓和 ETF 基准比较；另提供候选实验的训练/验证/测试与 Walk-forward 评估器。它用于验证研究假设，不能直接替代实盘交易系统。

不支持分钟级交易、融资融券、整手委托、停复牌原因、涨跌停成交限制或税费；研究评估器只在配置文件显式列出的候选和有限参数网格内搜索，不进行无上限的自动超参数寻优。启用 `[training]` 时会在训练集内真实拟合候选因子集合的非负 Ridge 权重。

## 2. 架构与数据流

```plantuml
@startuml
[v_daily_valuation] --> [Data Access]
[daily_kline] --> [Data Access]
[fin_indicator] --> [Data Access]
[etf_kline] --> [Daily Backtest Engine]
[Data Access] --> [Strategy Registry]
[Strategy Registry] --> [Daily Backtest Engine] : Target weights
[Daily Backtest Engine] --> [Result Reporter]
[Result Reporter] --> [workspace/backtest/results]
@enduml
```

回测启动时通过 `db_manager.ensure_views(...)` 按策略数据需求显式加载 `v_daily_valuation`、`daily_kline`、`fin_indicator` 和 `etf_kline`。所有市场与财务查询均经统一视图完成，不直接读取 Parquet 文件。

| 模块 | 职责 |
|:---|:---|
| `backtest/config.py` | 读取 TOML、校验日期、资金、费用与基准参数 |
| `backtest/data_access.py` | 通过统一视图加载行情、按 TTM 公告日和指标数据可用日期对齐财务因子 |
| `backtest/strategy_base.py` | 定义策略契约和标准目标权重表校验 |
| `backtest/strategy_registry.py` | 显式注册可执行策略 |
| `backtest/strategies/` | 每个策略独立加载信号数据并生成目标权重 |
| `backtest/runner.py` | 统一执行已解析的内存回测，供正式回测和研究评估复用；研究评估器可按窗口缓存因子与市场输入 |
| `backtest/factor_trainer.py` | 使用点时月末因子和未来收益拟合非负 Ridge 因子权重 |
| `backtest/hyperparameter_search.py` | 展开因子组合、因子窗口、持仓数量、缩尾范围和 Ridge 强度的有限组合 |
| `backtest/research_evaluator.py` | 按固定切分和滚动 Walk-forward 训练、选择候选并锁定测试集 |
| `analysis/factors/` | 定义可复用点时因子、注册表、计算引擎和截面变换 |
| `backtest/engine.py` | 按 T+1 开盘调仓，逐日计算净值、现金和交易成本 |
| `backtest/metrics.py` | 计算收益、波动率、夏普和最大回撤 |
| `backtest/reporter.py` | 将输入参数、目标、交易、净值和摘要写入独立结果目录 |

## 3. 无未来函数规则

1. 信号在调仓日 T 收盘后生成。
2. T 日估值来自 `v_daily_valuation`：TTM 估值分母按 `fin_ttm.pub_date` ASOF 对齐，净资产按资产负债表的 `数据可用日期` ASOF 对齐，均不能使用各自生效日之前的数据。TTM 的 `pub_date` 是当前报告期四源统一后的公告日期；四源最大日期仍作为财务源全部可用性的派生边界。同步时若公告日期超过法定期限，还会通过对应交易所官方公告二次核验并覆盖该字段。
3. `daily_kline` 视图按 `(symbol, date)` 做确定性去重，避免存量 Parquet 中的重复行污染滚动窗口；`v_daily_valuation` 和 `fin_indicator` 的 ASOF 右表均按股票及生效日期排序，保证不同 DuckDB 连接得到同一条历史记录。
4. `fin_indicator` 的质量因子在回测查询中以 `数据可用日期` ASOF 对齐，不能以 `report_date` 直接对齐；同一股票同一生效日的多条记录按 `report_date` 降序确定性去重。
5. 调仓最早在下一个实际有行情的交易日 T+1 开盘执行，禁止 T 日收盘信号以 T 日价格成交。
6. 不复权价用于估值与原始成交价记录；后复权价用于持仓收益、净值和基准收益。
7. 持仓证券的行情在其最后交易日收盘后终结（退市/摘牌），当日收盘后按收盘价强制清算为现金并记录 `DELIST` 交易；清算后不再产生交易与定价，亦不再阻塞后续调仓。行情终结判定基于数据湖实际行情（该股最后一条日线），不依赖 `stocks` 快照的 `is_active` 状态。

持仓以连续价值而非整手股数表示。这样可以隔离策略本身与不同证券价格、最小交易单位导致的资金闲置；整手及涨跌停等实盘撮合约束留待独立模块实现。

## 4. 内置策略

所有内置策略在每月最后一个交易日筛选标的，并于下一交易日开盘等权调仓。可用策略通过以下命令查看：

```bash
uv run main.py list-backtest-strategies
```

### 4.1 quality-value-recovery

| 条件 | 默认阈值 |
|:---|:---|
| 上市交易日 | 至少 250 日 |
| 估值 | `pe_ttm` 与 `pb` 均处于合格股票横截面最低 40% |
| 质量 | 加权 ROE 大于 8%，经营现金流/营业收入大于 0 |
| 趋势 | `price_trend_above_ma_120d` 确认后复权收盘价高于 120 日均线 |
| 排名 | PE 与 PB 横截面百分位之和，分数由低到高 |
| 持仓 | 前 20 只等权，可通过 TOML 的 `holding_count` 修改 |

若目标证券 T+1 没有有效开盘价，则不建仓，其目标权重保留为现金。若既有持仓没有有效开盘价（停牌等临时缺失），则该次调仓整体跳过，避免将无法交易的证券以虚构价格卖出；行情终结（退市/摘牌）的持仓不在此列——其在最后交易日收盘后已按收盘价强制清算，不再阻塞后续调仓。

该策略使用 `valuation_pe_ttm`、`valuation_pb`、`quality_roe_weighted`、`quality_operating_cashflow_ratio` 和 `price_trend_above_ma_120d` 等独立因子；PE/PB 百分位排名和策略阈值仍在策略层完成。

### 4.2 price-momentum

| 条件 | 默认阈值 |
|:---|:---|
| 上市交易日 | 至少 250 日 |
| 动量 | 后复权 120 日收益率，由高到低排名 |
| 趋势 | `price_trend_above_ma_120d` 确认后复权收盘价高于 120 日均线 |
| 持仓 | 前 20 只等权 |

动量策略仅使用市场数据，不读取财务指标；动量和趋势信号由独立因子库计算，因此它也是验证策略注册表与策略特有数据需求的基准实现。

### 4.3 multi-factor-quality-value-momentum

该策略使用独立因子库计算价值、质量、动量、趋势和低波动特征。因子在同一信号日内先缩尾并按方向做百分位排名，再按配置权重合成综合分数。

| 因子 | 默认权重 |
|:---|---:|
| `price_momentum_120d` | 20% |
| `price_trend_gap_120d` | 15% |
| `price_volatility_60d` | 10% |
| `valuation_pe_ttm` | 15% |
| `valuation_pb` | 15% |
| `quality_roe_weighted` | 12.5% |
| `quality_operating_cashflow_ratio` | 12.5% |

默认要求上市交易日不少于 250 日，综合分数最高的前 20 只股票等权持有。因子接口、输入字段和未来函数约束见 [独立因子库](factor_library.md)。

### 4.4 factor-composite-experiment

该策略用于单因子和小组合研究，不改变三个正式策略的配置和行为。调用方通过 `factor_weights` 显式选择因子；策略根据因子元数据的 `higher_is_better` 统一方向，先按交易日缩尾并做百分位排名，再按权重合成分数，选择前 `holding_count` 只股票等权持有。

实验策略最多同时使用 6 个因子。`factor_parameters` 用于传递因子自身参数，例如反转因子的窗口；同一信号日只要任一选中因子缺失，该股票就从组合中剔除。权重会在参数解析时归一化，并把因子版本和参数写入 `parameters.json`。

单因子配置示例：

```bash
uv run main.py run-backtest \
  --backtest-config config/backtest/factor_experiment_reversal.toml
```

小组合配置示例：

```bash
uv run main.py run-backtest \
  --backtest-config config/backtest/factor_experiment_value_growth.toml
```

该策略是研究实验入口，不会自动修改 `multi-factor-quality-value-momentum` 的因子权重，也不代表某个因子已经通过投资有效性检验。

### 4.5 训练/验证/测试与 Walk-forward 评估

研究评估器读取一个研究 TOML，候选回测配置必须显式列出。启用 `[training]` 后，所有 `factor-composite-experiment` 候选在各自训练窗口内使用点时月末因子和未来收益拟合非负 Ridge 权重；如果同时启用 `[hyperparameter_search]`，还会在显式有限网格中展开因子组合、因子窗口、持仓数量、缩尾范围和 Ridge 强度，每组组合分别拟合权重。验证集选择完整参数组合，测试集只运行入选组合。训练失败的组合会记录原因并排除，不会退回默认权重；只有全部组合都失败时窗口才终止。启用 `[validity]` 后，训练默认至少需要 24 个有效信号日，验证和测试默认至少需要 11 个实际可执行信号及 100 个目标观测；低于门槛会阻断对应组合或窗口，而不是仅生成收益报告。开启 Walk-forward 后，每个完整滚动窗口都会重新展开、训练和选择，最终只汇总各窗口测试段。未启用训练时，才是仅比较候选原始配置的兼容模式。

为避免有限参数网格重复加载相同点时数据和行情结构，研究评估器在每个 split 内以有界 LRU 复用因子实验的原始信号数据、基础候选表以及与目标无关的市场日历/价格映射；默认最多保留 4 组因子输入和 2 个市场区间，超出后释放最久未使用项。缩尾边界、因子权重、排名、持仓数量和逐日持仓状态仍按每组试验重新计算。缓存不跨 split，也不改变普通 `run-backtest` 的默认路径。

```bash
uv run main.py evaluate-factor-experiments \
  --research-config config/backtest/factor_experiment_evaluation.toml
```

若要进行正式的样本外研究，推荐使用 `config/backtest/factor_experiment_evaluation_robust.toml`：该配置使用 3 年训练、2 年验证、2 年测试的完整自然年 Walk-forward，验证和测试各要求至少 20 个可执行信号日，并移除基线中已确认没有区分度的 `ridge_alpha=1.0`。原配置保留为短窗口基线和选择风险对照，不能与稳健配置的测试结果混合解读。

示例配置见 `config/backtest/factor_experiment_evaluation.toml`，设计与边界见 [`workspace/design_factor_experiment_evaluation.md`](../workspace/design_factor_experiment_evaluation.md) 和 [`workspace/design_factor_hyperparameter_training.md`](../workspace/design_factor_hyperparameter_training.md)。结果写入 `workspace/backtest/evaluations/<name>_<timestamp>/`，包括标准人读结果报告 `summary.md`、`training_models.csv`、`hyperparameter_trials.csv`（每组参数的训练状态、训练/验证指标、拟合权重和失败原因）、`evaluation_failures.csv`、`research_validity.csv`（训练 / 验证 / 测试的实际覆盖、阈值和门禁结果）、`selection_diagnostics.csv`（每个窗口的比较组合数、验证信号日、第一/二名分数差距、并列数量和选择负担风险）、`factor_weight_diagnostics.csv`（全部训练组合的权重集中度、有效因子数和入选标记）、候选指标、入选记录和参数快照。`summary.md` 固定报告执行状态、点时样本覆盖、研究有效性门禁、验证集选择稳健性、全部训练组合与入选组合的权重集中度、失败组合、入选权重、训练/验证/测试表现、Walk-forward 稳定性和研究边界；CSV 是完整审计明细。验证信号日偏少或比较组合数多于验证信号日时只产生研究风险提示，不改变测试集隔离和入选规则。评估器不把测试结果反写到候选配置，也不跨窗口传递持仓或净值。

### 4.6 因子实验点时规模暴露诊断

`diagnose-factor-exposures` 是独立于训练和回测结果的暴露审计命令，仅支持 `factor-composite-experiment`。它按每个信号日的可选股票池计算 `v_daily_valuation.market_cap` 的截面规模分组，再比较最终入选持仓在各组的占比和选择提升；缺失或非正市值会从分组中排除，并单独记录覆盖率。为保证规模组编号有效，每个信号日必须至少有 `quantile_count` 个有效市值候选；不满足时命令会拒绝生成报告。该命令不改变策略目标、因子权重、成交或净值。

历史行业数据资产现已独立存储在 `industry_classification_sw`，按申万 `effective_date` 做 ASOF 对齐；当前命令仍只输出规模暴露，不直接输出行业暴露或实施行业中性化，行业覆盖与暴露由下方独立命令审计。

```bash
uv run main.py diagnose-factor-exposures \
  --backtest-config config/backtest/factor_experiment_value_growth.toml \
  --quantile-count 5
```

结果默认写入 `workspace/factor_exposure_diagnostics/<name>_<timestamp>/`，包含 `summary.md`、`size_exposure.csv`、`size_exposure_summary.csv`、`size_exposure_coverage.csv` 和 `parameters.json`。报告用于识别规模选择偏向，不构成收益因果归因。当前 `stocks.industry` 仍只有未版本化的元数据快照，不能据此输出历史行业暴露；历史行业数据已经独立存储在 `industry_classification_sw`，行业审计结果由独立命令输出，仍不实施行业中性化。

行业覆盖与暴露使用独立命令：

```bash
uv run main.py diagnose-factor-industry-exposures \
  --backtest-config config/backtest/factor_experiment_value_growth.toml
```

该命令按候选池和入选持仓的信号日，从 `industry_classification_sw.effective_date` 做 ASOF 对齐，输出行业覆盖率、缺失数量和行业选择提升；不实施行业中性化。结果默认写入 `workspace/factor_industry_exposure_diagnostics/<name>_<timestamp>/`。

### 4.7 因子实验行业/规模中性化对照

`diagnose-factor-neutralization` 是研究专用的残差化对照工具。它固定 `factor-composite-experiment` 的候选池、因子综合评分和持仓数量，逐信号日将评分对行业哑变量、`log(market_cap)` 或二者回归，使用残差重新排序，并与基准目标比较。行业通过 `industry_classification_sw` 的 `effective_date` ASOF 对齐，规模使用 `v_daily_valuation.market_cap`；缺失控制变量和不足持仓数的信号日都保留在覆盖审计中，失败时不回退。

```bash
uv run main.py diagnose-factor-neutralization \
  --backtest-config config/backtest/factor_experiment_value_growth.toml \
  --quantile-count 5
```

输出包含 `summary.md`、`parameters.json`、`neutralization_summary.csv`、`neutralization_coverage.csv`、`neutralization_target_overlap.csv`、`neutralization_industry_exposure.csv` 和 `neutralization_size_exposure.csv`。行业暴露的 universe 分母是全候选池中已分类股票，规模暴露的 universe 分母是全候选池中正且有限市值候选；selected 分母只使用相应有效目标，覆盖率文件保留被排除候选。残差化不等价于严格的组合行业/规模中性：它可能显著改变目标，却仍保留行业或规模选择偏离；是否开发带配额或权重约束的正式策略，必须基于该对照的覆盖率、目标重合、成本和样本外收益另行决定。命令不改变任何正式策略、训练参数或回测结果。

### 4.8 因子实验比例配额选股对照

`diagnose-factor-constrained-selection` 是残差化对照之后的研究工具。它固定原始综合评分，在每个信号日按有效行业、规模或行业×规模候选数量分配 `holding_count`，使用 Hamilton 最大余数法处理整数配额，组内按原始评分排序。行业使用 `industry_classification_sw` 的 `effective_date` ASOF，规模使用 `v_daily_valuation.market_cap` 的 5 组截面分组。

```bash
uv run main.py diagnose-factor-constrained-selection \
  --backtest-config config/backtest/factor_experiment_value_growth.toml \
  --quantile-count 5
```

输出包含 `summary.md`、`parameters.json`、`constraint_summary.csv`、`constraint_coverage.csv`、`constraint_target_overlap.csv` 和 `constraint_exposure.csv`。配额误差为实际入选数减目标配额，报告的占比差以各模式有效控制变量候选池为 universe 分母；缺失控制变量的候选只保留在完整候选池覆盖率中。有效候选不足持仓数时不回退。该命令不接入正式策略，不能把配额可行或目标重合较高解释为样本外收益改善。

### 4.9 因子选股变体统一成本与成交回测比较

`evaluate-factor-selection-variants` 将 `baseline`、三种残差化目标和三种比例配额目标送入同一个 `DailyBacktestEngine`。七种目标共享同一份点时因子候选、原始综合评分、行情日历、后复权价格、T+1 开盘成交、手续费、滑点和基准；比较收益、波动、夏普、回撤、换手、交易成本、跳过调仓和退市清算，不改变正式策略默认值。

推荐把已预先锁定、未参与方案选择的测试区间显式传入，才可作为样本外比较：

```bash
uv run main.py evaluate-factor-selection-variants \
  --backtest-config config/backtest/factor_experiment_value_growth.toml \
  --evaluation-start-date 2022-07-01 \
  --evaluation-end-date 2024-06-30 \
  --quantile-count 5
```

`--evaluation-start-date` 和 `--evaluation-end-date` 必须成对出现；省略时使用配置原始区间，报告会明确标记为未锁定样本外区间。结果默认写入 `workspace/factor_selection_comparison/<name>_<timestamp>/`，包括 `summary.md`、`parameters.json`、`selection_comparison.csv`、`selection_daily_nav.csv`、`selection_trades.csv`、`selection_targets.csv`、`selection_coverage.csv` 和 `selection_target_overlap.csv`。换手只统计 BUY/SELL 的单边名义金额，DELIST 与 SKIP_REBALANCE 单独计数；控制变量不足的信号日不回退到 baseline，并在覆盖文件记录失败原因。

### 4.10 多因子组合边际贡献验证

`evaluate-factor-marginal-contributions` 固定正式多因子策略的点时公共候选池，比较完整七因子组合、每个单因子和移除一个因子的七个 leave-one-out 组合。完整组合使用正式配置中的归一化权重，单因子使用 100% 权重，leave-one-out 组合按剩余正式权重重新归一化；所有变体共享同一 `PreparedMarketData`、T+1 开盘成交、手续费、滑点和净值计算，避免因行情准备或成交口径不同造成比较偏差。回测引擎对目标和成交股票按代码排序，防止集合遍历顺序改变交易记录或成本汇总的末位结果。

```bash
uv run main.py evaluate-factor-marginal-contributions \
  --backtest-config config/backtest/multi_factor_quality_value_momentum.toml \
  --evaluation-start-date 2022-07-01 \
  --evaluation-end-date 2024-06-30
```

评估日期必须成对指定，显式区间才标记为锁定评估。结果默认写入 `workspace/factor_marginal_contribution/<name>_<timestamp>/`，包括标准摘要、解析参数、组合收益/风险/换手/成本、逐日净值、逐笔成交、目标、公共候选覆盖率和相对完整组合的目标重合率。该命令只用于判断因子的边际信息和组合互补性，不自动修改正式因子权重或策略配置；基准没有行情时会明确告警，不会伪造基准收益。

## 5. 成交、成本和净值

单边交易成本为 `commission_bps + slippage_bps`。每次调仓先按开盘时持仓与目标持仓的绝对差额计算名义换手，再从组合净值中扣除成本，最后按扣成本后的净值配置目标权重。因此现金不会因成本而变为负数。

后复权收盘价涵盖分红与除权影响。引擎先将前一收盘持仓价值按当日后复权开盘价变动至开盘，再在收盘时按后复权收盘价变动，避免将公司行为误记为投资收益或损失。

## 6. 配置、命令与输出

回测输入为 TOML。配置由 `[run]`、`[strategy]` 与 `[strategy.parameters]` 组成；前两部分定义通用运行环境，最后一部分由已注册策略严格校验。示例配置位于 `config/backtest/`。

```toml
[run]
start_date = "2018-01-01"
end_date = "2026-08-07"
initial_capital = 1000000
commission_bps = 5
slippage_bps = 5
benchmark_symbol = "510300"

[strategy]
name = "price-momentum"

[strategy.parameters]
holding_count = 20
lookback_days = 120
trend_window = 120
min_listing_days = 250
```

```bash
uv run main.py run-backtest \
  --backtest-config config/backtest/price_momentum.toml
```

将 `benchmark_symbol` 设为空字符串可跳过 ETF 基准。每次运行写入 `workspace/backtest/results/<strategy>_<timestamp>/`：

| 文件 | 内容 |
|:---|:---|
| `parameters.json` | 解析并校验后的通用参数、策略参数和策略版本 |
| `daily_nav.csv` | 每日策略净值、现金、持仓市值和基准净值 |
| `rebalance_targets.csv` | 信号日、候选评分、排名和目标权重 |
| `trades.csv` | T+1 成交记录、原始及后复权开盘价、名义金额与成本 |
| `summary.md` | 普通回测的收益、风险、换手和交易记录摘要；因子实验评估另生成固定章节的训练结果报告 |

结果目录已被 Git 忽略。可复用的研究结论应在复核后写入 `investigation/`，不应把单次运行结果直接提交。

## 7. 新增策略

新增策略必须在 `backtest/strategies/` 中实现 `BacktestStrategy` 契约，并在 `backtest/strategy_registry.py` 显式注册。策略只能负责点时数据需求和标准目标权重表，不能修改引擎的 T+1 成交、复权收益与成本口径。

每个策略必须：

1. 声明名称、版本、说明和参数摘要。
2. 为 TOML 参数设置默认值、类型与范围校验，并拒绝未知参数。
3. 通过 `BacktestDataAccess` 读取统一视图，财务字段必须按公告日对齐。
4. 返回 `date`、`symbol`、`score`、`rank`、`target_weight` 五列目标权重表。
5. 提供策略筛选、未来函数边界和可手算样本测试。

## 8. 验证与扩展

单元测试覆盖 TOML 配置、策略注册、因子计算与截面变换、质量价值与动量策略排序、多因子策略权重、T+1 成交、交易成本、缺失目标开盘价处理和行情终结（退市）清算。两个旧策略迁移后还通过固定同一份点时输入的前后回测文件比较，验证目标、交易、净值和摘要保持一致。修改时间线、费用、复权口径、目标权重或清算逻辑时，必须先补充对应的可手算测试样例。

因子 IC、分位收益、覆盖率、换手、信号自相关和因子相关性诊断已由 `diagnose-factors` 提供；训练/验证/测试与 Walk-forward 评估由 `evaluate-factor-experiments` 提供。后续可独立扩展行业权重约束、卖出印花税、停牌与涨跌停规则、整手仿真、因子归因和参数敏感性分析。这些功能不得改变本模块现有的点时数据和 T+1 成交约束。
