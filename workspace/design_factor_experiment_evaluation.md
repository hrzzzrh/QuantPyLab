# 因子实验的训练、验证、测试与 Walk-forward 设计

**设计日期**：2026-08-20
**设计状态**：已完成实施
**适用范围**：已注册回测策略的时间切分评估，首要服务于 `factor-composite-experiment`

## 1. 目标

建立一条严格按时间顺序运行的研究流程，自动完成：

1. 在训练集内真实拟合候选因子组合的非负 Ridge 权重。
2. 仅使用验证集的指定指标选择候选因子集合或策略结构。
3. 只对被选中的方案运行最终测试集，使用训练窗口冻结的权重，避免测试集参与筛选。
4. 按固定长度滚动生成多个训练/验证/测试窗口，每个窗口重新拟合权重并汇总 Walk-forward 的样本外结果。
5. 保存候选配置、时间窗口、训练标签、拟合权重、选择指标、入选方案和测试结果，保证研究结论可复现。

本功能是研究评估器，当前实现的是可解释的线性因子权重训练。候选方案必须在 TOML 中显式列出；候选配置定义因子集合和策略结构，训练集拟合该集合内的权重，验证集用于候选选择，测试集锁定后只运行入选方案。有限的外层参数网格搜索由 [`workspace/design_factor_hyperparameter_training.md`](design_factor_hyperparameter_training.md) 定义，未启用该区段时保持本设计的单候选训练行为。

## 2. 配置协议

新增 `evaluate-factor-experiments` 命令，读取一个研究配置文件。配置包含候选回测 TOML、固定三段切分和可选滚动窗口：

```toml
[experiment]
name = "factor-experiment-walk-forward"
selection_metric = "sharpe_ratio"
selection_direction = "max"
candidate_configs = [
  "factor_experiment_reversal.toml",
  "factor_experiment_value_growth.toml",
]

[training]
enabled = true
label_horizon_days = 20
ridge_alpha = 0.1
max_iterations = 5000
minimum_training_observations = 200
minimum_training_dates = 24

[validity]
enabled = true
minimum_training_signal_dates = 24
minimum_validation_signal_dates = 11
minimum_test_signal_dates = 11
minimum_validation_observations = 100
minimum_test_observations = 100

[split]
train_start_date = "2020-01-01"
train_end_date = "2022-12-31"
validation_start_date = "2023-01-01"
validation_end_date = "2024-12-31"
test_start_date = "2025-01-01"
test_end_date = "2026-08-07"

[walk_forward]
enabled = true
start_date = "2018-01-01"
end_date = "2026-08-07"
train_years = 3
validation_years = 1
test_years = 1
step_years = 1
```

候选路径相对于研究配置文件所在目录解析。候选 TOML 中的 `[run]` 日期只作为基础配置，执行每个窗口时由研究评估器覆盖；手续费、滑点、基准和策略参数保持候选文件中的定义。

启用训练后，候选必须是 `factor-composite-experiment`。训练样本取训练窗口内的月末信号日：输入为各选中因子按方向缩尾后的截面百分位排名，标签为信号日后下一个交易日开盘至 `label_horizon_days` 个交易观察后的收盘收益。训练窗口末尾会额外加载标签所需的未来行情，但这些行情只用于训练标签，不进入训练回测的信号或收益统计。

固定切分和 Walk-forward 可以同时启用。Walk-forward 使用固定长度的滚动训练窗：每次将训练、验证和测试窗口整体向前移动 `step_years` 年，只保留完整落在总日期范围内的窗口。

## 3. 选择规则

- 训练集对全部候选方案分别拟合权重并运行拟合后的样本内回测；验证集使用各自训练出的冻结权重运行全部候选方案。
- 如果某个候选在当前窗口因训练样本不足、没有足够信号日或权重全部退化为零而训练失败，记录失败原因并排除该候选；不使用未训练的默认权重替代。只有全部候选都无法训练时，当前窗口才失败。
- `[validity]` 是研究有效性硬门禁：训练默认至少需要 24 个有效信号日，验证和测试默认各需要至少 11 个实际可执行信号及 100 个目标观测；一年月频窗口最后一个月末信号没有区间内 T+1 执行日，因此门禁按实际可执行信号计数。候选或窗口低于门槛时记录实际值、阈值和失败原因，并阻断对应阶段的结论。
- 每个窗口只按验证集的 `selection_metric` 选择一个候选方案；默认方向为最大化夏普比率。
- 指标相同按候选配置在 TOML 中的先后顺序确定，保证结果稳定。
- 测试集只运行入选方案；未入选方案不会读取测试区间，不允许用测试结果反向调参。
- 支持的选择指标为 `total_return`、`annualized_return`、`annualized_volatility`、`sharpe_ratio` 和 `max_drawdown`；波动率可以指定 `selection_direction = "min"`，其他指标通常使用 `max`。
- 因子集合、标签期限、正则强度和结构参数仍属于人工设定的研究假设；拟合权重只在当前训练窗口生效，不把一次测试结果写回候选配置。
- 未启用外层搜索时，标签期限、正则强度和结构参数属于人工设定；启用外层搜索时，正则强度和部分结构参数从显式有限网格中选择，标签期限和数据质量门槛仍固定。

权重训练使用按信号日去均值后的因子排名和收益，避免市场整体涨跌直接决定因子权重。Ridge 目标包含 L2 正则项，权重使用投影梯度法约束为非负并归一化为权重和 1；如果训练数据不足、没有足够信号日或所有权重均退化为零，该候选在当前窗口直接失败，不使用未训练的默认权重替代。

每个回测窗口继续沿用现有点时数据、信号日收盘生成信号、T+1 开盘成交和交易成本口径。财务数据的 ASOF 日期不会因切分而改变；加载训练窗口前的历史行情只用于满足因子滚动窗口，不进入该窗口的收益统计。

## 4. 输出协议

评估结果写入 `workspace/backtest/evaluations/`，包含：

| 文件 | 内容 |
|:---|:---|
| `parameters.json` | 研究配置、候选路径、切分窗口、选择指标、代码版本、候选配置哈希、因子版本和搜索参数覆盖信息 |
| `training_models.csv` | 每个窗口和候选的训练状态、失败原因（如有）、训练观测数、信号日数、迭代状态和拟合权重 |
| `hyperparameter_trials.csv` | 启用外层搜索时，每个参数组合的搜索维度、训练状态、拟合权重、训练/验证指标和失败原因 |
| `candidate_metrics.csv` | 训练/验证的全部候选指标，以及测试阶段入选候选的指标 |
| `selections.csv` | 每个固定或滚动窗口的入选候选、验证得分和测试得分 |
| `evaluation_failures.csv` | 训练、验证、测试回测或指标计算失败的窗口、候选、试验和原因 |
| `research_validity.csv` | 各窗口、候选和阶段的目标观测数、信号日数、门槛、通过状态和失败原因 |
| `summary.md` | 固定章节标准结果报告：执行状态、点时样本覆盖、训练状态、失败原因、入选权重、训练/验证/测试表现、Walk-forward 稳定性和研究边界 |

不保存每个候选的完整日净值和交易明细，避免一次参数实验产生大量重复产物；需要深入检查的入选方案可使用其原始回测 TOML 单独运行 `run-backtest`。

## 5. 未来函数与统计边界

1. 不随机打乱日期，不在训练集之后使用验证集或测试集数据拟合权重。
2. 20 日等有重叠持有期的研究若扩展到因子诊断，切分边界需要增加至少一个最长持有期的隔离区；本回测评估器的收益在每个独立窗口内重新从现金开始，不跨窗口传递持仓。
3. 训练、验证和测试均使用数据湖中截至对应信号日可见的财务字段；禁止用当前最新财务记录回填历史窗口。
4. 股票池、退市清算、停牌缺失、滑点和费用沿用 `DailyBacktestEngine`，不能为提高测试结果而改变。
5. Walk-forward 的汇总只统计各窗口测试段，不把训练或验证收益拼入样本外曲线；每个测试段使用其前方训练窗口独立拟合的权重。

## 6. 验收标准

- 配置解析拒绝日期重叠、空候选、非法选择指标和不完整 Walk-forward 参数。
- 研究有效性检查拒绝训练、验证或测试阶段的不足样本，并在 `research_validity.csv` 和 `summary.md` 中保留实际值与阈值。
- 单元测试验证窗口生成、候选选择、测试集只执行入选候选及输出文件。
- 既有 `run-backtest` 结果生成路径和三个正式策略行为不变。
- 使用真实点时数据至少完成一个固定切分和一个 Walk-forward 烟测。
- 全量 Ruff 和 `pytest tests/` 通过后，才可进入提交前 Review。
