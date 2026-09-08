# 股东人数表 (holder_number)

记录个股股东户数历史，用于筹码集中度分析（户数拐点、户均持股）。

## 1. 字段定义

| 序号 | 字段名 | 类型 | 说明 | 样例值 |
| :--- | :--- | :--- | :--- | :--- |
| 1 | `stat_date` | DATE | 股东户数统计截止日（去重键；展示用标签日期，**禁止**用于点时关联） | 2026-06-30 |
| 2 | `holder_count` | BIGINT | 本次股东户数（户） | 296404 |
| 3 | `prev_holder_count` | BIGINT | 上次股东户数（来源自带存证；未知时为 NULL，**禁止**按 0 解读） | 243159 |
| 4 | `holder_change` | BIGINT | 户数增减（未知时为 NULL，**禁止**按“不变”解读） | 53245 |
| 5 | `holder_change_pct` | DOUBLE | 户数增减比例（%） | 21.897195 |
| 6 | `announce_date` | DATE | 公告日期（**ASOF 关联键**，缺失的行入库时丢弃；`cninfo` 行取法定披露截止日伪值，方向保守只偏旧） | 2026-08-15 |
| 7 | `source` | VARCHAR | 数据来源：`em`（东财）/ `cninfo`（巨潮 fallback） | em |
| 8 | `symbol` | VARCHAR | 股票代码 (纯数字，如 600519)。物理文件位于 `symbol={code}` 分区目录，视图层通过文件名提取 | 600519 |

## 2. 数据来源

- 主源：东方财富个股股东户数明细 (`ak.stock_zh_a_gdhs_detail_em`)，单股全量历史。
- Fallback：东财确定性缺数的股票（约 48 只，多为 A+B / A+H / CDR 结构，
  如 601899、000011，2026-09-08 首轮全量同步实测）回退到巨潮季度统计
  (`ak.stock_hold_num_cninfo`)，仅补本地缺失的季度（20170331 起），
  同一 `stat_date` 东财行优先。巨潮行 `source='cninfo'` 打标。
- 口径：东财对公司披露的二次整理；关键结论引用时应回巨潮公告原文核实。
  同一季末与巨潮专题统计存在约 0.03% 级别的口径差（如 002594 2025-06-30 期），
  系统只存东财单源口径，不混合拼接。
- 历史深度：2013 年起，单股约 60+ 行（含季末与月内自愿披露）。
- 披露滞后：约 1~1.5 个月（如 2026-06-30 期多在 2026-08-15~29 公告），
  增量以本地最大 `stat_date` 驱动，不以交易日历驱动。

## 3. 同步状态与覆盖局限

- 每只股票同步成功后在 `metadata.db` 的 `sync_status` 表记录当次同步日期
  (`dataset='holder_number'`)；批量默认跳过当日已同步，失败无记录、重跑自动补抓；
  不设失败台账，重跑即重试，单股定点补救用 `--symbol`。
  两源皆空不记成功但不计失败（空≠成功，下次重跑补抓；打错代码返回空目标会告警）。
- **存活者偏差**：源接口无退市股覆盖（退市股明细请求直接无返回），
  批量同步仅覆盖在市股（含 `is_active` 为 NULL 的历史行）；北交所在市股已覆盖。
  回测引用时必须声明该局限。

## 4. 查询口径（无穿越铁律）

关联键必须为 `k.date >= h.announce_date`，严禁按 `stat_date` 关联：

```sql
SELECT k.date, h.stat_date, h.holder_count
FROM daily_kline k
ASOF JOIN holder_number h
  ON k.symbol = h.symbol AND k.date >= h.announce_date
WHERE k.symbol = '600519'
ORDER BY k.date;
```
