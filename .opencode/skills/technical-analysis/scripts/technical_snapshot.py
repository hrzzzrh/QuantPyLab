"""
技术面统计快照生成器 (Technical Snapshot Generator)
用于深度研报模块七技术面分析，输出标准化统计快照。

口径铁律：
- 趋势指标（MA/RSI/MACD/布林带）一律使用后复权价 (v_daily_valuation.close_hfq)
- 不复权价 (daily_kline.close) 仅用于定价标注与市值计算
- 后复权点位必须折算标注当前对应不复权市价

用法:
    PYTHONPATH=. uv run python .opencode/skills/technical-analysis/scripts/technical_snapshot.py <symbol> --output <path.md>
"""

import argparse
import sys
from datetime import date

sys.path.insert(0, "/Volumes/wdblack/some_project/QuantPyLab")

from storage.database.manager import db_manager


def compute_rsi_wilder(closes, period=14):
    """Wilder 平滑 RSI。closes 为后复权收盘价序列（升序）。"""
    if len(closes) <= period:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def compute_ema(values, period):
    """标准 EMA。values 为升序序列。"""
    if not values:
        return []
    k = 2.0 / (period + 1)
    ema = [values[0]]
    for v in values[1:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema


def main():
    parser = argparse.ArgumentParser(description="技术面统计快照生成器")
    parser.add_argument("symbol", help="6位股票代码")
    parser.add_argument("--output", required=True, help="快照输出 Markdown 路径")
    args = parser.parse_args()
    symbol = args.symbol

    try:
        conn = db_manager.get_duckdb_conn()

        # 基础信息：最近一日
        base = conn.execute(
            f"""
            SELECT v.date, v.raw_close, v.close_hfq, v.market_cap, k.adj_factor, v.total_shares
            FROM v_daily_valuation v
            JOIN daily_kline k ON k.symbol = v.symbol AND k.date = v.date
            WHERE v.symbol = '{symbol}'
            ORDER BY v.date DESC LIMIT 1
            """
        ).df()
        if base.empty:
            print(f"ERROR: 无 {symbol} 的行情数据")
            sys.exit(1)
        row = base.iloc[0]
        last_date = row["date"]
        raw_close = float(row["raw_close"])
        close_hfq = float(row["close_hfq"])
        adj_factor = float(row["adj_factor"])
        mkt_cap = float(row["market_cap"])
        total_shares = float(row["total_shares"])

        # 全量日线（升序）用于均线/指标
        hist = conn.execute(
            f"""
            SELECT v.date, v.raw_close, v.close_hfq, k.volume, k.amount
            FROM v_daily_valuation v
            JOIN daily_kline k ON k.symbol = v.symbol AND k.date = v.date
            WHERE v.symbol = '{symbol}'
            ORDER BY v.date
            """
        ).df()
        n = len(hist)
        closes_hfq = hist["close_hfq"].tolist()

        def ma(period):
            if n < period:
                return None
            return float(closes_hfq[-period:][0] and sum(closes_hfq[-period:]) / period)

        ma20, ma60, ma120, ma250 = ma(20), ma(60), ma(120), ma(250)

        def conv(hfq_val):
            """后复权价 → 当前不复权折算价（近似，供标注用）"""
            if hfq_val is None:
                return None
            return hfq_val / adj_factor * (raw_close / (close_hfq / adj_factor))

        # 历史极值
        ath_row = hist.loc[hist["close_hfq"].idxmax()]
        hist_52w = hist[hist["date"] >= hist["date"].max() - __import__("pandas").Timedelta(days=365)]
        hi52 = hist_52w.loc[hist_52w["close_hfq"].idxmax()]
        lo52 = hist_52w.loc[hist_52w["close_hfq"].idxmin()]

        # 年度表现（不复权，标注除权）
        hist["year"] = hist["date"].dt.year
        yearly = []
        for yr, grp in hist.groupby("year"):
            grp = grp.sort_values("date")
            yearly.append(
                f"| {yr} | {grp['raw_close'].iloc[0]:.2f} | {grp['raw_close'].iloc[-1]:.2f} "
                f"| {grp['raw_close'].min():.2f} | {grp['raw_close'].max():.2f} "
                f"| {(grp['raw_close'].iloc[-1] / grp['raw_close'].iloc[0] - 1) * 100:+.1f}% |"
            )

        # 动能指标
        rsi14 = compute_rsi_wilder(closes_hfq, 14)
        ema12, ema26 = compute_ema(closes_hfq, 12), compute_ema(closes_hfq, 26)
        dif = ema12[-1] - ema26[-1]
        dea = compute_ema([a - b for a, b in zip(ema12, ema26)], 9)[-1]
        macd_hist = (dif - dea) * 2
        bb_mid = ma20
        if n >= 20:
            std20 = float(closes_hfq[-20:][0] and (sum((x - bb_mid) ** 2 for x in closes_hfq[-20:]) / 20) ** 0.5)
            bb_up, bb_low = bb_mid + 2 * std20, bb_mid - 2 * std20
        else:
            bb_up = bb_low = None

        # 量价特征（近20日）
        last20 = hist.tail(20)
        vol_max = last20.loc[last20["volume"].idxmax()]
        vol_min = last20.loc[last20["volume"].idxmin()]
        avg_vol = float(last20["volume"].mean())
        today_vol = float(last20["volume"].iloc[-1])

        # 近10日行情明细
        detail_rows = []
        for _, r in hist.tail(10).iloc[::-1].iterrows():
            detail_rows.append(
                f"| {r['date'].strftime('%Y-%m-%d')} | {r['raw_close']:.2f} | {r['close_hfq']:.2f} "
                f"| {r['volume']:.0f} | {r['amount'] / 1e4:.0f} |"
            )

        # 均线排列状态
        vals = [(v, l) for v, l in [(ma20, "MA20"), (ma60, "MA60"), (ma120, "MA120"), (ma250, "MA250")] if v is not None]
        if all(v > close_hfq for v, _ in vals):
            state = "空头排列（全部均线位于价格上方）"
        elif all(v < close_hfq for v, _ in vals):
            state = "多头排列（全部均线位于价格下方）"
        else:
            state = "均线纠缠（多空交织）"

        # 组装输出
        lines = []
        lines.append(f"# 技术面统计快照：{symbol}")
        lines.append(f"\n> 生成日期：{date.today().strftime('%Y年%m月%d日')} | 数据截止：{last_date.strftime('%Y-%m-%d')}")
        lines.append(f"> 统计口径：趋势指标均为后复权价，不复权价仅用于标注")
        lines.append("")
        lines.append("## 一、基础信息")
        lines.append("")
        lines.append(f"| 项目 | 数值 |")
        lines.append(f"|:---|:---:|")
        lines.append(f"| 不复权收盘价 | {raw_close:.2f} 元 |")
        lines.append(f"| 后复权收盘价 | {close_hfq:.2f} 元 |")
        lines.append(f"| 复权因子 | {adj_factor:.4f} |")
        lines.append(f"| 总股本 | {total_shares / 1e4:.2f} 万股 |")
        lines.append(f"| 总市值 | {mkt_cap / 1e8:.2f} 亿元 |")
        lines.append(f"| 均线状态 | {state} |")
        lines.append("")
        lines.append("## 二、均线系统（后复权）")
        lines.append("")
        lines.append("| 均线 | 后复权值 | 折算当前不复权（约） | 相对当前价 |")
        lines.append("|:---|:---:|:---:|:---:|")
        for v, l in vals:
            conv_v = conv(v)
            pct = (v / close_hfq - 1) * 100
            lines.append(f"| {l} | {v:.2f} 元 | {conv_v:.2f} 元 | {pct:+.1f}% |")
        lines.append("")
        lines.append("## 三、历史极值")
        lines.append("")
        lines.append(f"| 项目 | 日期 | 后复权价 | 折算不复权（约） |")
        lines.append(f"|:---|:---:|:---:|:---:|")
        lines.append(f"| 历史最高 (ATH) | {ath_row['date'].strftime('%Y-%m-%d')} | {ath_row['close_hfq']:.2f} 元 | {conv(ath_row['close_hfq']):.2f} 元 |")
        lines.append(f"| 52周最高 | {hi52['date'].strftime('%Y-%m-%d')} | {hi52['close_hfq']:.2f} 元 | {conv(hi52['close_hfq']):.2f} 元 |")
        lines.append(f"| 52周最低 | {lo52['date'].strftime('%Y-%m-%d')} | {lo52['close_hfq']:.2f} 元 | {conv(lo52['close_hfq']):.2f} 元 |")
        lines.append("")
        lines.append("## 四、年度表现（不复权）")
        lines.append("")
        lines.append("| 年份 | 首日收盘 | 末日收盘 | 最低 | 最高 | 年度涨跌 |")
        lines.append("|:---|:---:|:---:|:---:|:---:|:---:|")
        lines.extend(yearly)
        lines.append("")
        lines.append("## 五、动能指标（后复权）")
        lines.append("")
        lines.append(f"| 指标 | 数值 |")
        lines.append(f"|:---|:---:|")
        lines.append(f"| RSI(14) | {rsi14:.1f} |" if rsi14 is not None else "| RSI(14) | 数据不足 |")
        lines.append(f"| MACD DIF | {dif:.3f} |")
        lines.append(f"| MACD DEA | {dea:.3f} |")
        lines.append(f"| MACD 柱 | {macd_hist:.3f} |")
        lines.append(f"| 布林上轨(20,2) | {bb_up:.2f} |" if bb_up else "| 布林上轨 | 数据不足 |")
        lines.append(f"| 布林中轨(20) | {bb_mid:.2f} |" if bb_mid else "| 布林中轨 | 数据不足 |")
        lines.append(f"| 布林下轨(20,2) | {bb_low:.2f} |" if bb_low else "| 布林下轨 | 数据不足 |")
        lines.append("")
        lines.append("## 六、量价特征（近20日）")
        lines.append("")
        lines.append(f"| 项目 | 数值 |")
        lines.append(f"|:---|:---:|")
        lines.append(f"| 今日成交量 | {today_vol:.0f} 手 |")
        lines.append(f"| 20日均量 | {avg_vol:.0f} 手 |")
        lines.append(f"| 放量倍数（今/均） | {today_vol / avg_vol:.2f}x |" if avg_vol else "| 放量倍数 | - |")
        lines.append(f"| 区间最大量 | {vol_max['volume']:.0f} 手（{vol_max['date'].strftime('%Y-%m-%d')}，收盘 {vol_max['close_hfq']:.2f} 后复权） |")
        lines.append(f"| 区间最小量（地量） | {vol_min['volume']:.0f} 手（{vol_min['date'].strftime('%Y-%m-%d')}，收盘 {vol_min['close_hfq']:.2f} 后复权） |")
        lines.append("")
        lines.append("## 七、近10个交易日行情")
        lines.append("")
        lines.append("| 日期 | 不复权收盘 | 后复权收盘 | 成交量(手) | 成交额(万) |")
        lines.append("|:---|:---:|:---:|:---:|:---:|")
        lines.extend(detail_rows)
        lines.append("")

        with open(args.output, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        print(f"OK: 快照已写入 {args.output}")

        # 汇报摘要
        print(f"当前价(不复权): {raw_close:.2f} | 后复权: {close_hfq:.2f} | 复权因子: {adj_factor:.4f}")
        print(f"均线状态: {state}")
        print(f"RSI14: {rsi14:.1f}" if rsi14 is not None else "RSI14: 数据不足")
        print(f"ATH: {ath_row['date'].strftime('%Y-%m-%d')} {ath_row['close_hfq']:.2f}（后复权）")
        print(f"52周区间: {lo52['close_hfq']:.2f} ~ {hi52['close_hfq']:.2f}（后复权）")

    finally:
        db_manager.close_all()


if __name__ == "__main__":
    main()
