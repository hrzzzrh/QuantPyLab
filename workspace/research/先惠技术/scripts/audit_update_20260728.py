"""先惠技术 2026-07-28 跟踪审计数据抽取。"""
from pathlib import Path

import pandas as pd
import pypdf

from storage.database.manager import db_manager


SYMBOL = "688155"
ROOT = Path("workspace/research/先惠技术")
TMP_DIR = ROOT / "tmp_data"
REPORT_DIR = ROOT / "financial_reports"


def export_dataframe(df: pd.DataFrame, filename: str) -> None:
    output_path = TMP_DIR / filename
    df.to_csv(output_path, index=False)
    print(f"\n已导出: {output_path}")
    print(df.to_string(index=False))


def extract_pdf_text(pdf_path: Path, output_path: Path) -> None:
    with pdf_path.open("rb") as source:
        reader = pypdf.PdfReader(source)
        with output_path.open("w", encoding="utf-8") as target:
            for page_number, page in enumerate(reader.pages, start=1):
                text = page.extract_text() or ""
                target.write(f"\n=== PAGE {page_number} ===\n")
                target.write(text)
        print(f"\n已抽取财报文本: {output_path}，页数: {len(reader.pages)}")


def query_market_and_financial_data() -> None:
    conn = db_manager.get_duckdb_conn()

    latest = conn.execute(
        f"""
        SELECT
            v.date,
            v.raw_close,
            v.close_hfq,
            k.open,
            k.high,
            k.low,
            k.volume,
            k.amount,
            v.market_cap / 1e8 AS market_cap_100m,
            v.pe_ttm,
            v.pb,
            v.ps_ttm,
            v.pcf_ttm,
            v.total_shares / 1e8 AS total_shares_100m
        FROM v_daily_valuation v
        JOIN daily_kline k
          ON v.symbol = k.symbol
         AND v.date = k.date
        WHERE v.symbol = '{SYMBOL}'
        ORDER BY v.date DESC
        LIMIT 30
        """
    ).df()
    export_dataframe(latest, "audit_20260728_recent_30_trading_days.csv")

    close_series = latest.sort_values("date").copy()
    close_series["return_from_20260624"] = (
        close_series["raw_close"] / 81.83 - 1
    )
    export_dataframe(close_series.tail(15), "audit_20260728_recent_15_trading_days.csv")

    moving_average = conn.execute(
        f"""
        WITH daily AS (
            SELECT
                v.date,
                v.raw_close,
                v.close_hfq,
                AVG(v.raw_close) OVER (
                    ORDER BY v.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS ma20_raw,
                AVG(v.raw_close) OVER (
                    ORDER BY v.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS ma60_raw,
                AVG(v.raw_close) OVER (
                    ORDER BY v.date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW
                ) AS ma120_raw,
                AVG(v.close_hfq) OVER (
                    ORDER BY v.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS ma20_hfq,
                AVG(v.close_hfq) OVER (
                    ORDER BY v.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS ma60_hfq,
                AVG(v.close_hfq) OVER (
                    ORDER BY v.date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW
                ) AS ma120_hfq
            FROM v_daily_valuation v
            WHERE v.symbol = '{SYMBOL}'
        )
        SELECT *
        FROM daily
        ORDER BY date DESC
        LIMIT 20
        """
    ).df()
    export_dataframe(moving_average, "audit_20260728_moving_average.csv")

    drawdown = conn.execute(
        f"""
        SELECT
            MIN(k.low) AS min_low_since_20260624,
            MIN(v.raw_close) AS min_close_since_20260624,
            MAX(v.raw_close) AS max_close_since_20260624,
            (MIN(v.raw_close) / 81.83 - 1) AS max_close_drawdown_from_20260624,
            (MIN(k.low) / 81.83 - 1) AS max_intraday_drawdown_from_20260624
        FROM v_daily_valuation v
        JOIN daily_kline k
          ON v.symbol = k.symbol
         AND v.date = k.date
        WHERE v.symbol = '{SYMBOL}'
          AND v.date >= DATE '2026-06-24'
        """
    ).df()
    export_dataframe(drawdown, "audit_20260728_drawdown_since_20260624.csv")

    valuation_stats = conn.execute(
        f"""
        SELECT
            MIN(pe_ttm) AS pe_min_1y,
            PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY pe_ttm) AS pe_p10_1y,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pe_ttm) AS pe_p25_1y,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY pe_ttm) AS pe_p50_1y,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pe_ttm) AS pe_p75_1y,
            MAX(pe_ttm) AS pe_max_1y,
            MIN(pb) AS pb_min_1y,
            PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY pb) AS pb_p10_1y,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pb) AS pb_p25_1y,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY pb) AS pb_p50_1y,
            COUNT(*) AS trading_days
        FROM v_daily_valuation
        WHERE symbol = '{SYMBOL}'
          AND date >= DATE '2025-07-28'
          AND pe_ttm > 0
          AND pb > 0
        """
    ).df()
    export_dataframe(valuation_stats, "audit_20260728_valuation_stats.csv")

    ttm = conn.execute(
        f"""
        SELECT
            report_date,
            pub_date,
            revenue_ttm / 1e8 AS revenue_ttm_100m,
            net_profit_ttm / 1e8 AS net_profit_ttm_100m,
            deduct_net_profit_ttm / 1e8 AS deduct_net_profit_ttm_100m,
            ocf_ttm / 1e8 AS ocf_ttm_100m
        FROM fin_ttm
        WHERE symbol = '{SYMBOL}'
        ORDER BY report_date DESC
        LIMIT 8
        """
    ).df()
    export_dataframe(ttm, "audit_20260728_ttm.csv")

    quarter_metrics = conn.execute(
        f"""
        SELECT
            i.report_date,
            i."营业总收入" / 1e8 AS revenue_100m,
            i."归属净利润" / 1e8 AS net_profit_parent_100m,
            i."扣非净利润" / 1e8 AS deduct_net_profit_100m,
            i."营业总收入同比增长" AS revenue_yoy,
            i."归属净利润同比增长" AS net_profit_yoy,
            i."毛利率" AS gross_margin,
            i."净利率" AS net_margin,
            i."资产负债率" AS debt_ratio,
            i."每股经营现金流" AS ocf_per_share
        FROM fin_indicator i
        WHERE i.symbol = '{SYMBOL}'
        ORDER BY i.report_date DESC
        LIMIT 8
        """
    ).df()
    export_dataframe(quarter_metrics, "audit_20260728_quarter_metrics.csv")


def main() -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    query_market_and_financial_data()
    q1_pdf = REPORT_DIR / "688155_上海先惠自动化技术股份有限公司2026年第一季度报告.pdf"
    extract_pdf_text(q1_pdf, TMP_DIR / "audit_20260728_q1_2026.txt")


if __name__ == "__main__":
    try:
        main()
    finally:
        db_manager.close_all()
