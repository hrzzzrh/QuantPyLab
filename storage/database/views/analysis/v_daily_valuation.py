from storage.database.view_base import DuckDBView

class DailyValuationView(DuckDBView):
    name = "v_daily_valuation"
    dependencies = ["daily_kline", "share_capital", "fin_ttm", "fin_balance_sheet"]

    def get_sql(self, warehouse_dir: str) -> str:
        return f"""
            CREATE OR REPLACE VIEW {self.name} AS
            WITH 
            -- 1. 准备基础行情
            base_kline AS (
                SELECT symbol, CAST(date AS DATE) as date, close, adj_factor 
                FROM daily_kline
            ),
            -- 2. 准备股本历史
            capital_hist AS (
                SELECT symbol, CAST(change_date AS DATE) as change_date, total_shares 
                FROM share_capital
            ),
            -- 3. 准备财务 TTM 历史
            ttm_hist AS (
                SELECT symbol, strptime(pub_date, '%Y%m%d')::DATE as pub_date, net_profit_ttm, deduct_net_profit_ttm, revenue_ttm, ocf_ttm
                FROM fin_ttm
            ),
            -- 4. 准备净资产历史
            assets_hist AS (
                SELECT 
                    symbol, 
                    CASE 
                        WHEN length(公告日期) = 8 THEN strptime(公告日期, '%Y%m%d')::DATE
                        ELSE CAST(LEFT(公告日期, 10) AS DATE)
                    END as pub_date,
                    "归属于母公司股东权益合计" as net_assets
                FROM fin_balance_sheet
            )

            SELECT 
                k.date,
                k.symbol,
                k.close AS raw_close,
                (k.close * k.adj_factor) AS close_hfq,
                s.total_shares,
                (k.close * s.total_shares) AS market_cap,
                (k.close * s.total_shares) / NULLIF(t.net_profit_ttm, 0) AS pe_ttm,
                (k.close * s.total_shares) / NULLIF(t.deduct_net_profit_ttm, 0) AS pe_deduct_ttm,
                (k.close * s.total_shares) / NULLIF(a.net_assets, 0) AS pb,
                (k.close * s.total_shares) / NULLIF(t.revenue_ttm, 0) AS ps_ttm,
                (k.close * s.total_shares) / NULLIF(t.ocf_ttm, 0) AS pcf_ttm

            FROM base_kline k
            ASOF JOIN capital_hist s 
                ON k.symbol = s.symbol AND k.date >= s.change_date
            ASOF JOIN ttm_hist t 
                ON k.symbol = t.symbol AND k.date >= t.pub_date
            ASOF JOIN assets_hist a
                ON k.symbol = a.symbol AND k.date >= a.pub_date;
        """
