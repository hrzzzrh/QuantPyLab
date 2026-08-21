from storage.database.view_base import DuckDBView


class DailyValuationView(DuckDBView):
    name = "v_daily_valuation"
    dependencies = ["daily_kline", "share_capital", "fin_ttm", "fin_balance_sheet"]

    def get_sql(self, warehouse_dir: str) -> str:
        return f"""
            CREATE OR REPLACE VIEW {self.name} AS
            WITH 
            -- 1. 准备基础行情
            base_kline AS MATERIALIZED (
                SELECT symbol, CAST(date AS DATE) as date, close, adj_factor 
                FROM daily_kline
            ),
            -- 2. 准备股本历史
            capital_source AS MATERIALIZED (
                SELECT symbol, CAST(change_date AS DATE) as change_date, total_shares 
                FROM share_capital
            ),
            capital_hist AS MATERIALIZED (
                SELECT symbol, change_date, MAX(total_shares) AS total_shares
                FROM capital_source
                WHERE change_date IS NOT NULL
                GROUP BY symbol, change_date
            ),
            -- 3. 准备财务 TTM 历史
            ttm_source AS MATERIALIZED (
                SELECT
                    symbol,
                    COALESCE(
                        try_strptime(LEFT(CAST(pub_date AS VARCHAR), 10), '%Y-%m-%d'),
                        try_strptime(LEFT(CAST(pub_date AS VARCHAR), 8), '%Y%m%d')
                    )::DATE AS pub_date,
                    report_date,
                    net_profit_ttm,
                    deduct_net_profit_ttm,
                    revenue_ttm,
                    ocf_ttm,
                    md5(concat_ws(
                        '|',
                        COALESCE(CAST(filename AS VARCHAR), '<NULL>'),
                        COALESCE(CAST(report_date AS VARCHAR), '<NULL>'),
                        COALESCE(CAST(net_profit_ttm AS VARCHAR), '<NULL>'),
                        COALESCE(CAST(deduct_net_profit_ttm AS VARCHAR), '<NULL>'),
                        COALESCE(CAST(revenue_ttm AS VARCHAR), '<NULL>'),
                        COALESCE(CAST(ocf_ttm AS VARCHAR), '<NULL>')
                    )) AS record_tie_breaker
                FROM fin_ttm
            ),
            keyed_ttm_source AS MATERIALIZED (
                SELECT *, concat_ws(
                    '|',
                    COALESCE(CAST(report_date AS VARCHAR), '<NULL>'),
                    record_tie_breaker
                ) AS selection_key
                FROM ttm_source
            ),
            selected_ttm_records AS MATERIALIZED (
                SELECT
                    symbol,
                    pub_date,
                    arg_max(
                        struct_pack(
                            net_profit_ttm := net_profit_ttm,
                            deduct_net_profit_ttm := deduct_net_profit_ttm,
                            revenue_ttm := revenue_ttm,
                            ocf_ttm := ocf_ttm
                        ),
                        selection_key
                    ) AS selected_record
                FROM keyed_ttm_source
                WHERE pub_date IS NOT NULL
                GROUP BY symbol, pub_date
            ),
            ttm_hist AS MATERIALIZED (
                SELECT
                    symbol,
                    pub_date,
                    selected_record.net_profit_ttm AS net_profit_ttm,
                    selected_record.deduct_net_profit_ttm AS deduct_net_profit_ttm,
                    selected_record.revenue_ttm AS revenue_ttm,
                    selected_record.ocf_ttm AS ocf_ttm
                FROM selected_ttm_records
            ),
            -- 4. 准备净资产历史
            assets_source AS MATERIALIZED (
                SELECT
                    symbol,
                    COALESCE(
                        CASE
                            WHEN length(数据可用日期) = 8 THEN strptime(数据可用日期, '%Y%m%d')::DATE
                            ELSE try_strptime(LEFT(数据可用日期, 10), '%Y-%m-%d')::DATE
                        END,
                        CASE
                            WHEN length(公告日期) = 8 THEN try_strptime(公告日期, '%Y%m%d')::DATE
                            ELSE try_strptime(LEFT(公告日期, 10), '%Y-%m-%d')::DATE
                        END
                    ) as pub_date,
                    report_date,
                    "归属于母公司股东权益合计" as net_assets,
                    md5(concat_ws(
                        '|',
                        COALESCE(CAST(filename AS VARCHAR), '<NULL>'),
                        COALESCE(CAST(report_date AS VARCHAR), '<NULL>'),
                        COALESCE(CAST("归属于母公司股东权益合计" AS VARCHAR), '<NULL>')
                    )) AS record_tie_breaker
                FROM fin_balance_sheet
            ),
            keyed_assets_source AS MATERIALIZED (
                SELECT *, concat_ws(
                    '|',
                    COALESCE(CAST(report_date AS VARCHAR), '<NULL>'),
                    record_tie_breaker
                ) AS selection_key
                FROM assets_source
            ),
            selected_assets_records AS MATERIALIZED (
                SELECT
                    symbol,
                    pub_date,
                    arg_max(
                        struct_pack(net_assets := net_assets),
                        selection_key
                    ) AS selected_record
                FROM keyed_assets_source
                WHERE pub_date IS NOT NULL
                GROUP BY symbol, pub_date
            ),
            assets_hist AS MATERIALIZED (
                SELECT
                    symbol,
                    pub_date,
                    selected_record.net_assets AS net_assets
                FROM selected_assets_records
            ),
            capital_intervals AS MATERIALIZED (
                -- 按唯一生效日构造半开区间，避免依赖 ASOF 右表的物理顺序。
                SELECT
                    symbol,
                    change_date,
                    LEAD(change_date) OVER (
                        PARTITION BY symbol ORDER BY change_date
                    ) AS next_change_date,
                    total_shares
                FROM capital_hist
            ),
            ttm_intervals AS MATERIALIZED (
                SELECT
                    symbol,
                    pub_date,
                    LEAD(pub_date) OVER (
                        PARTITION BY symbol ORDER BY pub_date
                    ) AS next_pub_date,
                    net_profit_ttm,
                    deduct_net_profit_ttm,
                    revenue_ttm,
                    ocf_ttm
                FROM ttm_hist
            ),
            assets_intervals AS MATERIALIZED (
                SELECT
                    symbol,
                    pub_date,
                    LEAD(pub_date) OVER (
                        PARTITION BY symbol ORDER BY pub_date
                    ) AS next_pub_date,
                    net_assets
                FROM assets_hist
            ),
            capital_valuation AS MATERIALIZED (
                SELECT k.symbol, k.date, k.close, k.adj_factor, s.total_shares
                FROM base_kline k
                INNER JOIN capital_intervals s
                    ON k.symbol = s.symbol
                   AND k.date >= s.change_date
                   AND (
                       k.date < s.next_change_date
                       OR s.next_change_date IS NULL
                   )
            ),
            ttm_valuation AS MATERIALIZED (
                SELECT k.*, t.net_profit_ttm, t.deduct_net_profit_ttm,
                       t.revenue_ttm, t.ocf_ttm
                FROM capital_valuation k
                INNER JOIN ttm_intervals t
                    ON k.symbol = t.symbol
                   AND k.date >= t.pub_date
                   AND (
                       k.date < t.next_pub_date
                       OR t.next_pub_date IS NULL
                   )
            ),
            assets_valuation AS MATERIALIZED (
                SELECT k.*, a.net_assets
                FROM ttm_valuation k
                INNER JOIN assets_intervals a
                    ON k.symbol = a.symbol
                   AND k.date >= a.pub_date
                   AND (
                       k.date < a.next_pub_date
                       OR a.next_pub_date IS NULL
                   )
            )
            SELECT
                date,
                symbol,
                close AS raw_close,
                (close * adj_factor) AS close_hfq,
                total_shares,
                (close * total_shares) AS market_cap,
                (close * total_shares) / NULLIF(net_profit_ttm, 0) AS pe_ttm,
                (close * total_shares) / NULLIF(deduct_net_profit_ttm, 0) AS pe_deduct_ttm,
                (close * total_shares) / NULLIF(net_assets, 0) AS pb,
                (close * total_shares) / NULLIF(revenue_ttm, 0) AS ps_ttm,
                (close * total_shares) / NULLIF(ocf_ttm, 0) AS pcf_ttm
            FROM assets_valuation;
        """
