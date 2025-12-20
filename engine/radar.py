# FILE PATH: engine/radar.py
import pandas as pd
from sqlalchemy import text, func
from database.models import SessionLocal, StockBasic, DWSMarketIndicators, DWSFinanceStd

class RadarEngine:
    def __init__(self):
        self.db = SessionLocal()

    def query(self, 
              min_roe=8.0,           # 核心：ROE 扣非
              max_pe=30.0,           # 估值：PE(TTM)
              max_pb=3.0,            # 估值：PB
              min_mv=100.0,          # 规模：总市值(亿)
              max_debt=60.0,         # 风险：负债率
              trend_up=True,         # 趋势：收盘 > MA20
              pool='CSI800'          # 范围：CSI800 / Watchlist / All
              ):
        """
        [PRD 3.2] 选股雷达核心筛选逻辑
        """
        # 1. 获取数据库最新行情日期 (T-1 后视镜)
        latest_date = self.db.query(func.max(DWSMarketIndicators.trade_date)).scalar()
        if not latest_date:
            return pd.DataFrame()

        # 2. 构建三表关联查询 (SQL 模式以提升性能)
        # 关联: Basic(B) -> Indicators(I) -> Finance(F)
        # 注意: Finance 取最近一个报告期 (end_date)
        # --- 架构级修复：联接 ODS 获取原始涨跌幅 ---
        sql = text("""
            SELECT 
                b.ts_code, b.name, b.industry,
                i.trade_date, i.close_qfq, i.pe_ttm, i.pb, i.total_mv, i.ma_20,
                m.pct_chg,
                f.end_date as last_report, 
                COALESCE(f.roe, 0) as roe, -- 这里暂时取 f.roe，我们将修改 process_finance_dws 来填充它
                f.debt_to_assets,
                -- V7.4 侦探指标计算 
                COALESCE(f.n_cashflow_act / NULLIF(f.n_income_attr_p, 0), 0) as ocf_to_net_profit,
                -- 垃圾资产比: (其他应收+预付) / 总资产
                COALESCE((f.oth_receiv + f.prepayment) / NULLIF(f.total_assets, 0), 0) as toxic_asset_ratio,
                -- 商誉占比: 商誉 / 归母净资产
                COALESCE(f.goodwill / NULLIF(f.total_hldr_eqy_exc_min_int, 0), 0) as goodwill_net_asset_ratio
            FROM stock_basic b
            JOIN dws_market_indicators i ON b.ts_code = i.ts_code
            JOIN ods_market_daily m ON i.ts_code = m.ts_code AND i.trade_date = m.trade_date
            LEFT JOIN (
                SELECT DISTINCT ON (ts_code) *
                FROM dws_finance_std
                ORDER BY ts_code, end_date DESC
            ) f ON b.ts_code = f.ts_code
            WHERE i.trade_date = :t_date
            AND (:pool = 'All' OR 
                (:pool = 'CSI800' AND b.is_csi800 = True) OR
                (:pool = 'Watchlist' AND b.ts_code IN (SELECT ts_code FROM watchlist))
            )
        """)

        df = pd.read_sql(sql, self.db.bind, params={"t_date": latest_date, "pool": pool})
        if df.empty: return df

        # --- 架构级修复：处理空值防止误杀 ---
        # 将 ROE 缺失填充为 0，负债率缺失填充为 0 (代表风险未知但不拦截)
        df['roe'] = df['roe'].fillna(0)
        df['debt_to_assets'] = df['debt_to_assets'].fillna(0)
        df['pe_ttm'] = df['pe_ttm'].fillna(999) # PE 缺失则设为极大值拦截
        df['ocf_to_net_profit'] = df['ocf_to_net_profit'].fillna(0)
        df['toxic_asset_ratio'] = df['toxic_asset_ratio'].fillna(0)
        df['goodwill_net_asset_ratio'] = df['goodwill_net_asset_ratio'].fillna(0)

        # 3. 执行多因子过滤
        mask = (
            (df['pe_ttm'] > 0) & (df['pe_ttm'] < max_pe) &
            (df['pb'] < max_pb) &
            (df['total_mv'] >= min_mv * 10000) & # 亿元转万元
            (df['roe'] >= min_roe) &
            (df['debt_to_assets'] <= max_debt) &
            (df['ocf_to_net_profit'] >= 0.8) & 
            (df['toxic_asset_ratio'] < 0.05) & 
            (df['goodwill_net_asset_ratio'] < 0.25)
        )
        
        if trend_up:
            mask = mask & (df['close_qfq'] > df['ma_20'])

        result = df[mask].copy()

        # --- V7.4 动态理由生成 ---
        def generate_reason(row):
            reasons = []
            # 亮点挖掘
            if row['roe'] >= 20: reasons.append("高ROE(>20%)")
            if row['ocf_to_net_profit'] >= 1.2: reasons.append("现金含量极高")
            
            # 风险提示
            if row['goodwill_net_asset_ratio'] > 0.2: reasons.append("⚠商誉偏高")
            if row['toxic_asset_ratio'] > 0.04: reasons.append("⚠资产成色一般")
            
            return " | ".join(reasons) if reasons else "多因子均衡"

        result['selection_reason'] = result.apply(generate_reason, axis=1)
        
        # 格式化输出
        result['total_mv_unit'] = (result['total_mv'] / 10000).round(2) # 转回亿元显示
        
        # 统一保留两位小数
        numeric_cols = result.select_dtypes(include=['number']).columns
        result[numeric_cols] = result[numeric_cols].round(2)

        return result.sort_values('roe', ascending=False)

    def close(self):
        self.db.close()