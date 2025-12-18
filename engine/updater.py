import pandas as pd
import time
from datetime import datetime, timedelta
from sqlalchemy import text
from interface.tushare_client import ts_client
from database.models import (
    SessionLocal, StockBasic, Watchlist, 
    ODSMarketDaily, ODSAdjFactor, ODSFinanceReport, 
    DWSMarketIndicators, DWSFinanceStd
)
from core.mapping import SOURCE_TABLE_MAP

class DataUpdater:
    def __init__(self):
        self.db = SessionLocal()

    def close(self):
        self.db.close()

    def _get_universe_pool(self) -> set:
        """[PRD 1.2] 获取中证800+自选股的并集"""
        csi800 = self.db.query(StockBasic.ts_code).filter(StockBasic.is_csi800 == True).all()
        watchlist = self.db.query(Watchlist.ts_code).all()
        pool = {row.ts_code for row in csi800} | {row.ts_code for row in watchlist}
        return pool

    def sync_stock_list(self):
        """[PRD 3.1] 全量同步股票列表并标记中证800"""
        print("🔄 Syncing Stock List...")
        df_basics = ts_client.fetch_stock_basic()
        if df_basics.empty: return

        # Mark CSI 800
        try:
            now_str = datetime.now().strftime("%Y%m%d")
            df_csi800 = ts_client.pro.index_weight(index_code='000906.SH', start_date='20240101', end_date=now_str)
            
            if not df_csi800.empty:
                latest_date = df_csi800['trade_date'].max()
                df_latest = df_csi800[df_csi800['trade_date'] == latest_date]
                csi800_set = set(df_latest['con_code'].tolist())
            else:
                csi800_set = set()
        except Exception as e:
            print(f"⚠️ CSI800 Fetch Error: {e}")
            csi800_set = set()

        # Processing
        df_basics['is_csi800'] = False
        if csi800_set:
            df_basics.loc[df_basics['ts_code'].isin(csi800_set), 'is_csi800'] = True

        # Upsert Logic
        for _, row in df_basics.iterrows():
            stock = StockBasic(
                ts_code=row['ts_code'],
                symbol=row['symbol'],
                name=row['name'],
                area=row['area'],
                industry=row['industry'],
                market=row['market'],
                list_date=row['list_date'],
                is_csi800=row['is_csi800']
            )
            self.db.merge(stock)
        
        self.db.commit()
        print(f"✅ Stock List Synced. CSI800 Count: {len(csi800_set)}")

    # --- 场景 S1/S2/S5: 垂直历史回溯 (按代码同步) ---

    def sync_stock_history(self, ts_code: str, start_date="20150101"):
        """补全单只股票的所有历史数据 (行情+财报)"""
        # 1. 行情与复权因子 [cite: 15-16]
        df_daily = ts_client.fetch_daily(ts_code=ts_code, start_date=start_date)
        df_adj = ts_client.fetch_adj_factor(ts_code=ts_code, start_date=start_date)
        
        if not df_daily.empty:
            for _, row in df_daily.iterrows():
                self.db.merge(ODSMarketDaily(
                    ts_code=row['ts_code'], trade_date=row['trade_date'],
                    open=row['open'], high=row['high'], low=row['low'], close=row['close'],
                    pre_close=row['pre_close'], change=row['change'], pct_chg=row['pct_chg'],
                    vol=row['vol'], amount=row['amount']
                ))

        if not df_adj.empty:
            for _, row in df_adj.iterrows():
                self.db.merge(ODSAdjFactor(ts_code=row['ts_code'], trade_date=row['trade_date'], adj_factor=row['adj_factor']))

        # 2. 四大财报同步 (JSONB 存储) [cite: 90-95]
        tasks = {
            "income": ts_client.fetch_income,
            "balancesheet": ts_client.fetch_balancesheet,
            "cashflow": ts_client.fetch_cashflow,
            "fina_indicator": ts_client.fetch_fina_indicator
        }
        for category, api_func in tasks.items():
            df = api_func(ts_code=ts_code, start_date=start_date)
            if not df.empty:
                # --- 核心修复：内存预去重 ---
                # 定义我们的五维主键 (category 在循环中固定)
                pk_cols = ['ts_code', 'end_date', 'report_type', 'update_flag']
                # Tushare 接口返回的字段名可能略有不同，先做个安全检查
                actual_pk = [c for c in pk_cols if c in df.columns]
                # 根据主键去重，保留最后一条（通常是最新的）
                df = df.drop_duplicates(subset=actual_pk, keep='last')

                df = df.astype(object).where(pd.notnull(df), None)
                for record in df.to_dict('records'):
                    self.db.merge(ODSFinanceReport(
                        ts_code=record['ts_code'], end_date=record['end_date'],
                        report_type=record.get('report_type', '1'),
                        update_flag=record.get('update_flag', '0'),
                        category=category, data=record, ann_date=record.get('ann_date')
                    ))
                # 每一类报表提交一次，缩小冲突范围并提升调试效率
                self.db.commit()

    # --- 场景 S3: 水平每日行情 (按日期同步) ---

    def sync_daily_market(self, trade_date: str):
        """[PRD S3] 每日增量行情同步 (水平模式)"""
        universe = self._get_universe_pool()
        if not universe: return

        try:
            # 1. Fetch Full Market
            df_daily = ts_client.fetch_daily(trade_date=trade_date)
            df_adj = ts_client.fetch_adj_factor(trade_date=trade_date)
            
            if df_daily.empty: return

            # 2. Funnel Filter
            df_daily_filtered = df_daily[df_daily['ts_code'].isin(universe)]
            
            # 3. Save ODS
            for _, row in df_daily_filtered.iterrows():
                self.db.merge(ODSMarketDaily(
                    ts_code=row['ts_code'], trade_date=row['trade_date'],
                    open=row['open'], high=row['high'], low=row['low'], close=row['close'],
                    pre_close=row['pre_close'], change=row['change'], pct_chg=row['pct_chg'],
                    vol=row['vol'], amount=row['amount']
                ))

            if not df_adj.empty:
                df_adj_filtered = df_adj[df_adj['ts_code'].isin(universe)]
                for _, row in df_adj_filtered.iterrows():
                    self.db.merge(ODSAdjFactor(
                        ts_code=row['ts_code'], trade_date=row['trade_date'],
                        adj_factor=row['adj_factor']
                    ))

            self.db.commit()
            print(f"  ✅ Market Snapshot {trade_date}: Saved {len(df_daily_filtered)} records.")

        except Exception as e:
            self.db.rollback()
            print(f"  ❌ Market Snapshot Failed: {e}")

    # --- 场景 S4: 水平每日财报 (按公告日同步) ---

    def sync_financial_daily(self, ann_date: str):
        """[PRD S4] 每日增量财报同步 (水平模式)"""
        universe = self._get_universe_pool()
        
        tasks = {
            "income": (ts_client.fetch_income, "income"),
            "balancesheet": (ts_client.fetch_balancesheet, "balance"),
            "cashflow": (ts_client.fetch_cashflow, "cashflow"),
            "fina_indicator": (ts_client.fetch_fina_indicator, "indicator")
        }

        for name, (api_func, category) in tasks.items():
            try:
                df = api_func(ann_date=ann_date)
                if df.empty: continue

                df = df[df['ts_code'].isin(universe)]
                if df.empty: continue

                df = df.astype(object).where(pd.notnull(df), None)
                records = df.to_dict('records')

                for record in records:
                    self.db.merge(ODSFinanceReport(
                        ts_code=record['ts_code'], end_date=record['end_date'],
                        report_type=record.get('report_type', '1'),
                        update_flag=record.get('update_flag', '0'),
                        category=category, data=record, ann_date=record.get('ann_date')
                    ))
                self.db.commit()
            except Exception as e:
                print(f"  ⚠️ {name} Error: {e}")

    # --- DWS 计算逻辑 ---

    def process_market_dws(self, ts_code: str):
        """DWS: Calculate MA and QFQ Price"""
        query = f"SELECT * FROM ods_market_daily WHERE ts_code = '{ts_code}' ORDER BY trade_date"
        query_adj = f"SELECT trade_date, adj_factor FROM ods_adj_factor WHERE ts_code = '{ts_code}' ORDER BY trade_date"
        
        df_daily = pd.read_sql(query, self.db.bind)
        df_adj = pd.read_sql(query_adj, self.db.bind)
        
        if df_daily.empty or df_adj.empty: return

        df = pd.merge(df_daily, df_adj, on='trade_date', how='left')
        df['adj_factor'] = df['adj_factor'].ffill()
        latest_factor = df['adj_factor'].iloc[-1]
        df['close_qfq'] = df['close'] * (df['adj_factor'] / latest_factor)

        for ma in [20, 50, 120, 250, 850]:
            df[f'ma_{ma}'] = df['close_qfq'].rolling(window=ma, min_periods=ma).mean()

        for _, row in df.iterrows():
            self.db.merge(DWSMarketIndicators(
                ts_code=row['ts_code'],
                trade_date=row['trade_date'],
                close_qfq=row['close_qfq'],
                ma_20=row['ma_20'] if pd.notna(row['ma_20']) else None,
                ma_50=row['ma_50'] if pd.notna(row['ma_50']) else None,
                ma_120=row['ma_120'] if pd.notna(row['ma_120']) else None,
                ma_250=row['ma_250'] if pd.notna(row['ma_250']) else None,
                ma_850=row['ma_850'] if pd.notna(row['ma_850']) else None,
            ))
        self.db.commit()

    def process_finance_dws(self, ts_code: str):
        """DWS: Standardize Finance"""
        reports = self.db.query(ODSFinanceReport).filter(
            ODSFinanceReport.ts_code == ts_code,
            ODSFinanceReport.report_type == '1'
        ).all()
        
        merged = {}
        for r in reports:
            if r.end_date not in merged: merged[r.end_date] = {"ann_date": r.ann_date}
            data = r.data
            # TODO: Use FINANCE_EXTRACT_PIPELINE from mapping.py for industry-specific extraction
            for k in ['revenue', 'n_income_attr_p', 'n_cashflow_act', 'debt_to_assets', 'roe', 'grossprofit_margin']:
                if k in data: merged[r.end_date][k] = data[k]
        
        for end_date, m in merged.items():
            self.db.merge(DWSFinanceStd(
                ts_code=ts_code, end_date=end_date, ann_date=m.get('ann_date'),
                revenue=m.get('revenue'), n_income_attr_p=m.get('n_income_attr_p'),
                n_cashflow_act=m.get('n_cashflow_act'), debt_to_assets=m.get('debt_to_assets'),
                roe=m.get('roe'), grossprofit_margin=m.get('grossprofit_margin')
            ))
        self.db.commit()

    # --- 调度器 (支持进度返回) ---

    def run_full_backfill(self, start_date="20150101"):
        """[PRD S5] 核心池财务与行情全量初始化"""
        yield "🚀 开始全量回溯 (Full Backfill)..."
        self.sync_stock_list()
        
        universe = list(self._get_universe_pool())
        total = len(universe)
        
        for i, ts_code in enumerate(universe):
            # 使用 yield 让前端 NiceGUI 可以实时更新进度条 [cite: 107-108]
            yield f"正在补全第 {i+1}/{total} 只: {ts_code}"
            try:
                self.sync_stock_history(ts_code, start_date)
                # DWS Calculation
                self.process_market_dws(ts_code)
                self.process_finance_dws(ts_code)
                time.sleep(0.1) # 频次保护
            except Exception as e:
                yield f"⚠️ {ts_code} 同步失败: {str(e)}"
        yield "✅ 全量回溯任务完成"

    def run_daily_routine(self):
        """[PRD S3/S4] 日常更新流程"""
        today = datetime.now().strftime('%Y%m%d')
        yield f"🚀 Starting Daily Routine for {today}..."
        
        yield "Step 1/4: Updating Stock Universe..."
        self.sync_stock_list()
        
        yield "Step 2/4: Syncing Market Data..."
        self.sync_daily_market(today)
        
        yield "Step 3/4: Checking Financial Announcements..."
        self.sync_financial_daily(today)
        
        yield "Step 4/4: Recalculating DWS Indicators..."
        universe = list(self._get_universe_pool())
        total = len(universe)
        for i, ts_code in enumerate(universe):
            if i % 50 == 0:
                yield f"  > DWS 计算进度: {i}/{total}..."
            self.process_market_dws(ts_code)
            self.process_finance_dws(ts_code)
            
        yield "✅ Daily Routine Completed Successfully!"

if __name__ == "__main__":
    u = DataUpdater()
    # u.sync_stock_list()
    u.close()