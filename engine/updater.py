import pandas as pd
from sqlalchemy import text
from interface.tushare_client import ts_client
from database.models import (
    SessionLocal, StockBasic, Watchlist, 
    ODSMarketDaily, ODSAdjFactor, ODSFinanceReport, 
    DWSMarketIndicators, DWSFinanceStd
)
from datetime import datetime, timedelta
import time

class DataUpdater:
    def __init__(self):
        self.db = SessionLocal()

    def close(self):
        self.db.close()

    def _get_universe_pool(self) -> set:
        """
        [Core Filter] Get the set of TS_CODES for CSI800 + Watchlist.
        PRD 1.2: Only maintain these stocks.
        """
        # 1. Get CSI800
        csi800 = self.db.query(StockBasic.ts_code).filter(StockBasic.is_csi800 == True).all()
        pool = {row.ts_code for row in csi800}
        
        # 2. Get Watchlist
        watchlist = self.db.query(Watchlist.ts_code).all()
        for row in watchlist:
            pool.add(row.ts_code)
            
        print(f"🎯 Universe Pool Size: {len(pool)}")
        return pool

    def sync_stock_list(self):
        """
        PRD 3.1: Update Stock List & CSI800 Marks
        """
        print("🔄 Syncing Stock List...")
        df_basics = ts_client.fetch_stock_basic()
        if df_basics.empty: return

        # Mark CSI 800
        try:
            now_str = datetime.now().strftime("%Y%m%d")
            # Pull index weights (requires 2000 points, verified)
            df_csi800 = ts_client.pro.index_weight(index_code='000906.SH', start_date='20240101', end_date=now_str)
            
            if not df_csi800.empty:
                latest_date = df_csi800['trade_date'].max()
                df_latest = df_csi800[df_csi800['trade_date'] == latest_date]
                csi800_set = set(df_latest['con_code'].tolist())
            else:
                csi800_set = set()
                print("⚠️ Warning: Could not fetch CSI800 constituents.")
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

    def sync_daily_market(self, start_date: str, end_date: str):
        """
        S3 Scenario: Wide Core Update (Funnel Mode)
        Fetches ALL market data, filters for Universe, saves to ODS.
        """
        print(f"📈 Syncing Market ({start_date} - {end_date})...")
        universe = self._get_universe_pool()
        if not universe:
            print("⚠️ Universe is empty! Run sync_stock_list first or add to Watchlist.")
            return

        dates = pd.date_range(start=start_date, end=end_date).strftime('%Y%m%d').tolist()
        
        for trade_date in dates:
            try:
                # 1. Fetch Full Market (1 API Call)
                df_daily = ts_client.fetch_daily(trade_date=trade_date)
                df_adj = ts_client.fetch_adj_factor(trade_date=trade_date)
                
                if df_daily.empty:
                    print(f"  - {trade_date}: No Trading Data (Holiday?)")
                    continue

                # 2. THE FUNNEL: Filter by Universe
                df_daily_filtered = df_daily[df_daily['ts_code'].isin(universe)]
                
                # 3. Save ODS Market
                daily_objs = []
                for _, row in df_daily_filtered.iterrows():
                    daily_objs.append(ODSMarketDaily(
                        ts_code=row['ts_code'],
                        trade_date=row['trade_date'],
                        open=row['open'], high=row['high'], low=row['low'], close=row['close'],
                        pre_close=row['pre_close'], change=row['change'], pct_chg=row['pct_chg'],
                        vol=row['vol'], amount=row['amount']
                    ))
                
                # Bulk save is safer for memory, but merge is fine for <1000 rows
                for obj in daily_objs:
                    self.db.merge(obj)

                # 4. Save ODS Adj Factor (Filtered)
                if not df_adj.empty:
                    df_adj_filtered = df_adj[df_adj['ts_code'].isin(universe)]
                    for _, row in df_adj_filtered.iterrows():
                        self.db.merge(ODSAdjFactor(
                            ts_code=row['ts_code'],
                            trade_date=row['trade_date'],
                            adj_factor=row['adj_factor']
                        ))

                self.db.commit()
                print(f"  ✅ {trade_date}: Saved {len(daily_objs)} records (Filtered from {len(df_daily)})")

            except Exception as e:
                self.db.rollback()
                print(f"  ❌ {trade_date}: Failed - {e}")

    def sync_financial_daily(self, ann_date: str):
        """
        S4 Scenario: Deep Core Update (Incremental)
        Fetches financials by Announcement Date -> Filter -> Save.
        """
        print(f"💰 Syncing Financials for Ann Date: {ann_date}...")
        universe = self._get_universe_pool()
        
        tasks = {
            "income": (ts_client.fetch_income, "income"),
            "balancesheet": (ts_client.fetch_balancesheet, "balance"),
            "cashflow": (ts_client.fetch_cashflow, "cashflow"),
            "fina_indicator": (ts_client.fetch_fina_indicator, "indicator")
        }

        for name, (api_func, category) in tasks.items():
            try:
                # Fetch by Ann Date (Efficient)
                df = api_func(ann_date=ann_date)
                if df.empty: continue

                # Funnel Filter
                df = df[df['ts_code'].isin(universe)]
                if df.empty: continue

                # Handle NaNs for JSONB
                df = df.astype(object).where(pd.notnull(df), None)
                records = df.to_dict('records')

                for record in records:
                    pk_data = {
                        "ts_code": record.get("ts_code"),
                        "end_date": record.get("end_date"),
                        "report_type": record.get("report_type", '1'), 
                        "update_flag": record.get("update_flag", '0'),
                        "ann_date": record.get("ann_date"),
                        "category": category,
                        "data": record
                    }
                    self.db.merge(ODSFinanceReport(**pk_data))
                
                self.db.commit()
                print(f"  - {name}: Updated {len(records)} reports")

            except Exception as e:
                print(f"  ⚠️ {name} Error: {e}")

    def process_market_dws(self, ts_code: str):
        """
        DWS: Calculate MA and QFQ Price (Per Stock)
        """
        # (Same logic as V1, simply retained)
        # 1. Read ODS
        query = f"SELECT * FROM ods_market_daily WHERE ts_code = '{ts_code}' ORDER BY trade_date"
        query_adj = f"SELECT trade_date, adj_factor FROM ods_adj_factor WHERE ts_code = '{ts_code}' ORDER BY trade_date"
        
        df_daily = pd.read_sql(query, self.db.bind)
        df_adj = pd.read_sql(query_adj, self.db.bind)
        
        if df_daily.empty or df_adj.empty: return

        # 2. Merge & Calc
        df = pd.merge(df_daily, df_adj, on='trade_date', how='left')
        df['adj_factor'] = df['adj_factor'].ffill()
        latest_factor = df['adj_factor'].iloc[-1]
        df['close_qfq'] = df['close'] * (df['adj_factor'] / latest_factor)

        # 3. MA
        for ma in [20, 50, 120, 250, 850]:
            df[f'ma_{ma}'] = df['close_qfq'].rolling(window=ma, min_periods=ma).mean()

        # 4. Save
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
        """
        DWS: Standardize Finance (Extract Report Type 1)
        """
        # (Same logic as V1, retained for completeness)
        reports = self.db.query(ODSFinanceReport).filter(
            ODSFinanceReport.ts_code == ts_code,
            ODSFinanceReport.report_type == '1'
        ).all()
        
        merged = {}
        for r in reports:
            if r.end_date not in merged: merged[r.end_date] = {"ann_date": r.ann_date}
            # Simplified mapping
            data = r.data
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

    def run_daily_routine(self):
        """
        [The Orchestrator] 
        Generator function for NiceGUI. 
        Executes the S3/S4 flows for 'Today'.
        """
        today = datetime.now().strftime('%Y%m%d')
        # today = "20231229" # Debug hardcode if needed
        
        yield f"🚀 Starting Daily Routine for {today}..."
        
        # 1. Universe Update
        yield "Step 1/4: Updating Stock Universe..."
        self.sync_stock_list()
        
        # 2. Market Data (Wide Funnel)
        yield "Step 2/4: Syncing Market Data..."
        self.sync_daily_market(today, today)
        
        # 3. Financial Data (Incremental)
        yield "Step 3/4: Checking Financial Announcements..."
        self.sync_financial_daily(today)
        
        # 4. DWS Calculation (Deep Loop)
        yield "Step 4/4: Recalculating DWS Indicators..."
        universe = self._get_universe_pool()
        total = len(universe)
        for i, ts_code in enumerate(universe):
            if i % 10 == 0:
                yield f"  > Processing DWS: {i}/{total} ({ts_code})..."
            self.process_market_dws(ts_code)
            self.process_finance_dws(ts_code)
            
        yield "✅ Daily Routine Completed Successfully!"

if __name__ == "__main__":
    # CLI Test
    u = DataUpdater()
    # Pull last 3 days for test
    start = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d')
    end = datetime.now().strftime('%Y%m%d')
    u.sync_stock_list()
    u.sync_daily_market(start, end)
    u.close()