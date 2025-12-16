import sys
import os
import pandas as pd
from sqlalchemy import text, func, distinct
from datetime import datetime

# Path setup
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import (
    SessionLocal, StockBasic, Watchlist, 
    ODSMarketDaily, ODSFinanceReport, 
    DWSMarketIndicators, DWSFinanceStd
)

class DBInspectorV2:
    def __init__(self):
        self.db = SessionLocal()
        print(f"\n🩺 === Invest System DB Inspector V2.0 (Funnel Edition) ===")
        print(f"📅 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)

    def close(self):
        self.db.close()

    def _get_universe_count(self):
        """Helper to get universe set size"""
        csi800_count = self.db.query(StockBasic).filter(StockBasic.is_csi800 == True).count()
        watchlist_count = self.db.query(Watchlist).count()
        # Note: Set union size logic is complex in SQL without subqueries, 
        # listing counts separately is enough for inspection.
        return csi800_count, watchlist_count

    def check_asset_structure(self):
        """1. 检查资产构成 (Universe Structure)"""
        print("\n[1. Asset Universe Structure]")
        
        total_stocks = self.db.query(StockBasic).count()
        csi800, watchlist = self._get_universe_count()
        
        print(f"  - 🏢 Total Stocks Listed: {total_stocks}")
        print(f"  - 💎 CSI 800 Components:  {csi800}  (Target: ~800)")
        print(f"  - 🌟 User Watchlist:      {watchlist}")
        
        if csi800 == 0:
            print("  ⚠️ CRITICAL: CSI 800 not marked! Run 'updater.sync_stock_list()' ASAP.")
        else:
            print("  ✅ Core Assets marked.")

    def check_data_density(self):
        """2. 检查数据密度 (Data Density)"""
        print("\n[2. Data Layer Density]")
        
        counts = {
            "ODS Market (Rows)": self.db.query(ODSMarketDaily).count(),
            "ODS Finance (Reports)": self.db.query(ODSFinanceReport).count(),
            "DWS Market (Indicators)": self.db.query(DWSMarketIndicators).count(),
            "DWS Finance (Std Rows)": self.db.query(DWSFinanceStd).count()
        }
        
        for k, v in counts.items():
            print(f"  - {k:<25}: {v}")

    def check_funnel_health(self):
        """3. 漏斗机制验证 (Funnel Validation)"""
        print("\n[3. Funnel Mechanism Check]")
        
        # Logic: Pick a non-universe stock and ensure it has NO financial data
        # 1. Find a stock NOT in CSI800 and NOT in Watchlist
        # This is a bit heavy for SQL, we do a simple sampling
        
        subquery = self.db.query(Watchlist.ts_code)
        sample_trash = self.db.query(StockBasic.ts_code).filter(
            StockBasic.is_csi800 == False,
            StockBasic.ts_code.not_in(subquery)
        ).limit(1).scalar()
        
        if sample_trash:
            # Check if this "trash" stock has data in ODS Finance
            junk_data = self.db.query(ODSFinanceReport).filter(ODSFinanceReport.ts_code == sample_trash).count()
            if junk_data > 0:
                print(f"  ❌ LEAK DETECTED! Non-universe stock {sample_trash} has {junk_data} financial reports.")
            else:
                print(f"  ✅ Funnel Working: Non-universe stock {sample_trash} has 0 financial reports.")
        else:
            print("  ⚠️ Cannot verify funnel (No non-universe stocks found or DB empty).")

    def check_latest_status(self):
        """4. 最新状态 (Freshness)"""
        print("\n[4. Data Freshness]")
        
        latest_daily = self.db.query(func.max(ODSMarketDaily.trade_date)).scalar()
        latest_report = self.db.query(func.max(ODSFinanceReport.ann_date)).scalar()
        
        print(f"  - Latest Market Date:   {latest_daily or 'N/A'}")
        print(f"  - Latest Report Ann:    {latest_report or 'N/A'}")

    def run(self):
        try:
            self.check_asset_structure()
            self.check_data_density()
            self.check_funnel_health()
            self.check_latest_status()
        except Exception as e:
            print(f"❌ Inspection Failed: {e}")
        finally:
            self.close()
        print("\n" + "="*60)

if __name__ == "__main__":
    inspector = DBInspectorV2()
    inspector.run()