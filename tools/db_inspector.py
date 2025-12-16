import sys
import os
import pandas as pd
from sqlalchemy import text, func
from datetime import datetime

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import SessionLocal, StockBasic, ODSMarketDaily, ODSFinanceReport, DWSMarketIndicators, DWSFinanceStd
from core.config import settings

class DBInspector:
    def __init__(self):
        self.db = SessionLocal()
        print(f"\n🩺 === Invest System DB Inspector V1.1 ===")
        print(f"📅 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*50)

    def check_counts(self):
        """基础计数检查"""
        print("\n[1. Base Counts]")
        
        counts = {
            "Stocks (Total)": self.db.query(StockBasic).count(),
            "ODS Daily Rows": self.db.query(ODSMarketDaily).count(),
            "ODS Finance Rows": self.db.query(ODSFinanceReport).count(), # 新增
            "DWS Market Rows": self.db.query(DWSMarketIndicators).count(),
            "DWS Finance Rows": self.db.query(DWSFinanceStd).count()
        }
        
        for k, v in counts.items():
            print(f"  - {k:<20}: {v}")

    def inspect_finance_health(self):
        """财务数据深度检查"""
        print("\n[2. Finance Data Health]")
        
        # 1. 检查 Report Type 分布
        # 这能帮我们确认 Tushare 返回的是字符串 '1' 还是数字 1
        results = self.db.query(
            ODSFinanceReport.report_type, 
            func.count(ODSFinanceReport.ts_code)
        ).group_by(ODSFinanceReport.report_type).all()
        
        if not results:
            print("  ⚠️ ODS Finance is EMPTY! (Fetch failed)")
        else:
            print(f"  Report Type Dist: {dict(results)}")
            
        # 2. 检查 JSON 结构采样
        if results:
            sample = self.db.query(ODSFinanceReport).first()
            print(f"  Sample Keys (Top 5): {list(sample.data.keys())[:5]}")
            print(f"  Sample Category: {sample.category}")

    def inspect_market_integrity(self, check_date=None):
        print("\n[3. Market Integrity]")
        if not check_date:
            latest = self.db.execute(text("SELECT MAX(trade_date) FROM ods_market_daily")).scalar()
            check_date = latest
        
        if check_date:
            ods_count = self.db.query(ODSMarketDaily).filter(ODSMarketDaily.trade_date == check_date).count()
            dws_ma_count = self.db.execute(text(f"SELECT count(*) FROM dws_market_indicators WHERE trade_date = '{check_date}' AND ma_20 IS NOT NULL")).scalar()
            print(f"  Date: {check_date} | ODS: {ods_count} | DWS(MA): {dws_ma_count}")
        else:
            print("  ⚠️ No market data.")

    def close(self):
        self.db.close()

if __name__ == "__main__":
    inspector = DBInspector()
    try:
        inspector.check_counts()
        inspector.inspect_finance_health() # 新增检查
        inspector.inspect_market_integrity()
    except Exception as e:
        print(f"❌ Inspection Failed: {e}")
    finally:
        inspector.close()
    print("\n" + "="*50)