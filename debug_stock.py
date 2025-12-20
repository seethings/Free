# FILE PATH: debug_stock.py
import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from database.models import SessionLocal, DWSMarketIndicators, DWSFinanceStd, StockBasic
from sqlalchemy import func

def debug_haier():
    db = SessionLocal()
    ts_code = '600690.SH'
    
    # 1. 检查基础标记
    basic = db.query(StockBasic).filter(StockBasic.ts_code == ts_code).first()
    print(f"🏗️ 基础检查: {ts_code} | 名称: {basic.name if basic else '未找到'}")

    # 2. 检查 DWS 行情指标 (取最新一条)
    market = db.query(DWSMarketIndicators).filter(DWSMarketIndicators.ts_code == ts_code)\
               .order_by(DWSMarketIndicators.trade_date.desc()).first()
    if market:
        print(f"⚖️ 行情检查: 日期={market.trade_date}, PE={market.pe_ttm}, PB={market.pb}, 市值={market.total_mv}")
    else:
        print("❌ 行情检查: DWSMarketIndicators 中无数据")

    # 3. 检查 DWS 标准财务 (取最新一条)
    finance = db.query(DWSFinanceStd).filter(DWSFinanceStd.ts_code == ts_code)\
                .order_by(DWSFinanceStd.end_date.desc()).first()
    if finance:
        print(f"🏰 财务检查: 报告期={finance.end_date}, ROE={finance.roe}, 负债率={finance.debt_to_assets}")
    else:
        print("❌ 财务检查: DWSFinanceStd 中无数据")
    
    db.close()

if __name__ == "__main__":
    debug_haier()