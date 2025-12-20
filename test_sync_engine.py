# FILE PATH: test_sync_engine.py
import sys
import os

# 确保能加载项目模块
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from engine.updater import DataUpdater
from database.models import SessionLocal, ODSMarketDaily, ODSFinanceReport, DWSMarketIndicators, DWSFinanceStd
from sqlalchemy import func

def run_test_sync(ts_code='600519.SH'):
    print(f"🧪 === Invest System 引擎冒烟测试: {ts_code} ===")
    updater = DataUpdater()
    db = SessionLocal()

    try:
        # 1. 清理该标的既有数据 (为了测试纯净性)
        print(f"🧹 正在清理 {ts_code} 的旧数据...")
        db.query(ODSMarketDaily).filter(ODSMarketDaily.ts_code == ts_code).delete()
        db.query(ODSFinanceReport).filter(ODSFinanceReport.ts_code == ts_code).delete()
        db.commit()

        # 2. 模拟 S1/S2 同步过程
        print(f"📥 正在同步 ODS 原始数据 (从 2015-01-01 起)...")
        # 备注：由于是生成器，这里模拟 UI 调用循环
        updater.sync_stock_history(ts_code, start_date="20150101")
        
        # 3. 验证 ODS 落地情况
        daily_count = db.query(ODSMarketDaily).filter(ODSMarketDaily.ts_code == ts_code).count()
        finance_count = db.query(ODSFinanceReport).filter(ODSFinanceReport.ts_code == ts_code).count()
        print(f"✅ ODS 落地检查：行情 {daily_count} 行, 财报 {finance_count} 份。")

        # 4. 执行 DWS 炼制
        print(f"⚙️ 正在执行 DWS 层衍生指标计算...")
        updater.process_market_dws(ts_code)
        updater.process_finance_dws(ts_code)

        # 5. 验证 DWS 产出
        latest_ma = db.query(DWSMarketIndicators).filter(DWSMarketIndicators.ts_code == ts_code).order_by(DWSMarketIndicators.trade_date.desc()).first()
        std_finance = db.query(DWSFinanceStd).filter(DWSFinanceStd.ts_code == ts_code).count()
        
        if latest_ma:
            print(f"📈 DWS 行情检查：最新收盘价(QFQ): {latest_ma.close_qfq:.2f}, MA250: {latest_ma.ma_250 or '计算中'}")
        print(f"💰 DWS 财务检查：已炼制标准化财报 {std_finance} 条。")

        if daily_count > 0 and finance_count > 0:
            print("\n🎉 === 测试通过：同步引擎链路已打通！ ===")
        else:
            print("\n❌ === 测试失败：未获取到有效数据，请检查 Tushare Token 和网络 === ")

    finally:
        db.close()
        updater.close()

if __name__ == "__main__":
    # 如果要测试多只标的，可以在此修改
    run_test_sync('600519.SH')