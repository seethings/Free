import time
from engine.updater import DataUpdater
from database.models import SessionLocal, StockBasic

def run_industrial_backfill():
    print("🏗️ === Invest System V7.3 全量历史回溯启动 ===")
    print("📅 目标起点: 2015-01-01 | 🎯 目标池: CSI800 + Watchlist")
    
    updater = DataUpdater()
    try:
        # 1. 确保 Universe 名单是最新的
        print("\nStep 1: 更新标的名单与中证800标记...")
        updater.sync_stock_list()
        
        # 2. 获取所有需要回溯的标的
        universe = list(updater._get_universe_pool())
        total = len(universe)
        print(f"\nStep 2: 准备处理共 {total} 只核心标的...")

        for i, ts_code in enumerate(universe):
            start_time = time.time()
            print(f"\n--- [{i+1}/{total}] 正在深度处理: {ts_code} ---")
            
            try:
                # A. 垂直补全 ODS 原始数据 (行情 + 四大财报)
                print(f"  📥 正在抓取 2015 至今原始数据...")
                updater.sync_stock_history(ts_code, start_date="20150101")
                
                # B. 炼制 DWS 行情指标 (QFQ + MA均线)
                print(f"  📈 正在计算 QFQ 行情与均线...")
                updater.process_market_dws(ts_code) 
                
                # C. 炼制 DWS 财务宽表 (标准化 + 行业感知)
                print(f"  💰 正在执行标准化财务炼制...")
                updater.process_finance_dws(ts_code)
                
                elapsed = time.time() - start_time
                print(f"  ✅ {ts_code} 处理完成，耗时: {elapsed:.2f}s")
                
                # 频次保护：2000积分账户每分钟限200次，每只股票处理完强制休息 0.5s
                time.sleep(0.5)

            except Exception as e:
                print(f"  ❌ {ts_code} 处理失败: {str(e)}")
                continue

        print("\n🎉 === 全量历史回溯任务圆满完成！ ===")
        print("💡 建议运行 python3 tools/audit_system.py 进行最终质量审计。")

    finally:
        updater.close()

if __name__ == "__main__":
    run_industrial_backfill()