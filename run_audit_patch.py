import time
from engine.updater import DataUpdater

def run_audit_patch():
    print("🚑 === Invest System 数据炼制补丁工具 (Audit Patch) ===")
    
    # 强制指定需要“二次炼制”的目标
    # 600036.SH (验证金融行业感知) | 600519.SH (验证一般工商业)
    targets = ['600036.SH', '600519.SH'] 
    
    updater = DataUpdater()
    try:
        print(f"🎯 正在重新处理: {targets}")

        for ts_code in targets:
            start_time = time.time()
            print(f"\n--- 正在重炼: {ts_code} ---")
            
            try:
                # 1. 补全原始数据 (如果缺失)
                print(f"  📥 Step 1: 检查 ODS 原始层 (20150101起)...")
                updater.sync_stock_history(ts_code, start_date="20150101")
                
                # 2. 重新炼制行情 (MA/QFQ/PE/PB)
                print(f"  📈 Step 2: 重新炼制 DWS 行情指标...")
                updater.process_market_dws(ts_code) 
                
                # 3. 重新炼制财务 (ROE/营收/现金流)
                print(f"  💰 Step 3: 重新炼制 DWS 财务宽表...")
                updater.process_finance_dws(ts_code)
                
                print(f"  ✅ {ts_code} 炼制完成，耗时: {time.time() - start_time:.2f}s")

            except Exception as e:
                print(f"  ❌ {ts_code} 失败: {str(e)}")

        print("\n✨ 补丁运行完毕！现在可以执行 python3 tools/data_exporter.py 验收结果。")

    finally:
        updater.close()

if __name__ == "__main__":
    run_audit_patch()