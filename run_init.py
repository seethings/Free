from engine.updater import DataUpdater
from database.models import SessionLocal, StockBasic

def start_universe():
    updater = DataUpdater()
    try:
        print("🚀 开始初始化股票池 (Universe Setup)...")
        
        # 1. 同步基础列表并标记中证800
        # 此操作会调用 Tushare 的 stock_basic 和 index_weight 接口 [cite: 73-74]
        updater.sync_stock_list()
        
        # 2. 简单校验
        db = SessionLocal()
        total = db.query(StockBasic).count()
        csi800 = db.query(StockBasic).filter(StockBasic.is_csi800 == True).count()
        db.close()
        
        print(f"\n✅ 初始化完成！")
        print(f"📊 全市场标的: {total} 只")
        print(f"💎 中证800成分股: {csi800} 只")
        print("\n现在你可以运行 python3 tools/db_inspector.py 查看详细体检报告了。")
        
    except Exception as e:
        print(f"❌ 初始化失败: {e}")
    finally:
        updater.close()

if __name__ == "__main__":
    start_universe()