import sys
import os
import argparse
from datetime import datetime

# Path setup
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import SessionLocal, StockBasic, Watchlist
from sqlalchemy.exc import IntegrityError

class WatchlistManager:
    def __init__(self):
        self.db = SessionLocal()

    def list_all(self):
        """列出当前自选股"""
        stocks = self.db.query(Watchlist).all()
        print(f"\n🌟 当前自选股 ({len(stocks)}):")
        print(f"{'TS Code':<12} {'Name':<10} {'Industry':<10} {'Added Time'}")
        print("-" * 50)
        for s in stocks:
            print(f"{s.ts_code:<12} {s.name:<10} {s.industry:<10} {s.add_time.strftime('%Y-%m-%d')}")
        print("-" * 50)

    def add_stock(self, ts_code_input: str):
        """添加股票 (带校验)"""
        ts_code = ts_code_input.upper()
        
        # 1. 校验是否存在于基础列表
        basic = self.db.query(StockBasic).filter(StockBasic.ts_code == ts_code).first()
        if not basic:
            print(f"❌ 错误: 代码 {ts_code} 不存在于 StockBasic 表中。请先运行 updater 更新列表。")
            return

        # 2. 添加到 Watchlist
        try:
            new_watch = Watchlist(
                ts_code=basic.ts_code,
                name=basic.name,
                industry=basic.industry,
                weight=1.0, # 默认权重
                add_time=datetime.now()
            )
            self.db.add(new_watch)
            self.db.commit()
            print(f"✅ 成功添加: {basic.name} ({ts_code})")
        except IntegrityError:
            self.db.rollback()
            print(f"⚠️ 警告: {ts_code} 已经在自选股中了。")

    def remove_stock(self, ts_code_input: str):
        """移除股票"""
        ts_code = ts_code_input.upper()
        res = self.db.query(Watchlist).filter(Watchlist.ts_code == ts_code).delete()
        self.db.commit()
        if res:
            print(f"🗑️ 已移除: {ts_code}")
        else:
            print(f"⚠️ 未找到: {ts_code}")

    def close(self):
        self.db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Invest System Watchlist Manager")
    parser.add_argument("-l", "--list", action="store_true", help="List all watchlist stocks")
    parser.add_argument("-a", "--add", type=str, help="Add a stock by TS_CODE (e.g., 600519.SH)")
    parser.add_argument("-r", "--remove", type=str, help="Remove a stock by TS_CODE")
    
    args = parser.parse_args()
    
    wm = WatchlistManager()
    
    if args.add:
        wm.add_stock(args.add)
    elif args.remove:
        wm.remove_stock(args.remove)
    elif args.list:
        wm.list_all()
    else:
        parser.print_help()
    
    wm.close()