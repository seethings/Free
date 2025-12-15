import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from interface.tushare_client import ts_client
from database.models import SessionLocal, StockBasic
from core.config import settings
from datetime import datetime

class DataUpdater:
    def __init__(self):
        self.db = SessionLocal()

    def sync_stock_list(self):
        """
        全量同步股票列表，并标记中证800成分股 (PRD 3.1)
        """
        print("🔄 开始同步股票基础列表...")
        
        # 1. 拉取全市场股票 (Tushare API)
        df_basics = ts_client.fetch_stock_basic()
        if df_basics.empty:
            print("⚠️ 未获取到股票列表，任务终止")
            return

        # 2. 拉取中证800成分股 (用于标记核心资产)
        # 000906.SH 是中证800指数代码
        # 注意: index_weight 接口需要 2000 积分 
        try:
            # 获取最新一个月的成分股（这里简化逻辑，取上个月的成分）
            # 实际生产中可能需要动态计算日期，这里暂取最近的逻辑
            # Tushare Pro 的 index_weight 通常按月更新
            now_str = datetime.now().strftime("%Y%m%d")
            # 尝试拉取最新的成分
            df_csi800 = ts_client.pro.index_weight(index_code='000906.SH', start_date='20240101', end_date=now_str)
            
            # 如果没取到（比如年初还没更新），可以尝试取去年的，这里做简单容错
            if df_csi800.empty:
                 print("⚠️ 警告: 未获取到中证800成分股，将跳过标记步骤")
                 csi800_set = set()
            else:
                 # 取最新日期的成分
                 latest_date = df_csi800['trade_date'].max()
                 df_latest = df_csi800[df_csi800['trade_date'] == latest_date]
                 csi800_set = set(df_latest['con_code'].tolist())
                 print(f"✅ 获取到中证800成分股 ({latest_date}): {len(csi800_set)} 只")

        except Exception as e:
            print(f"⚠️ 中证800接口调用失败: {e}")
            csi800_set = set()

        # 3. 数据处理与标记
        # 默认全部 False
        df_basics['is_csi800'] = False
        # 如果代码在 csi800_set 中，设为 True
        if csi800_set:
            df_basics.loc[df_basics['ts_code'].isin(csi800_set), 'is_csi800'] = True

        # 4. 写入数据库 (Upsert 模式)
        # 使用 SQLAlchemy Core 的 bulk insert 效率较高，或者逐行 merge
        # 这里为了演示清晰，使用 pandas to_sql 的替代方案或 ORM 循环
        # 考虑到只有 5000 条数据，ORM 效率可接受
        
        count = 0
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
            self.db.merge(stock) # merge 会根据主键自动 insert 或 update
            count += 1
            
        self.db.commit()
        print(f"✅ 股票列表同步完成! 共处理: {count} 只, 其中中证800: {len(csi800_set)} 只")

    def close(self):
        self.db.close()

# 快捷入口
if __name__ == "__main__":
    updater = DataUpdater()
    updater.sync_stock_list()
    updater.close()