import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from interface.tushare_client import ts_client
from database.models import SessionLocal, StockBasic, ODSMarketDaily, ODSAdjFactor, ODSFinanceReport, DWSMarketIndicators, DWSFinanceStd
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

    def sync_daily_market(self, start_date: str, end_date: str):
        """
        S3 场景: 日线行情增量更新 (ODS层)
        """
        print(f"📈 开始同步日线行情 ({start_date} - {end_date})...")
        
        # 1. 获取日线 (全市场)
        # 策略: 按日期循环拉取，每天约 5000 条，适合 WideCore 模式
        # [cite_start]Tushare daily 接口支持单日全量 [cite: 1515]
        
        dates = pd.date_range(start=start_date, end=end_date).strftime('%Y%m%d').tolist()
        
        for trade_date in dates:
            try:
                # 1.1 拉取行情
                df_daily = ts_client.fetch_daily(trade_date=trade_date)
                if df_daily.empty:
                    print(f"  - {trade_date}: 无数据 (休市?)")
                    continue
                
                # 1.2 拉取复权因子
                df_adj = ts_client.fetch_adj_factor(trade_date=trade_date)
                
                # 1.3 入库 ODSMarketDaily
                # 使用 to_dict 转换，利用 SQLAlchemy 的 bulk_insert_mappings (需 Core 模式) 
                # 或循环 ORM merge (简单但慢)。鉴于每日仅 5000 条，ORM merge 尚可，
                # 但为性能推荐 bulk insert (这里简化演示使用 merge 逻辑的变体)
                
                daily_objs = []
                for _, row in df_daily.iterrows():
                    daily_objs.append({
                        "ts_code": row['ts_code'],
                        "trade_date": row['trade_date'],
                        "open": row['open'],
                        "high": row['high'],
                        "low": row['low'],
                        "close": row['close'],
                        "pre_close": row['pre_close'],
                        "change": row['change'],
                        "pct_chg": row['pct_chg'],
                        "vol": row['vol'],
                        "amount": row['amount']
                    })
                
                # 批量插入 (使用 Core 的 insert..on_conflict_do_update 会更优，这里用 ORM 逐个添加演示)
                # 实际生产建议: self.db.execute(insert(ODSMarketDaily).values(daily_objs).on_conflict_do_nothing())
                # 这里为了兼容性保持简单逻辑：
                for obj in daily_objs:
                    self.db.merge(ODSMarketDaily(**obj))
                
                # 1.4 入库 ODSAdjFactor
                if not df_adj.empty:
                    for _, row in df_adj.iterrows():
                        self.db.merge(ODSAdjFactor(
                            ts_code=row['ts_code'],
                            trade_date=row['trade_date'],
                            adj_factor=row['adj_factor']
                        ))
                
                self.db.commit()
                print(f"  ✅ {trade_date}: 行情入库完成 (Stocks: {len(df_daily)})")
                
            except Exception as e:
                self.db.rollback()
                print(f"  ❌ {trade_date}: 处理失败 - {e}")

    def sync_financial_report(self, ts_code: str, start_date: str = None, end_date: str = None):
        """
        S2/S4 场景: 财报数据更新 (ODS层 - JSONB)
        [FIXED V2]: 强力修复 NaN -> None，兼容 PostgreSQL JSONB
        """
        print(f"💰 开始同步财报: {ts_code}...")
        
        tasks = {
            "income": (ts_client.fetch_income, "income"),
            "balancesheet": (ts_client.fetch_balancesheet, "balance"),
            "cashflow": (ts_client.fetch_cashflow, "cashflow"),
            "fina_indicator": (ts_client.fetch_fina_indicator, "indicator")
        }
        
        for name, (api_func, category) in tasks.items():
            try:
                # 1. 拉取数据
                df = api_func(ts_code=ts_code, start_date=start_date, end_date=end_date)
                if df.empty:
                    continue
                
                # 2. [关键修复] 数据清洗
                # 先转为 object 类型，防止 pandas 将 None 自动回滚为 NaN
                # 然后将所有 NaN 替换为 None (JSON null)
                df = df.astype(object).where(pd.notnull(df), None)
                
                # 3. 转换为字典列表
                records = df.to_dict('records')
                
                # 4. 逐条入库 (Merge)
                # 注意：这里使用 bulk_insert 会更快，但为了演示 update_flag 逻辑保持循环
                # 生产环境建议优化为 bulk_insert_mappings
                for record in records:
                    pk_data = {
                        "ts_code": record.get("ts_code"),
                        "end_date": record.get("end_date"),
                        "report_type": record.get("report_type", '1'), 
                        "update_flag": record.get("update_flag", '0'),
                        "ann_date": record.get("ann_date"),
                        "category": category,
                        "data": record # record 中的 NaN 现在是 None 了
                    }
                    
                    self.db.merge(ODSFinanceReport(**pk_data))
                
                self.db.commit()
                print(f"  - {name}: {len(records)} 条记录")
                
            except Exception as e:
                self.db.rollback()
                print(f"  ⚠️ {name} 同步失败: {e}")

    def process_market_dws(self, ts_code: str):
        """
        DWS 核心逻辑: 计算复权价格与均线 (PRD 2.2)
        触发时机: 单只股票 ODS 行情更新后
        """
        print(f"🧮 计算 DWS 指标: {ts_code}...")
        
        # 1. 读取 ODS 数据 (Raw Price + Adj Factor)
        # 使用 pandas read_sql 简化处理
        query_daily = f"SELECT * FROM ods_market_daily WHERE ts_code = '{ts_code}' ORDER BY trade_date"
        query_adj = f"SELECT trade_date, adj_factor FROM ods_adj_factor WHERE ts_code = '{ts_code}' ORDER BY trade_date"
        
        df_daily = pd.read_sql(query_daily, self.db.bind)
        df_adj = pd.read_sql(query_adj, self.db.bind)
        
        if df_daily.empty or df_adj.empty:
            print("  ⚠️ 数据不足，跳过计算")
            return

        # 2. 合并复权因子
        df = pd.merge(df_daily, df_adj, on='trade_date', how='left')
        # 填充缺失因子 (向前填充)
        df['adj_factor'] = df['adj_factor'].ffill()
        
        # 3. 计算前复权价格 (QFQ)
        # 公式: P_qfq = P_raw * (Factor_curr / Factor_latest) 
        latest_factor = df['adj_factor'].iloc[-1]
        df['close_qfq'] = df['close'] * (df['adj_factor'] / latest_factor)
        
        # 4. 计算均线 (MA)
        # PRD 2.2: MA20, MA50, MA120, MA250, MA850
        ma_list = [20, 50, 120, 250, 850]
        for ma in ma_list:
            col_name = f'ma_{ma}'
            # min_periods=ma 确保数据不够时为 NaN (None)
            df[col_name] = df['close_qfq'].rolling(window=ma, min_periods=ma).mean()
            
        # 5. 准备入库数据 (DWSMarketIndicators)
        dws_records = []
        for _, row in df.iterrows():
            # 基础指标转换
            record = {
                "ts_code": row['ts_code'],
                "trade_date": row['trade_date'],
                "close_qfq": row['close_qfq'],
                "ma_20": row['ma_20'] if pd.notna(row['ma_20']) else None,
                "ma_50": row['ma_50'] if pd.notna(row['ma_50']) else None,
                "ma_120": row['ma_120'] if pd.notna(row['ma_120']) else None,
                "ma_250": row['ma_250'] if pd.notna(row['ma_250']) else None,
                "ma_850": row['ma_850'] if pd.notna(row['ma_850']) else None,
                # 透传 ODS 基础字段 (用于雷达筛选)
                "turnover_rate": None, # 需从 daily_basic 补充，此处暂留空或后续 Join
                "pe_ttm": None,        # 同上
                "pb": None,            # 同上
                "total_mv": None       # 同上
            }
            dws_records.append(record)
            
        # 6. 批量入库 (Upsert)
        for r in dws_records:
            self.db.merge(DWSMarketIndicators(**r))
            
        self.db.commit()
        print(f"  ✅ DWS 计算完成: {len(dws_records)} 条均线数据")

    def process_finance_dws(self, ts_code: str):
        """
        DWS 核心逻辑: 标准化财务宽表清洗 (PRD 2.2)
        规则: 仅提取 report_type='1' (合并报表)
        """
        print(f"🧹 清洗财务数据: {ts_code}...")
        
        # 1. 提取所有类型的 JSONB 数据
        # 获取该股票所有 report_type='1' 的记录
        reports = self.db.query(ODSFinanceReport).filter(
            ODSFinanceReport.ts_code == ts_code,
            ODSFinanceReport.report_type == '1'
        ).all()
        
        # 按 end_date 聚合数据
        # 结构: { '20231231': { 'revenue': 100, 'roe': 5... } }
        merged_data = {}
        
        for r in reports:
            if r.end_date not in merged_data:
                merged_data[r.end_date] = {"ann_date": r.ann_date}
            
            # 将 JSONB 中的数据打平合并
            # 映射关系参考 core/mapping.py
            # 实际生产建议严格按 Mapping 提取，这里做自动映射
            raw_dict = r.data
            target_fields = [
                'revenue', 'n_income_attr_p', 'n_cashflow_act', 
                'debt_to_assets', 'roe', 'grossprofit_margin'
            ]
            
            for field in target_fields:
                if field in raw_dict:
                    merged_data[r.end_date][field] = raw_dict[field]

        # 2. 入库 DWSFinanceStd
        for end_date, metrics in merged_data.items():
            dws_obj = DWSFinanceStd(
                ts_code=ts_code,
                end_date=end_date,
                ann_date=metrics.get('ann_date'),
                revenue=metrics.get('revenue'),
                n_income_attr_p=metrics.get('n_income_attr_p'),
                n_cashflow_act=metrics.get('n_cashflow_act'),
                debt_to_assets=metrics.get('debt_to_assets'),
                roe=metrics.get('roe'),
                grossprofit_margin=metrics.get('grossprofit_margin')
            )
            self.db.merge(dws_obj)
            
        self.db.commit()
        print(f"  ✅ 财务清洗完成: {len(merged_data)} 个报告期")

    def close(self):
        self.db.close()

# 快捷入口
if __name__ == "__main__":
    updater = DataUpdater()
    updater.sync_stock_list()
    updater.close()