import tushare as ts
import pandas as pd
import time
from core.config import settings
from functools import wraps

class TushareClient:
    def __init__(self):
        if not settings.TS_TOKEN:
            raise ValueError("Tushare Token is missing in settings")
        
        # 初始化 Pro 接口 (PRD 1.1)
        self.pro = ts.pro_api(settings.TS_TOKEN)
        print(f"📡 Tushare Client Initialized. Token: {settings.TS_TOKEN[:5]}***")

    def retry_policy(func):
        """
        装饰器: Tushare 官方建议的重试机制
        Ref: Tushare PDF [cite: 18]
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            max_retries = 3
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"⚠️ API Warning: {e}, Retrying ({i+1}/{max_retries})...")
                    time.sleep(1)
            raise Exception(f"❌ API Failed after {max_retries} retries.")
        return wrapper

    # --- 1. 基础数据 ---
    
    @retry_policy
    def fetch_stock_basic(self):
        """获取全市场股票列表 (PRD 2.1)"""
        fields = 'ts_code,symbol,name,area,industry,market,list_date'
        return self.pro.stock_basic(exchange='', list_status='L', fields=fields)

    # --- 2. 市场行情 (Column Storage) ---

    @retry_policy
    def fetch_daily(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        """
        日线行情
        Ref: Tushare PDF Daily Interface [cite: 252]
        """
        return self.pro.daily(ts_code=ts_code, trade_date=trade_date, 
                              start_date=start_date, end_date=end_date)

    @retry_policy
    def fetch_adj_factor(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        """复权因子"""
        return self.pro.adj_factor(ts_code=ts_code, trade_date=trade_date, 
                                   start_date=start_date, end_date=end_date)

    # --- 3. 财务数据 (JSONB Storage) ---
    # PRD 要求: Store Everything, JSONB 宽表模式
    # 关键点: 必须获取 update_flag 以区分修正报表 

    def _fetch_financial(self, api_func, ts_code, start_date, end_date):
        """通用财报获取逻辑"""
        # 强制指定字段，确保 update_flag 存在
        fields = 'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,update_flag'
        
        # 注意: Tushare 的财报接口字段非常多，这里我们不枚举所有 metrics，
        # 而是依赖 Tushare 默认返回 (API 会返回该报表的所有字段)，
        # 我们只显式确保 meta 字段存在。
        # 实际上，如果不传 fields，Tushare 默认返回所有字段，这正符合我们 JSONB 全量存储的需求。
        # 但为了稳健，我们在外部调用时如果不传 fields，它就是全量的。
        
        return api_func(ts_code=ts_code, start_date=start_date, end_date=end_date)

    @retry_policy
    def fetch_income(self, ts_code, start_date, end_date):
        """利润表"""
        return self.pro.income(ts_code=ts_code, start_date=start_date, end_date=end_date)

    @retry_policy
    def fetch_balancesheet(self, ts_code, start_date, end_date):
        """资产负债表"""
        return self.pro.balancesheet(ts_code=ts_code, start_date=start_date, end_date=end_date)

    @retry_policy
    def fetch_cashflow(self, ts_code, start_date, end_date):
        """现金流量表"""
        return self.pro.cashflow(ts_code=ts_code, start_date=start_date, end_date=end_date)

    @retry_policy
    def fetch_fina_indicator(self, ts_code, start_date, end_date):
        """财务指标表"""
        return self.pro.fina_indicator(ts_code=ts_code, start_date=start_date, end_date=end_date)

# 单例模式
ts_client = TushareClient()