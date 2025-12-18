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
    @retry_policy
    def fetch_income(self, ts_code=None, ann_date=None, start_date=None, end_date=None, period=None):
        """利润表 - 将参数设为可选，支持垂直回溯 [cite: 631-632]"""
        return self.pro.income(ts_code=ts_code, ann_date=ann_date, 
                               start_date=start_date, end_date=end_date, period=period)

    @retry_policy
    def fetch_balancesheet(self, ts_code=None, ann_date=None, start_date=None, end_date=None, period=None):
        """资产负债表 [cite: 654-655]"""
        return self.pro.balancesheet(ts_code=ts_code, ann_date=ann_date, 
                                     start_date=start_date, end_date=end_date, period=period)

    @retry_policy
    def fetch_cashflow(self, ts_code=None, ann_date=None, start_date=None, end_date=None, period=None):
        """现金流量表 [cite: 692-693]"""
        return self.pro.cashflow(ts_code=ts_code, ann_date=ann_date, 
                                 start_date=start_date, end_date=end_date, period=period)

    @retry_policy
    def fetch_fina_indicator(self, ts_code=None, ann_date=None, start_date=None, end_date=None, period=None):
        """财务指标表 [cite: 753-754]"""
        return self.pro.fina_indicator(ts_code=ts_code, ann_date=ann_date, 
                                       start_date=start_date, end_date=end_date, period=period)

# 单例模式
ts_client = TushareClient()