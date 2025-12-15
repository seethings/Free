# 字段映射字典 (Key: DB/API Field, Value: Chinese Name)
# Ref: Tushare Pro 字段字典

FIELD_MAPPING = {
    # --- 基础信息 ---
    "ts_code": "TS代码",
    "symbol": "股票代码",
    "name": "股票名称",
    "industry": "行业",
    "area": "地域",
    "list_date": "上市日期",
    "is_csi800": "中证800",

    # --- 日线行情 (Daily) ---
    "trade_date": "交易日期",
    "open": "开盘价",
    "high": "最高价",
    "low": "最低价",
    "close": "收盘价",
    "pre_close": "昨收价",
    "change": "涨跌额",
    "pct_chg": "涨跌幅(%)",
    "vol": "成交量(手)",
    "amount": "成交额(千元)",
    "turnover_rate": "换手率(%)",
    "pe_ttm": "市盈率(TTM)",
    "pb": "市净率",
    "total_mv": "总市值(万)",
    "circ_mv": "流通市值(万)",

    # --- 财务核心指标 (Indicator/Income/Cashflow) ---
    "end_date": "报告期",
    "ann_date": "公告日期",
    "report_type": "报表类型",
    
    # 利润表 (Income) [cite: 526]
    "total_revenue": "营业总收入",
    "revenue": "营业收入",
    "n_income_attr_p": "归母净利润",  # 核心字段
    "operate_profit": "营业利润",
    
    # 资产负债表 (Balance Sheet) [cite: 549]
    "total_assets": "资产总计",
    "total_liab": "负债合计",
    "total_hldr_eqy_exc_min_int": "股东权益合计(不含少数股东)", # 归母权益
    "money_cap": "货币资金",
    
    # 现金流量表 (Cashflow) [cite: 587]
    "n_cashflow_act": "经营活动现金净流量",
    "n_cashflow_inv_act": "投资活动现金净流量",
    "n_cashflow_fnc_act": "筹资活动现金净流量",

    # 财务指标 (Indicator) [cite: 648]
    "roe": "ROE",
    "roe_dt": "ROE(扣非)",
    "grossprofit_margin": "毛利率",
    "netprofit_margin": "净利率",
    "debt_to_assets": "资产负债率",
    "current_ratio": "流动比率",
}