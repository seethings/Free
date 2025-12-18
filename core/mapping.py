# 字段映射字典 (Key: DB/API Field, Value: Chinese Name)
# Ref: Tushare Pro 字段字典

# 1. 业务术语标准化映射 (用于 UI 和 审计)
FIELD_MAPPING = {
    # --- 基础信息 ---
    "ts_code": "TS代码",
    "symbol": "股票代码",
    "name": "股票名称",
    "industry": "所属行业",
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
    
    # 利润表 (Income)
    "total_revenue": "营业总收入",
    "revenue": "营业收入",
    "n_income_attr_p": "归母净利润",  # 核心字段
    "operate_profit": "营业利润",
    
    # 资产负债表 (Balance Sheet)
    "total_assets": "总资产",
    "total_liab": "负债合计",
    "total_hldr_eqy_exc_min_int": "股东权益合计(不含少数股东)", # 归母权益
    "money_cap": "货币资金",
    
    # 现金流量表 (Cashflow)
    "n_cashflow_act": "经营活动现金流",
    "n_cashflow_inv_act": "投资活动现金净流量",
    "n_cashflow_fnc_act": "筹资活动现金净流量",

    # 财务指标 (Indicator)
    "roe": "ROE",
    "roe_dt": "ROE(扣非)",
    "grossprofit_margin": "毛利率",
    "netprofit_margin": "净利率",
    "debt_to_assets": "资产负债率",
    "current_ratio": "流动比率",
}

# 2. 行业感知提取规则 (解决跨表字段对齐的关键！)
# 逻辑：当我们需要提取某个业务指标时，根据公司所属行业（industry）选择正确的 ODS 字段
FINANCE_EXTRACT_PIPELINE = {
    "General": { # 一般工商业
        "revenue": ["revenue"],
        "n_income": ["n_income_attr_p"],
        "cash_flow": ["n_cashflow_act"]
    },
    "Bank": { # 银行 [cite: 634-636]
        "revenue": ["int_income", "comm_income", "n_oth_b_income"], # 收入 = 利息收入+佣金收入+其他
        "n_income": ["n_income_attr_p"],
        "cash_flow": ["n_cashflow_act"]
    },
    "Insurance": { # 保险
        "revenue": ["prem_earned"], # 已赚保费
        "n_income": ["n_income_attr_p"],
    },
    "Securities": { # 证券
        "revenue": ["n_sec_tb_income", "n_sec_uw_income"], # 代理买卖证券+承销
        "n_income": ["n_income_attr_p"],
    }
}

# 3. 接口归属映射 (审计系统使用：知道去哪个表查哪个字段)
SOURCE_TABLE_MAP = {
    "income": ["revenue", "int_income", "n_income_attr_p", "prem_earned"],
    "balancesheet": ["total_assets", "total_liab", "money_cap"],
    "cashflow": ["n_cashflow_act", "n_cashflow_fnc_act"],
    "fina_indicator": ["roe", "debt_to_assets", "grossprofit_margin"]
}

# 4. 报表类型映射
REPORT_TYPE_MAP = {
    "1": "合并报表",
    "6": "母公司报表",
    "11": "调整前合并报表"
}