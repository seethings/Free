# core/mapping.py

# ==============================================================================
# INVEST SYSTEM MASTER MAPPING V7.1 - 核心映射矩阵
# ==============================================================================

# 1. 业务术语标准化映射 (用于 UI、Excel 导出及审计)
# 严格遵循五大维度分类：🏗️基、🛡️盾、🏰核、🚀矛、⚖️秤
FIELD_MAPPING = {
    # --- 🏗️基 (Base: 基础信息) ---
    "ts_code": "TS代码",
    "symbol": "股票代码",
    "name": "股票名称",
    "industry": "所属行业",
    "area": "地域",
    "list_date": "上市日期",
    "is_csi800": "中证800",

    # --- 🛡️盾 (Shield: 风险排雷) ---
    "debt_to_assets": "资产负债率(%)",
    "current_ratio": "流动比率",
    "quick_ratio": "速动比率",
    "ocf_to_profit": "净现比(经营现金流/净利润)",  # 核心排雷：防止利润造假
    "goodwill": "商誉",
    "goodwill_to_assets": "商誉占总资产比(%)",      # 排除鸵鸟资产风险
    "intan_assets": "无形资产",
    "money_cap": "货币资金",
    "st_borr": "短期借款",

    # --- 🏰核 (Core: 商业壁垒与盈利) ---
    "roe": "ROE(净资产收益率)",
    "roe_dt": "ROE(扣非)",
    "grossprofit_margin": "毛利率(%)",
    "netprofit_margin": "净利率(%)",
    "roic": "ROIC(投入资本回报率)",
    "asset_turn": "总资产周转率",
    "n_income_attr_p": "归母净利润",
    "ocf_to_net_profit": "净现比(现金流/净利)",
    "toxic_asset_ratio": "垃圾资产占比(%)",
    "goodwill_net_asset_ratio": "商誉占比(%)",
    "ar_rev_gap": "应收营收增速差(%)",
    "selection_reason": "入选理由/风险提示",

    # --- 🚀矛 (Spear: 成长驱动) ---
    "tr_yoy": "营收同比增长(%)",
    "netprofit_yoy": "净利同比增长(%)",
    "dt_netprofit_yoy": "扣非净利同比增长(%)",
    "contract_liab": "合同负债(蓄水池)",
    "total_revenue": "营业总收入",
    "revenue": "营业收入",

    # --- ⚖️秤 (Scale: 估值与行情) ---
    "trade_date": "交易日期",
    "close_qfq": "前复权收盘价",
    "pe_ttm": "市盈率(TTM)",
    "pb": "市净率",
    "total_mv": "总市值(万)",
    "turnover_rate": "换手率(%)",
    "ma_20": "20日均线",
    "ma_250": "250日均线(年线)",
    "pct_chg": "涨跌幅(%)",
    "vol": "成交量(手)",
    "amount": "成交额(千元)",
}

# 2. 行业感知提取规则 (解决跨表字段对齐的关键逻辑) 
# 逻辑：当系统计算标准化财务宽表时，根据 industry 字段动态选择 ODS 原始字段
FINANCE_EXTRACT_PIPELINE = {
    "General": { # 一般工商业
        "revenue": ["revenue"],
        "n_income": ["n_income_attr_p"],
        "cash_flow": ["n_cashflow_act"]
    },
    "Bank": { # 银行
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

# 3. 接口归属映射 (审计系统与 ReportFactory 使用：确定字段来源) [cite: 983]
SOURCE_TABLE_MAP = {
    "income": ["revenue", "int_income", "n_income_attr_p", "prem_earned", "total_revenue"],
    "balancesheet": ["total_assets", "total_liab", "money_cap", "goodwill", "intan_assets", "contract_liab", "st_borr"],
    "cashflow": ["n_cashflow_act", "n_cashflow_fnc_act"],
    "fina_indicator": ["roe", "debt_to_assets", "grossprofit_margin", "netprofit_margin", "current_ratio", "quick_ratio", "roic"]
}

# 4. 报表类型映射 (用于数据清洗过滤) [cite: 1566, 1604, 1627]
REPORT_TYPE_MAP = {
    "1": "合并报表",
    "6": "母公司报表",
    "11": "调整前合并报表"
}