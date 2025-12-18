from sqlalchemy import Column, String, Float, Boolean, DateTime, Integer, Text, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
from core.config import settings

# 1. 数据库连接引擎
engine = create_engine(settings.DB_URL)

# 2. 会话工厂 (这就是报错缺失的部分)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class Base(DeclarativeBase):
    pass

# --- Meta Data Layer (基础信息) ---

class StockBasic(Base):
    """
    股票基础信息表 (PRD 4.1)
    """
    __tablename__ = "stock_basic"

    ts_code = Column(String(20), primary_key=True, comment="TS代码")
    symbol = Column(String(20), comment="股票代码")
    name = Column(String(50), comment="股票名称")
    area = Column(String(50), comment="地域")
    industry = Column(String(50), comment="所属行业")
    market = Column(String(50), comment="市场类型")
    list_date = Column(String(8), comment="上市日期")
    
    # 核心字段: 标记是否为中证800 (PRD 1.2)
    is_csi800 = Column(Boolean, default=False, index=True, comment="是否中证800")

class Watchlist(Base):
    """
    用户自选股池 (PRD 1.2)
    """
    __tablename__ = "watchlist"

    ts_code = Column(String(20), primary_key=True)
    name = Column(String(50))
    industry = Column(String(50))
    weight = Column(Float, default=1.0, comment="权重")
    add_time = Column(DateTime, default=datetime.now)

# --- ODS Layer (原始数据层 - Store Everything) ---

class ODSMarketDaily(Base):
    """
    日线行情 (PRD 2.1)
    """
    __tablename__ = "ods_market_daily"

    ts_code = Column(String(20), primary_key=True)
    trade_date = Column(String(8), primary_key=True, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    pre_close = Column(Float)
    change = Column(Float)
    pct_chg = Column(Float)
    vol = Column(Float, comment="成交量(手)")
    amount = Column(Float, comment="成交额(千元)")

class ODSAdjFactor(Base):
    """
    复权因子 (PRD 2.1)
    """
    __tablename__ = "ods_adj_factor"

    ts_code = Column(String(20), primary_key=True)
    trade_date = Column(String(8), primary_key=True)
    adj_factor = Column(Float)

class ODSFinanceReport(Base):
    """
    通用财务报表存储 (PRD 4.2)
    策略: JSONB 宽表模式
    """
    __tablename__ = "ods_finance_report"

    # 复合主键：必须包含 category 以区分 income/balancesheet/cashflow 接口数据 [cite: 8, 93-95]
    ts_code = Column(String(20), primary_key=True)
    end_date = Column(String(8), primary_key=True, comment="报告期")
    report_type = Column(String(10), primary_key=True, comment="报表类型")
    update_flag = Column(String(5), primary_key=True, default='0', comment="更新标记")
    category = Column(String(20), primary_key=True, index=True, comment="报表类别") # 升级为主键
    
    ann_date = Column(String(8), comment="公告日期")
    
    # 核心字段
    data = Column(JSONB, comment="原始财务数据JSON")

# --- DWS Layer (标准服务层 - Strict Logic) ---

class DWSMarketIndicators(Base):
    """
    市场衍生指标表 (PRD 2.2)
    包含: 复权后均线, PE/PB/市值
    """
    __tablename__ = "dws_market_indicators"

    ts_code = Column(String(20), primary_key=True)
    trade_date = Column(String(8), primary_key=True, index=True)
    
    # 基础指标 (来自 daily_basic)
    pe_ttm = Column(Float, comment="PE(TTM)")
    pb = Column(Float, comment="市净率")
    total_mv = Column(Float, comment="总市值")
    turnover_rate = Column(Float, comment="换手率")
    
    # 计算指标 (基于 QFQ)
    close_qfq = Column(Float, comment="前复权收盘价")
    ma_20 = Column(Float, comment="20日均线")
    ma_50 = Column(Float, comment="50日均线")
    ma_120 = Column(Float, comment="120日均线")
    ma_250 = Column(Float, comment="250日均线 (年线)")
    # PRD 3.1 容错: 行数<850时，ma_850为NULL
    ma_850 = Column(Float, comment="850日均线 (三年线)")

class DWSFinanceStd(Base):
    """
    标准化财务宽表 (PRD 2.2)
    逻辑: 仅存储 report_type='1' (合并报表) 的清洗后数据
    """
    __tablename__ = "dws_finance_std"

    ts_code = Column(String(20), primary_key=True)
    end_date = Column(String(8), primary_key=True, index=True, comment="报告期")
    ann_date = Column(String(8), comment="公告日期")
    
    # 核心字段 (映射自 Mapping)
    revenue = Column(Float, comment="营业收入")
    n_income_attr_p = Column(Float, comment="归母净利润")
    n_cashflow_act = Column(Float, comment="经营现金流")
    debt_to_assets = Column(Float, comment="资产负债率")
    roe = Column(Float, comment="ROE")
    grossprofit_margin = Column(Float, comment="毛利率")

# --- 工具函数 ---
def init_db():
    """初始化数据库表结构"""
    Base.metadata.create_all(bind=engine)