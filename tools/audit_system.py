import sys
import os
import pandas as pd
from sqlalchemy import func, text

# 路径设置
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import SessionLocal, StockBasic, ODSMarketDaily, ODSFinanceReport
from core.mapping import SOURCE_TABLE_MAP

class DataAuditor:
    def __init__(self):
        self.db = SessionLocal()
        print("\n⚖️ === Invest System 数据审计中心 V1.0 ===")

    def audit_market_data(self):
        """审计行情连续性"""
        print("\n[1. 行情连续性审计]")
        # 统计 Universe 中每只股票的记录数
        query = self.db.query(
            ODSMarketDaily.ts_code, 
            func.count(ODSMarketDaily.trade_date).label('count'),
            func.min(ODSMarketDaily.trade_date).label('start'),
            func.max(ODSMarketDaily.trade_date).label('end')
        ).group_by(ODSMarketDaily.ts_code).all()

        if not query:
            print("  ⚠️ ODS Market 表为空，请先运行同步。")
            return

        df = pd.DataFrame(query)
        print(f"  - 覆盖标的总数: {len(df)}")
        # 找出记录数显著偏少的标的 (例如少于 100 行)
        gaps = df[df['count'] < 100]
        if not gaps.empty:
            print(f"  - ⚠️ 警告: 发现 {len(gaps)} 只股票记录严重不足 (少于 100 天)。")

    def audit_financial_completeness(self):
        """审计财务报表齐备性 (四大金刚)"""
        print("\n[2. 财务报表齐备性审计]")
        
        # 统计每个 ts_code 在每个 end_date 下拥有的 category 数量
        # 理论上，一个健康的报告期应该拥有 4 个 category (income, balance, cashflow, indicator)
        query = text("""
            SELECT ts_code, end_date, COUNT(DISTINCT category) as cat_count
            FROM ods_finance_report
            WHERE report_type = '1'
            GROUP BY ts_code, end_date
            HAVING COUNT(DISTINCT category) < 4
        """)
        
        results = self.db.execute(query).fetchall()
        
        if not results:
            print("  ✅ 财务报表审计通过：所有入库报告期均已凑齐四大接口数据。")
        else:
            print(f"  ❌ 异常: 发现 {len(results)} 组财报数据不完整 (缺少部分接口数据)。")
            # 打印前 5 个异常样本
            for r in results[:5]:
                print(f"    - {r.ts_code} [{r.end_date}]: 仅有 {r.cat_count}/4 张报表")

    def run_full_audit(self):
        try:
            self.audit_market_data()
            self.audit_financial_completeness()
        finally:
            self.db.close()
            print("\n" + "="*40)

if __name__ == "__main__":
    auditor = DataAuditor()
    auditor.run_full_audit()