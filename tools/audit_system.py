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
        print("\n[2. 财务报表齐备性审计 (PRD 1.3 2015+ 标准)]")
        
        # 修改点：增加日期过滤，确保不审计 2015 之前的无效数据
        query = text("""
            WITH report_stats AS (
                SELECT ts_code, end_date, COUNT(DISTINCT category) as cat_count
                FROM ods_finance_report
                WHERE report_type = '1' AND end_date >= '20150101'
                GROUP BY ts_code, end_date
            )
            SELECT 
                COUNT(*) as total_reports,
                SUM(CASE WHEN cat_count = 4 THEN 1 ELSE 0 END) as perfect_reports
            FROM report_stats
        """)
        
        res = self.db.execute(query).fetchone()
        total = res.total_reports
        perfect = res.perfect_reports or 0
        health_rate = (perfect / total * 100) if total > 0 else 0
        
        print(f"  - 审计报告期总数: {total}")
        print(f"  - 完整报告期 (4/4): {perfect}")
        print(f"  - 2015后财务底座健康度: {health_rate:.2f}%")
        
        if health_rate < 100:
            # 展示真正的 2015 后的异常
            error_query = text("""
                SELECT ts_code, end_date, COUNT(DISTINCT category) as cat_count
                FROM ods_finance_report
                WHERE report_type = '1' AND end_date >= '20150101'
                GROUP BY ts_code, end_date
                HAVING COUNT(DISTINCT category) < 4
                LIMIT 5
            """)
            errors = self.db.execute(error_query).fetchall()
            if errors:
                print(f"  ❌ 发现 {total - perfect} 组异常，样本如下:")
                for r in errors:
                    print(f"    - {r.ts_code} [{r.end_date}]: 仅有 {r.cat_count}/4")

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