import sys
import os
from sqlalchemy import text
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.models import SessionLocal

def probe_latest_year():
    db = SessionLocal()
    target_period = '20250930'
    print(f"🕵️‍♂️ 正在专项审计 {target_period} (最新年报期) 的数据完整性...")
    
    query = text("""
        SELECT ts_code, COUNT(DISTINCT category) as cat_count
        FROM ods_finance_report
        WHERE end_date = :period AND report_type = '1'
        GROUP BY ts_code
    """)
    
    results = db.execute(query, {"period": target_period}).fetchall()
    total = len(results)
    perfect = len([r for r in results if r.cat_count == 4])
    
    print(f"\n📈 审计结果:")
    print(f"  - 目标标的总数: {total}")
    print(f"  - 满分标的 (4/4): {perfect}")
    print(f"  - 数据达成率: {(perfect/total*100 if total > 0 else 0):.2f}%")
    
    if perfect < total:
        print("\n❌ 缺失样本探测 (前5条):")
        for r in [r for r in results if r.cat_count < 4][:5]:
            print(f"    - {r.ts_code}: 仅有 {r.cat_count}/4 张报表")
    db.close()

if __name__ == "__main__":
    probe_latest_year()