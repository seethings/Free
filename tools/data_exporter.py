import pandas as pd
import sys
import os
import warnings

warnings.filterwarnings('ignore', category=FutureWarning)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import SessionLocal, DWSMarketIndicators, DWSFinanceStd
from core.mapping import FIELD_MAPPING

def export_audit_excel(ts_code: str):
    db = SessionLocal()
    try:
        # 1. 提取行情与财务数据
        m_query = db.query(DWSMarketIndicators).filter(DWSMarketIndicators.ts_code == ts_code).statement
        f_query = db.query(DWSFinanceStd).filter(DWSFinanceStd.ts_code == ts_code).statement
        
        df_market = pd.read_sql(m_query, db.bind)
        df_finance = pd.read_sql(f_query, db.bind)

        if df_market.empty:
            print(f"⚠️ {ts_code} 行情数据缺失")
            return

        # --- 核心修复：类型转换 ---
        # merge_asof 要求 key 必须是 numeric 或 datetime
        df_market['trade_date_int'] = df_market['trade_date'].astype(int)
        df_market = df_market.sort_values('trade_date_int')
        
        if not df_finance.empty:
            # 过滤掉公告日期为空的异常数据
            df_finance = df_finance.dropna(subset=['ann_date'])
            df_finance['ann_date_int'] = df_finance['ann_date'].astype(int)
            df_finance = df_finance.sort_values('ann_date_int')
            
            # 使用整数化的日期进行模糊对齐
            df_audit = pd.merge_asof(
                df_market, 
                df_finance.drop(columns=['ts_code']), 
                left_on='trade_date_int', 
                right_on='ann_date_int',
                direction='backward'
            )
        else:
            df_audit = df_market

        # 2. 字段过滤与中文翻译
        available_cols = [col for col in df_audit.columns if col in FIELD_MAPPING]
        df_final = df_audit[available_cols].copy()
        df_final.rename(columns=FIELD_MAPPING, inplace=True)

        # 3. 导出
        if not os.path.exists('temp'): os.makedirs('temp')
        output_path = f"temp/audit_{ts_code}.xlsx"
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_final.sort_values('交易日期', ascending=False).to_excel(writer, index=False)
            
        print(f"✅ 审计文件已成功生成: {output_path}")

    except Exception as e:
        print(f"❌ 导出失败: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    # 再次尝试审计招行与茅台
    targets = ['600036.SH', '600519.SH']
    for t in targets:
        export_audit_excel(t)