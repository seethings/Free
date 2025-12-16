import sys
import os
import pandas as pd

# Path setup
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from interface.tushare_client import ts_client

def debug_finance_fetch():
    print("🕵️‍♂️ === Tushare Finance API Probe ===")
    ts_code = '600000.SH'
    start_date = '20230101'
    end_date = '20240101'
    
    print(f"Target: {ts_code} | Range: {start_date} - {end_date}")
    
    try:
        # 1. 直接调用 Client 封装的方法
        print("\n[Attempt 1] Calling fetch_income...")
        df = ts_client.fetch_income(ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        if df is None:
            print("❌ Result is None!")
        elif df.empty:
            print("⚠️ Result is Empty DataFrame!")
        else:
            print(f"✅ Success! Rows fetched: {len(df)}")
            print("Columns:", df.columns.tolist())
            print("Sample Row 1:\n", df.iloc[0].to_dict())
            
            # 检查关键字段
            if 'update_flag' in df.columns:
                print(f"update_flag found: {df['update_flag'].unique()}")
            else:
                print("⚠️ Warning: 'update_flag' MISSING in response!")

    except Exception as e:
        print(f"❌ API Call Failed: {e}")

if __name__ == "__main__":
    debug_finance_fetch()