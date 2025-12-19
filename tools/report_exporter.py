import pandas as pd
import os
import sys
from sqlalchemy import text

# 路径设置：确保可以导入 database 和 core 模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import SessionLocal, StockBasic, DWSMarketIndicators, DWSFinanceStd
from core.mapping import FIELD_MAPPING

class ReportFactory:
    def __init__(self, ts_code: str):
        self.ts_code = ts_code
        self.db = SessionLocal()
        self.stock = self.db.query(StockBasic).filter(StockBasic.ts_code == ts_code).first()

    def _calculate_shield_metrics(self, df_f: pd.DataFrame):
        """
        [🛡️盾] 维度：核心风险指标二次计算
        """
        if df_f.empty:
            return df_f
        
        # 1. 计算净现比 (经营现金流 / 归母净利润)
        # 处理分母为0的情况
        df_f['ocf_to_profit'] = df_f.apply(
            lambda x: x['n_cashflow_act'] / x['n_income_attr_p'] if x['n_income_attr_p'] and x['n_income_attr_p'] != 0 else 0, 
            axis=1
        )
        
        # 2. 计算商誉占比 (商誉 / 总资产)
        # 注意：此处需确保 DWSFinanceStd 包含 goodwill 和 total_assets
        if 'goodwill' in df_f.columns and 'total_assets' in df_f.columns:
            df_f['goodwill_to_assets'] = df_f['goodwill'] / df_f['total_assets']
            
        return df_f

    def fetch_full_dataset(self):
        """抓取并聚合五大维度数据"""
        # A. 提取 DWS 财务标准化数据 (含 🏰核、🚀矛、🛡️盾 基础字段) [cite: 25]
        f_query = self.db.query(DWSFinanceStd).filter(DWSFinanceStd.ts_code == self.ts_code).statement
        df_f = pd.read_sql(f_query, self.db.bind).sort_values('end_date', ascending=False)
        
        # 执行深度诊断计算
        df_f = self._calculate_shield_metrics(df_f)
        
        # B. 提取 DWS 行情指标 (⚖️秤) [cite: 24]
        m_query = self.db.query(DWSMarketIndicators).filter(DWSMarketIndicators.ts_code == self.ts_code).statement
        df_m = pd.read_sql(m_query, self.db.bind).sort_values('trade_date', ascending=False)
        
        return df_f, df_m

    def generate_excel(self):
        """生成格式化 Excel 报告"""
        if not self.stock:
            print(f"❌ 错误：未在数据库中找到标的 {self.ts_code}")
            return

        print(f"🚀 正在为 [{self.stock.name}] 生成五维研报工厂数据...")
        df_f, df_m = self.fetch_full_dataset()

        # 创建导出目录
        out_dir = "data/reports"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        file_path = f"{out_dir}/Report_{self.ts_code}_{self.stock.name}.xlsx"

        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            # --- Sheet 1: 财务诊断 (🛡️盾、🏰核、🚀矛) ---
            # 过滤 Mapping 中定义的字段进行导出
            available_f = [col for col in df_f.columns if col in FIELD_MAPPING]
            df_f_final = df_f[available_f].rename(columns=FIELD_MAPPING)
            df_f_final.to_excel(writer, sheet_name='基本面诊断', index=False)
            
            # --- Sheet 2: 估值行情 (⚖️秤) ---
            available_m = [col for col in df_m.columns if col in FIELD_MAPPING]
            df_m_final = df_m[available_m].head(500).rename(columns=FIELD_MAPPING) # 取最近两年交易日 [cite: 24]
            df_m_final.to_excel(writer, sheet_name='行情与估值', index=False)

            # --- Sheet 3: 标的信息 (🏗️基) ---
            df_info = pd.DataFrame([self.stock.__dict__])
            available_i = [col for col in df_info.columns if col in FIELD_MAPPING]
            df_info_final = df_info[available_i].rename(columns=FIELD_MAPPING)
            df_info_final.to_excel(writer, sheet_name='公司基石', index=False)
            
        print(f"✅ 报告成功导出至: {file_path}")
        return file_path

    def close(self):
        self.db.close()

if __name__ == "__main__":
    # 针对核心标的进行验证 [cite: 4, 5]
    test_targets = ['600519.SH', '600036.SH']
    for code in test_targets:
        factory = ReportFactory(code)
        try:
            factory.generate_excel()
        finally:
            factory.close()