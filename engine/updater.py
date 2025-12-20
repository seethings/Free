import pandas as pd
import time
from datetime import datetime, timedelta
from sqlalchemy import text
from interface.tushare_client import ts_client
from database.models import (
    SessionLocal, StockBasic, Watchlist, 
    ODSMarketDaily, ODSAdjFactor, ODSFinanceReport, 
    DWSMarketIndicators, DWSFinanceStd, ODSDailyBasic
)
from core.mapping import SOURCE_TABLE_MAP

class DataUpdater:
    def __init__(self):
        self.db = SessionLocal()

    def close(self):
        self.db.close()

    def _get_universe_pool(self) -> set:
        """[PRD 1.2] 获取中证800+自选股的并集"""
        csi800 = self.db.query(StockBasic.ts_code).filter(StockBasic.is_csi800 == True).all()
        watchlist = self.db.query(Watchlist.ts_code).all()
        pool = {row.ts_code for row in csi800} | {row.ts_code for row in watchlist}
        return pool

    def sync_stock_list(self):
        """[PRD 3.1] 全量同步股票列表并标记中证800 (UI 适配版)"""
        yield "🔄 正在从 Tushare 获取全市场基础列表..."
        df_basics = ts_client.fetch_stock_basic()
        if df_basics.empty: 
            yield "❌ 获取失败：Tushare 返回为空。"
            return

        yield "💎 正在获取中证800最新成分股名单..."
        try:
            now_str = datetime.now().strftime("%Y%m%d")
            df_csi800 = ts_client.pro.index_weight(index_code='000906.SH', start_date='20240101', end_date=now_str)
            if not df_csi800.empty:
                latest_date = df_csi800['trade_date'].max()
                csi800_set = set(df_csi800[df_csi800['trade_date'] == latest_date]['con_code'].tolist())
            else:
                csi800_set = set()
        except Exception as e:
            yield f"⚠️ 指数获取异常: {e}"
            csi800_set = set()

        yield f"📥 正在写入数据库 (共 {len(df_basics)} 条记录)..."
        for _, row in df_basics.iterrows():
            is_in_index = row['ts_code'] in csi800_set
            stock = StockBasic(
                ts_code=row['ts_code'], symbol=row['symbol'], name=row['name'],
                area=row['area'], industry=row['industry'], market=row['market'],
                list_date=row['list_date'], is_csi800=is_in_index
            )
            self.db.merge(stock)
        
        self.db.commit()
        yield f"✅ 股票列表同步完成！已识别中证800成分股: {len(csi800_set)} 只。"

    # --- 场景 S1/S2/S5: 垂直历史回溯 (按代码同步) ---

    def run_watchlist_backfill(self):
        """
        [PRD S1/S2] 自选股行情与财报深度修补
        逻辑：针对 Watchlist 中的标的，从 20150101 起执行垂直同步 
        """
        watchlist = self.db.query(Watchlist.ts_code).all()
        targets = [r.ts_code for r in watchlist]
        
        if not targets:
            yield "⚠️ 自选池为空，请先在页面添加标的。"
            return

        total = len(targets)
        yield f"🚀 启动自选池深度同步：共 {total} 只标的"

        for i, ts_code in enumerate(targets):
            yield f"正在处理 [{i+1}/{total}]: {ts_code}"
            try:
                # 1. 执行 ODS 层垂直拉取 (行情+财报)
                self.sync_stock_history(ts_code, start_date="20150101")
                
                # 2. 执行 DWS 层数据炼制 (计算均线与标准化财报)
                self.process_market_dws(ts_code)
                self.process_finance_dws(ts_code)
                
                # 3. 2000 积分频次保护：每次同步后休眠 0.3s-0.5s [cite: 1635]
                time.sleep(0.3)
            except Exception as e:
                yield f"❌ {ts_code} 同步失败: {str(e)}"
                continue
        
        yield "✅ 自选池历史数据修复完成。"

    def sync_stock_history(self, ts_code: str, start_date="20150101"):
        """补全单只股票的所有历史数据 (ODS 层)"""
        # A. 行情数据同步
        df_daily = ts_client.fetch_daily(ts_code=ts_code, start_date=start_date)
        if not df_daily.empty:
            for _, row in df_daily.iterrows():
                self.db.merge(ODSMarketDaily(
                    ts_code=row['ts_code'], trade_date=row['trade_date'],
                    open=row['open'], high=row['high'], low=row['low'], close=row['close'],
                    pre_close=row['pre_close'], change=row['change'], pct_chg=row['pct_chg'],
                    vol=row['vol'], amount=row['amount']
                ))

        # B. 复权因子同步 [cite: 1760]
        df_adj = ts_client.fetch_adj_factor(ts_code=ts_code, start_date=start_date)
        if not df_adj.empty:
            for _, row in df_adj.iterrows():
                self.db.merge(ODSAdjFactor(ts_code=row['ts_code'], trade_date=row['trade_date'], adj_factor=row['adj_factor']))

        # C. 每日指标同步 (PE/PB/市值) [cite: 1766]
        df_basic = ts_client.pro.daily_basic(ts_code=ts_code, start_date=start_date)
        if not df_basic.empty:
            for _, row in df_basic.iterrows():
                self.db.merge(ODSDailyBasic(
                    ts_code=row['ts_code'], trade_date=row['trade_date'],
                    pe_ttm=row.get('pe_ttm'), pb=row.get('pb'), 
                    turnover_rate=row.get('turnover_rate'), total_mv=row.get('total_mv')
                ))

        # D. 四大财报同步 (JSONB 存储) [cite: 1761]
        tasks = {
            "income": ts_client.fetch_income,
            "balancesheet": ts_client.fetch_balancesheet,
            "cashflow": ts_client.fetch_cashflow,
            "fina_indicator": ts_client.fetch_fina_indicator
        }
        for category, api_func in tasks.items():
            df = api_func(ts_code=ts_code, start_date=start_date)
            if df is not None and not df.empty:
                # --- 架构级修复：动态检测主键 --- 
                # 理想的主键候选，但需兼容不同接口的字段差异
                pk_candidates = ['ts_code', 'end_date', 'report_type', 'update_flag']
                actual_pk = [col for col in pk_candidates if col in df.columns]
                
                # 执行安全去重：根据存在的字段保留最新一条 
                df = df.drop_duplicates(subset=actual_pk, keep='last')

                # 处理 NaN 并在字典转换时填充 None，防止 JSONB 写入报错 
                df = df.astype(object).where(pd.notnull(df), None)
                
                for record in df.to_dict('records'):
                    # 写入 ODS 时使用 .get() 兜底可选字段 [cite: 864-865]
                    self.db.merge(ODSFinanceReport(
                        ts_code=record['ts_code'], 
                        end_date=record['end_date'],
                        # 默认合并报表(1)和初始数据(0)以对齐数据库模型要求 [cite: 769, 864]
                        report_type=str(record.get('report_type', '1')), 
                        update_flag=str(record.get('update_flag', '0')), 
                        category=category, 
                        data=record, 
                        ann_date=record.get('ann_date')
                    ))
                self.db.commit() # 每一类报表提交一次，缩小冲突范围 [cite: 865]

    # --- 场景 S3: 水平每日行情 (按日期同步) ---

    def sync_daily_market(self, trade_date: str):
        """[PRD S3] 每日增量行情同步 (水平模式)"""
        universe = self._get_universe_pool()
        if not universe: return

        try:
            # 1. Fetch Full Market
            df_daily = ts_client.fetch_daily(trade_date=trade_date)
            df_adj = ts_client.fetch_adj_factor(trade_date=trade_date)
            
            if df_daily.empty: return

            # 2. Funnel Filter
            df_daily_filtered = df_daily[df_daily['ts_code'].isin(universe)]
            
            # 3. Save ODS
            for _, row in df_daily_filtered.iterrows():
                self.db.merge(ODSMarketDaily(
                    ts_code=row['ts_code'], trade_date=row['trade_date'],
                    open=row['open'], high=row['high'], low=row['low'], close=row['close'],
                    pre_close=row['pre_close'], change=row['change'], pct_chg=row['pct_chg'],
                    vol=row['vol'], amount=row['amount']
                ))

            if not df_adj.empty:
                df_adj_filtered = df_adj[df_adj['ts_code'].isin(universe)]
                for _, row in df_adj_filtered.iterrows():
                    self.db.merge(ODSAdjFactor(
                        ts_code=row['ts_code'], trade_date=row['trade_date'],
                        adj_factor=row['adj_factor']
                    ))

            self.db.commit()
            print(f"  ✅ Market Snapshot {trade_date}: Saved {len(df_daily_filtered)} records.")

        except Exception as e:
            self.db.rollback()
            print(f"  ❌ Market Snapshot Failed: {e}")

    # --- 场景 S4: 水平每日财报 (按公告日同步) ---

    def sync_financial_daily(self, ann_date: str):
        """
        [PRD S4 修正版] 每日增量财报同步
        针对 2000 积分优化：通过披露计划反查个股，避免全市场拉取报错
        """
        universe = self._get_universe_pool()
        if not universe:
            return

        try:
            # 1. 获取当日实际披露财报的名单 (actual_date)
            # Ref: Tushare PDF 
            df_ann = ts_client.pro.disclosure_date(actual_date=ann_date)
            if df_ann.empty:
                yield f"  ☕ {ann_date} 无财报披露。"
                return

            # 2. 筛选出属于我们 Universe 的标的
            targets = df_ann[df_ann['ts_code'].isin(universe)]['ts_code'].unique().tolist()
            
            if not targets:
                yield f"  ☕ {ann_date} 披露的 {len(df_ann)} 家公司均不在核心池中。"
                return

            yield f"  📢 发现 {len(targets)} 只核心标的披露财报，开始精准同步..."

            # 3. 逐个同步个股财报 (复用 S1/S2 的垂直同步逻辑)
            for i, ts_code in enumerate(targets):
                yield f"    > [{i+1}/{len(targets)}] 同步财报: {ts_code}"
                # 此处仅同步公告日前后的数据即可，为保险起见同步最近一年
                # start_date 设为公告日前一年
                sync_start = (datetime.strptime(ann_date, "%Y%m%d") - timedelta(days=365)).strftime("%Y%m%d")
                self.sync_stock_history(ts_code, start_date=sync_start)
                
                # 频次保护
                time.sleep(0.2)

            self.db.commit()
            yield f"  ✅ {ann_date} 财报增量同步完成。"

        except Exception as e:
            self.db.rollback()
            yield f"  ❌ 财报增量同步失败: {str(e)}"

    # --- DWS 计算逻辑 ---

    def process_market_dws(self, ts_code: str):
        """DWS: 计算均线、QFQ 并补全基本面指标"""
        # 1. 联合查询 ODS 行情和每日指标 (daily_basic)
        query = text("""
            SELECT m.*, b.pe_ttm, b.pb, b.total_mv, b.turnover_rate, a.adj_factor
            FROM ods_market_daily m
            LEFT JOIN ods_daily_basic b ON m.ts_code = b.ts_code AND m.trade_date = b.trade_date
            LEFT JOIN ods_adj_factor a ON m.ts_code = a.ts_code AND m.trade_date = a.trade_date
            WHERE m.ts_code = :ts_code
            ORDER BY m.trade_date
        """)
        
        df = pd.read_sql(query, self.db.bind, params={"ts_code": ts_code})
        if df.empty: return

        # 2. 计算前复权
        df['adj_factor'] = df['adj_factor'].ffill()
        latest_factor = df['adj_factor'].iloc[-1] if not df['adj_factor'].isnull().all() else 1.0
        df['close_qfq'] = df['close'] * (df['adj_factor'] / latest_factor)

        # 3. 计算均线 [cite: 843]
        for ma in [20, 50, 120, 250, 850]:
            df[f'ma_{ma}'] = df['close_qfq'].rolling(window=ma, min_periods=ma).mean()

        # 4. Upsert 写入 DWS
        for _, row in df.iterrows():
            self.db.merge(DWSMarketIndicators(
                ts_code=row['ts_code'],
                trade_date=row['trade_date'],
                pe_ttm=row.get('pe_ttm'),
                pb=row.get('pb'),
                total_mv=row.get('total_mv'),
                turnover_rate=row.get('turnover_rate'),
                close_qfq=row['close_qfq'],
                ma_20=row['ma_20'] if pd.notna(row['ma_20']) else None,
                ma_50=row['ma_50'] if pd.notna(row['ma_50']) else None,
                ma_120=row['ma_120'] if pd.notna(row['ma_120']) else None,
                ma_250=row['ma_250'] if pd.notna(row['ma_250']) else None,
                ma_850=row['ma_850'] if pd.notna(row['ma_850']) else None,
            ))
        self.db.commit()

    def process_finance_dws(self, ts_code: str):
        """[核心修复] 炼制时自动合并 roe 与 roe_dt，并计算审计指标"""
        reports = self.db.query(ODSFinanceReport).filter(
            ODSFinanceReport.ts_code == ts_code,
            ODSFinanceReport.report_type == '1'
        ).all()
        
        merged = {}
        for r in reports:
            end_date = r.end_date
            if end_date not in merged:
                merged[end_date] = {"ann_date": r.ann_date}
            
            data = r.data
            # 提取基础字段
            fields = [
                'revenue', 'n_income_attr_p', 'n_cashflow_act', 'grossprofit_margin',
                'oth_receiv', 'prepayment', 'goodwill', 'total_assets', 'total_hldr_eqy_exc_min_int',
                'debt_to_assets', 'roe', 'roe_dt', 'total_liab'
            ]
            for k in fields:
                if k in data and data[k] is not None:
                    merged[end_date][k] = data[k]
        
        for end_date, m in merged.items():
            # 必须有公告日期才能进行后续的 merge_asof [cite: 847]
            if not m.get('ann_date'): continue 
            
            # 1. 准备计算数据 (利用 .get 容错)
            net_profit = m.get('n_income_attr_p', 0) or 0
            ocf = m.get('n_cashflow_act', 0) or 0
            oth_receiv = m.get('oth_receiv', 0) or 0
            prepay = m.get('prepayment', 0) or 0
            assets = m.get('total_assets', 0) or 0
            goodwill = m.get('goodwill', 0) or 0
            equity = m.get('total_hldr_eqy_exc_min_int', 0) or 0
            
            # 2. 计算审计指标
            # 净现比
            ocf_ratio = ocf / net_profit if net_profit and net_profit != 0 else 0
            # 垃圾资产占比
            toxic_ratio = (oth_receiv + prepay) / assets if assets and assets != 0 else 0
            # 商誉占比
            gw_ratio = goodwill / equity if equity and equity != 0 else 0
            
            # 3. 负债率多路径提取
            debt_ratio = m.get('debt_to_assets')
            if debt_ratio is None:
                liab = m.get('total_liab')
                if liab and assets and assets != 0:
                    debt_ratio = (liab / assets) * 100
            
            # 4. ROE 优先级
            roe_final = m.get('roe_dt') if m.get('roe_dt') is not None else m.get('roe')

            self.db.merge(DWSFinanceStd(
                ts_code=ts_code, end_date=end_date, ann_date=m.get('ann_date'),
                revenue=m.get('revenue'),
                n_income_attr_p=m.get('n_income_attr_p'),
                n_cashflow_act=m.get('n_cashflow_act'),
                debt_to_assets=debt_ratio,
                roe=roe_final,
                grossprofit_margin=m.get('grossprofit_margin'),
                oth_receiv=m.get('oth_receiv'),
                prepayment=m.get('prepayment'),
                goodwill=m.get('goodwill'),
                total_assets=m.get('total_assets'),
                total_hldr_eqy_exc_min_int=m.get('total_hldr_eqy_exc_min_int'),
                # 存入物理字段
                ocf_to_net_profit=round(ocf_ratio, 4),
                toxic_asset_ratio=round(toxic_ratio, 4),
                goodwill_net_asset_ratio=round(gw_ratio, 4)
            ))
        self.db.commit()

    # --- 调度器 (支持进度返回) ---

    def run_full_backfill(self, start_date="20150101"):
        """[PRD S5] 核心池财务与行情全量初始化"""
        yield "🚀 开始全量回溯 (Full Backfill)..."
        yield from self.sync_stock_list()
        
        universe = list(self._get_universe_pool())
        total = len(universe)
        
        for i, ts_code in enumerate(universe):
            # 使用 yield 让前端 NiceGUI 可以实时更新进度条 [cite: 107-108]
            yield f"正在补全第 {i+1}/{total} 只: {ts_code}"
            try:
                self.sync_stock_history(ts_code, start_date)
                # DWS Calculation
                self.process_market_dws(ts_code)
                self.process_finance_dws(ts_code)
                time.sleep(0.1) # 频次保护
            except Exception as e:
                yield f"⚠️ {ts_code} 同步失败: {str(e)}"
        yield "✅ 全量回溯任务完成"

    def run_daily_routine(self):
        """
        [PRD S3/S4 进化版] 自动区间补全日更
        逻辑：自动计算断档期并循环补全，确保隔周/隔月更新不漏数据
        """
        # 1. 确定补全区间
        # 查找本地最新行情日期作为起点
        res = self.db.execute(text("SELECT max(trade_date) FROM ods_market_daily")).fetchone()
        last_date_str = res[0] if res and res[0] else "20241201" # 默认回溯起点
        
        start_date = (datetime.strptime(last_date_str, "%Y%m%d") + timedelta(days=1))
        end_date = datetime.now()
        
        # 获取期间所有交易日 (避免非交易日报错)
        # 注意：这里调用 tushare 交易日历接口
        cal = ts_client.pro.trade_cal(exchange='', start_date=start_date.strftime('%Y%m%d'), 
                                     end_date=end_date.strftime('%Y%m%d'), is_open='1')
        trade_days = cal['cal_date'].tolist()

        if not trade_days:
            yield "☕ 数据已是最新，无需更新。"
            return

        yield f"🚀 发现 {len(trade_days)} 个交易日待补全: {trade_days[0]} -> {trade_days[-1]}"

        # 2. 核心同步循环
        for date_str in trade_days:
            yield f"📅 正在处理: {date_str} ..."
            
            # A. 同步全市场行情 (S3) 
            self.sync_daily_market(date_str)
            
            # B. 同步每日指标 (PE/PB/市值) - 修正：需手动添加 horizontal 模式
            yield f"  > 拉取每日指标 (PE/PB)..."
            df_basic = ts_client.pro.daily_basic(trade_date=date_str)
            if not df_basic.empty:
                # 仅存 universe 内的
                universe = self._get_universe_pool()
                df_target = df_basic[df_basic['ts_code'].isin(universe)]
                for _, row in df_target.iterrows():
                    self.db.merge(ODSDailyBasic(
                        ts_code=row['ts_code'], trade_date=row['trade_date'],
                        pe_ttm=row.get('pe_ttm'), pb=row.get('pb'),
                        total_mv=row.get('total_mv'), turnover_rate=row.get('turnover_rate')
                    ))
            
            # C. 检查并同步当日披露的财报 (S4 修正版)
            # 这里调用上一张指令卡修复后的 sync_financial_daily
            # 由于 sync_financial_daily 是生成器，需要遍历它
            for msg in self.sync_financial_daily(date_str):
                yield f"    {msg}"
            
            self.db.commit()
            time.sleep(0.5) # 2000积分频次保护 [cite: 345]

        # 3. 统一触发 DWS 重炼 [cite: 140]
        yield "🔄 正在重新炼制 DWS 衍生指标..."
        universe = list(self._get_universe_pool())
        for i, ts_code in enumerate(universe):
            self.process_market_dws(ts_code)
            self.process_finance_dws(ts_code)
            if i % 100 == 0:
                yield f"  > 炼制进度: {i}/{len(universe)}"
        
        yield "✅ 全区间数据补全并炼制完成！"

if __name__ == "__main__":
    u = DataUpdater()
    # u.sync_stock_list()
    u.close()