# FILE PATH: ui/pages/radar.py
from nicegui import ui
from engine.radar import RadarEngine
import pandas as pd
import json
import os
from datetime import datetime
from core.mapping import FIELD_MAPPING

class RadarPage:
    def __init__(self):
        self.engine = RadarEngine()
        self.grid = None
        self.stats_label = None
        self.current_df = pd.DataFrame()

    def update_data(self):
        """核心交互逻辑"""
        try:
            df = self.engine.query(
                min_roe=float(self.roe_slider.value),
                max_pe=float(self.pe_slider.value),
                max_pb=float(self.pb_slider.value),
                min_mv=float(self.mv_slider.value),
                pool=self.pool_select.value,
                trend_up=self.trend_toggle.value
            )
            self.current_df = df
            records = json.loads(df.to_json(orient='records', date_format='iso'))
            
            if self.stats_label:
                count = len(records)
                self.stats_label.set_text(f"🎯 雷达发现: {count} 只标的")
                self.stats_label.classes('text-emerald-600' if count > 0 else 'text-rose-600', remove='text-rose-600 text-emerald-600')

            if self.grid:
                self.grid.options['rowData'] = records
                self.grid.update()
        except Exception as e:
            ui.notify(f"扫描异常: {str(e)}", type='negative')

    def get_export_path(self):
        """Chrome 风格导出路径"""
        downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
        target_dir = os.path.join(downloads_path, "InvestSystem_Exports")
        os.makedirs(target_dir, exist_ok=True)
        return target_dir

    def export_data(self):
        """导出结果"""
        if self.current_df.empty:
            ui.notify("结果为空", type='warning')
            return
        try:
            target_dir = self.get_export_path()
            filename = f"Radar_Picks_{datetime.now().strftime('%m%d_%H%M')}.xlsx"
            filepath = os.path.join(target_dir, filename)
            available_cols = [col for col in self.current_df.columns if col in FIELD_MAPPING]
            export_df = self.current_df[available_cols].rename(columns=FIELD_MAPPING)
            export_df.to_excel(filepath, index=False)
            ui.notify(f"🚀 已导出: {filename}", type='positive')
        except Exception as e:
            ui.notify(f"导出失败: {str(e)}")

    async def add_to_watchlist(self, event):
        """处理表格内的‘加入自选’点击事件"""
        # 仅响应‘操作’列的点击
        if event.args['colId'] != 'action':
            return
        
        row_data = event.args['data']
        ts_code = row_data['ts_code']
        name = row_data['name']
        
        # 实例化 DB 会话（注意：RadarPage 目前未持有 db 实例，建议即用即删）
        from database.models import SessionLocal, Watchlist
        db = SessionLocal()
        
        try:
            # 检查是否已存在 
            exists = db.query(Watchlist).filter(Watchlist.ts_code == ts_code).first()
            if exists:
                ui.notify(f"⚠️ {name} 已在自选池中", type='warning')
                return
                
            # 写入自选 
            new_item = Watchlist(
                ts_code=ts_code, name=name, industry=row_data.get('industry'),
                group_name='雷达发现', weight=1.0
            )
            db.add(new_item)
            db.commit()
            ui.notify(f"🌟 已将 {name} 加入自选池", type='positive')
        except Exception as e:
            db.rollback()
            ui.notify(f"添加失败: {str(e)}", type='negative')
        finally:
            db.close()

    def _create_compact_control(self, label, min_v, max_v, step_v, default_v, unit=""):
        """【架构师重制】优化宽度的水平控制组 """
        with ui.row().classes('items-center gap-3 w-full px-2 py-1 hover:bg-slate-50 rounded transition-all'):
            # 标签区
            ui.label(label).classes('text-[11px] font-bold text-slate-400 uppercase w-12 leading-tight')
            # 滑块区
            slider = ui.slider(min=min_v, max=max_v, step=step_v, value=default_v).classes('flex-grow')
            # 数字框 (扩容至 w-24，并增加外边距)
            number = ui.number(value=default_v, min=min_v, max=max_v, step=step_v, format='%.1f') \
                .props('dense outlined size=10').classes('w-24 text-[12px] bg-white') \
                .bind_value(slider, 'value')
            
            ui.label(unit).classes('text-[10px] text-slate-400 w-4')
            
            slider.on_value_change(self.update_data)
            number.on_value_change(self.update_data)
            return slider

    def content(self):
        # 修改 1：升级为全屏流式布局 (max-w-full)
        with ui.column().classes('w-full p-6 max-w-full mx-auto gap-6 bg-slate-50'):
            # 顶层头部
            with ui.row().classes('w-full items-center justify-between'):
                with ui.row().classes('items-center gap-4'):
                    ui.label('📡 选股雷达').classes('text-2xl font-light text-slate-700')
                    self.pool_select = ui.select(
                        options={'CSI800': '中证800', 'Watchlist': '自选池', 'All': '全市场'},
                        value='CSI800'
                    ).props('dense flat').classes('w-32').on_value_change(self.update_data)
                    self.trend_toggle = ui.switch('趋势向上', value=False).props('dense').on_value_change(self.update_data)
                
                with ui.row().classes('gap-3'):
                    self.stats_label = ui.label('正在预热...').classes('text-sm font-bold font-mono py-1')
                    ui.button(icon='download', on_click=self.export_data).props('flat round dense').tooltip('导出 Excel')

            # 2. 战略参数区
            with ui.card().props('flat bordered').classes('w-full p-2 bg-white rounded-lg shadow-sm'):
                with ui.grid(columns=4).classes('w-full divide-x divide-slate-100'):
                    self.roe_slider = self._create_compact_control('ROE%', 0, 40, 0.5, 10)
                    self.pe_slider = self._create_compact_control('PE', 1, 100, 1, 25)
                    self.pb_slider = self._create_compact_control('PB', 0.1, 10, 0.1, 3)
                    self.mv_slider = self._create_compact_control('市值', 0, 5000, 50, 100, "亿")

            # 3. 结果网格
            with ui.card().props('flat bordered').classes('w-full p-0 overflow-hidden bg-white rounded-lg'):
                self.grid = ui.aggrid({
                    'columnDefs': [
                        {'headerName': '操作', 'field': 'action', 'width': 70, 'pinned': 'left',
                         ':cellRenderer': 'params => "⭐"'},
                        {'headerName': '代码', 'field': 'ts_code', 'width': 100, 'pinned': 'left'},
                        {'headerName': '名称', 'field': 'name', 'width': 110, 'pinned': 'left'},
                        {'headerName': '入选理由/风险提示', 'field': 'selection_reason', 'minWidth': 250,
                         'cellClass': 'text-slate-500 italic text-xs'},
                        {'headerName': '行业', 'field': 'industry', 'width': 130},
                        {'headerName': 'ROE %', 'field': 'roe', 'width': 90, 'sortable': True, 
                         ':valueFormatter': 'params => params.value ? params.value.toFixed(2) : "0.00"',
                         'cellClassRules': {
                             'bg-emerald-50 text-emerald-700 font-bold': 'x >= 20',
                             'text-rose-600': 'x < 10'
                         }},
                        {'headerName': 'PE(TTM)', 'field': 'pe_ttm', 'width': 90, 'sortable': True},
                        {'headerName': 'PB', 'field': 'pb', 'width': 85, 'sortable': True},
                        {'headerName': '净现比', 'field': 'ocf_to_net_profit', 'width': 90, 'sortable': True,
                         ':valueFormatter': 'params => params.value ? params.value.toFixed(2) : "0.00"',
                         'cellClassRules': {
                             'bg-blue-50 text-blue-700': 'x >= 1.2',
                             'text-amber-700': 'x < 0.8'
                         }},
                        {'headerName': '垃圾资产%', 'field': 'toxic_asset_ratio', 'width': 100, 
                         ':valueFormatter': 'params => params.value ? (params.value * 100).toFixed(2) + "%" : "0.00%"'},
                        {'headerName': '市值(亿)', 'field': 'total_mv_unit', 'width': 105, 'sortable': True},
                        {'headerName': '今日涨跌', 'field': 'pct_chg', 'width': 100, 'sortable': True, ':cellRenderer': 'p => p.value > 0 ? "🔴 " + p.value + "%" : "🟢 " + p.value + "%"'},
                        {'headerName': '最新财报', 'field': 'last_report', 'width': 105},
                    ],
                    'rowData': [],
                    'pagination': True,
                    'paginationPageSize': 50,
                    'autoSizeStrategy': {'type': 'fitCellContents'},
                    'theme': 'balham',
                    'defaultColDef': {'sortable': True, 'filter': True, 'resizable': True} # 允许用户手动调节
                }).classes('w-full h-[600px] shadow-lg border-none').on('cellClicked', self.add_to_watchlist)

            # 修改 3：升级版“深蓝审计看板”
            with ui.card().props('flat').classes('w-full bg-slate-900 text-white p-6 rounded-xl'):
                with ui.row().classes('items-center gap-4'):
                    ui.icon('security', color='primary').classes('text-3xl')
                    ui.label('雷达审计逻辑看板 (V7.4 Forensic Edition)').classes('text-xl font-bold tracking-tight')
                
                ui.separator().classes('my-4 bg-slate-700')
                
                with ui.grid(columns=4).classes('w-full gap-8'):
                    with ui.column():
                        ui.label('🛡️ 利润成色').classes('text-blue-400 text-xs font-bold uppercase')
                        ui.label('净现比 ≥ 0.8 (剔除纸面富贵)').classes('text-sm')
                    with ui.column():
                        ui.label('🔍 资产审计').classes('text-blue-400 text-xs font-bold uppercase')
                        ui.label('垃圾资产 < 5% (严防财务黑洞)').classes('text-sm')
                    with ui.column():
                        ui.label('🏰 核心盈利').classes('text-blue-400 text-xs font-bold uppercase')
                        ui.label(f'ROE (扣非) ≥ {self.roe_slider.value}%').classes('text-sm')
                    with ui.column():
                        ui.label('⚖️ 估值边界').classes('text-blue-400 text-xs font-bold uppercase')
                        ui.label(f'PE ≤ {self.pe_slider.value} | PB ≤ {self.pb_slider.value}').classes('text-sm')

            ui.timer(0.5, self.update_data, once=True)