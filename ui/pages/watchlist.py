from nicegui import ui
from database.models import SessionLocal, Watchlist, StockBasic
from core.mapping import FIELD_MAPPING
from datetime import datetime
from sqlalchemy import or_

class WatchlistPage:
    def __init__(self):
        self.db = SessionLocal()
        self.grid = None
        # 预加载股票字典用于搜索提示 (代码 + 名称)
        self.stock_options = self._get_search_options()

    def _get_search_options(self):
        """缓存全量股票列表用于下拉提示"""
        stocks = self.db.query(StockBasic).all()
        return {s.ts_code: f"{s.symbol} | {s.name}" for s in stocks}

    def _fetch_data(self):
        """读取数据并按权重排序 """
        rows = self.db.query(Watchlist).order_by(Watchlist.weight.desc()).all()
        return [
            {
                'ts_code': r.ts_code,
                'name': r.name,
                'industry': r.industry,
                'group_name': r.group_name or '默认',
                'weight': r.weight,
                'add_time': r.add_time.strftime('%Y-%m-%d')
            } for r in rows
        ]

    async def update_cell(self, event):
        """行内编辑同步至数据库"""
        row_data = event.args['data']
        field = event.args['colId']
        new_val = event.args['newValue']
        
        target = self.db.query(Watchlist).filter(Watchlist.ts_code == row_data['ts_code']).first()
        if target:
            setattr(target, field, new_val)
            self.db.commit()
            ui.notify(f"已更新 {target.name} 的{field}")

    async def add_stock(self, value):
        """增强版添加逻辑：处理下拉选择的值或手动输入的值"""
        if not value:
            return
            
        # 如果用户选的是提示项，value 是 ts_code；如果是盲打输入，value 也是字符串
        ts_code = value.upper().strip()
        
        # 基础校验与写入逻辑 (复用之前逻辑)
        basic = self.db.query(StockBasic).filter(StockBasic.ts_code == ts_code).first()
        if not basic:
            ui.notify(f'标的不存在，请检查代码格式', type='negative')
            return
            
        if self.db.query(Watchlist).filter(Watchlist.ts_code == ts_code).first():
            ui.notify(f'{basic.name} 已在自选池中', type='info')
            return

        new_item = Watchlist(
            ts_code=basic.ts_code, name=basic.name, 
            industry=basic.industry, weight=1.0, group_name='核心观望'
        )
        self.db.add(new_item)
        self.db.commit()
        ui.notify(f'✅ 已成功添加: {basic.name}', type='positive')
        self.update_grid()

    def update_grid(self):
        if self.grid:
            self.grid.options['rowData'] = self._fetch_data()
            self.grid.update()

    def content(self):
        with ui.column().classes('w-full p-8 max-w-7xl mx-auto'):
            ui.label('🌟 自选池管理').classes('text-3xl font-light text-slate-700 mb-6')

            # 🛠️ 交互修正：搜索框 + 按钮 + 美化后的删除按钮
            with ui.row().classes('w-full items-center gap-4 mb-6 bg-white p-4 rounded-lg border border-slate-100 shadow-sm'):
                
                # 搜索框部分
                search_box = ui.select(
                    options=self.stock_options, 
                    with_input=True, 
                    label='输入代码 (如 600519.SH) 或名称',
                ).classes('w-96').props('use-input fill-input hide-selected outlined dense')
                
                # 添加按钮
                ui.button('添加', icon='add', on_click=lambda: self.add_stock(search_box.value)) \
                    .props('flat color=primary').classes('px-4 border border-slate-200 rounded-md')

                search_box.on('keydown.enter', lambda: self.add_stock(search_box.value))

                ui.label('💡 提示：双击表格修改分组/权重').classes('text-xs text-slate-400 ml-2')
                
                # --- 美化后的删除按钮 ---
                # 初始状态为 flat 红色，带删除图标
                self.delete_btn = ui.button('移除选中', icon='delete_outline', on_click=self.confirm_delete) \
                    .props('flat color=red').classes('ml-auto px-4 hover:bg-red-50 rounded-md transition-all text-sm font-medium')

            # AG Grid
            self.grid = ui.aggrid({
                'columnDefs': [
                    {'headerName': '代码', 'field': 'ts_code', 'checkboxSelection': True, 'headerCheckboxSelection': True},
                    {'headerName': '名称', 'field': 'name'},
                    {'headerName': '分组', 'field': 'group_name', 'editable': True, 'cellClass': 'bg-blue-50'},
                    {'headerName': '权重', 'field': 'weight', 'editable': True, 'cellClass': 'bg-green-50', 'sort': 'desc'},
                    {'headerName': '所属行业', 'field': 'industry'},
                ],
                'rowData': self._fetch_data(),
                'rowSelection': 'multiple',
                'theme': 'balham',
                'stopEditingWhenCellsLoseFocus': True
            }).classes('w-full h-[600px] bg-white rounded-lg shadow-sm').on('cellValueChanged', self.update_cell)

    async def confirm_delete(self):
        """弹出二次确认对话框"""
        selected = await self.grid.get_selected_rows()
        if not selected:
            ui.notify('请先在左侧勾选要移除的股票', type='warning')
            return

        with ui.dialog() as dialog, ui.card().classes('p-6'):
            ui.label(f'⚠️ 确定要从自选池移除这 {len(selected)} 只股票吗？').classes('text-lg font-medium')
            ui.label('此操作将删除您配置的分组与权重信息。').classes('text-sm text-slate-500 mb-4')
            with ui.row().classes('w-full justify-end gap-2'):
                ui.button('取消', on_click=dialog.close).props('flat')
                ui.button('确定移除', color='red', on_click=lambda: self.execute_delete(selected, dialog)).props('unelevated')
        dialog.open()

    async def execute_delete(self, selected, dialog):
        """执行实际物理删除"""
        for row in selected:
            self.db.query(Watchlist).filter(Watchlist.ts_code == row['ts_code']).delete()
        self.db.commit()
        dialog.close()
        ui.notify(f'🗑️ 已成功移除所选标的', type='info')
        self.update_grid()

    def __del__(self):
        self.db.close()