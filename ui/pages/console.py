# FILE PATH: ui/pages/console.py

from nicegui import ui
from engine.updater import DataUpdater
import asyncio
from datetime import datetime

class ConsolePage:
    def __init__(self):
        self.updater = DataUpdater()
        self.log_view = None

    async def run_task(self, task_func):
        """通用异步任务处理器"""
        if self.log_view:
            self.log_view.push(f"[{datetime.now().strftime('%H:%M:%S')}] 🚀 启动...")
        try:
            # 这里的 task_func 是 updater 中的生成器函数
            for message in task_func():
                self.log_view.push(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
                # 强制 UI 刷新，防止日志堆积导致的浏览器卡顿
                await asyncio.sleep(0.01)
        except Exception as e:
            self.log_view.push(f"❌ 运行异常: {str(e)}")

    def content(self):
        with ui.column().classes('w-full p-8 max-w-6xl mx-auto'):
            # 标题更名：从“系统控制台”改为“数据维护”
            ui.label('⚙️ 数据维护').classes('text-3xl font-light text-slate-700 mb-8')
            
            # 四磁贴布局 (对齐你的截图样式)
            with ui.row().classes('w-full gap-6'):
                
                # 磁贴 1: 日常同步 (S3/S4)
                with ui.card().props('flat bordered').classes('p-6 flex-1 bg-white'):
                    ui.label('日常同步').classes('text-xs text-slate-400 uppercase tracking-widest')
                    ui.label('收盘数据补全').classes('text-lg font-medium mb-4')
                    ui.button('一键日更', on_click=lambda: self.run_task(self.updater.run_daily_routine)) \
                        .props('flat color=primary').classes('px-4 border border-slate-200')

                # 磁贴 2: 元数据同步 (CSI800)
                with ui.card().props('flat bordered').classes('p-6 flex-1 bg-white'):
                    ui.label('底座维护').classes('text-xs text-slate-400 uppercase tracking-widest')
                    ui.label('同步成分股').classes('text-lg font-medium mb-4')
                    ui.button('同步 CSI800', on_click=lambda: self.run_task(self.updater.sync_stock_list)) \
                        .props('flat color=primary').classes('px-4 border border-slate-200')

                # 磁贴 3: 初始化 (S5)
                with ui.card().props('flat bordered').classes('p-6 flex-1 bg-white'):
                    ui.label('初始化').classes('text-xs text-slate-400 uppercase tracking-widest')
                    ui.label('核心池全回溯').classes('text-lg font-medium mb-4')
                    ui.button('开始回溯', on_click=lambda: self.run_task(self.updater.run_full_backfill)) \
                        .props('flat color=primary').classes('px-4 border border-slate-200')

                # 磁贴 4: 专项同步 (S1/S2) - 修正点
                with ui.card().props('flat bordered').classes('p-6 flex-1 bg-white'):
                    ui.label('专项同步').classes('text-xs text-slate-400 uppercase tracking-widest')
                    ui.label('自选池深度同步').classes('text-lg font-medium mb-4')
                    ui.button('立即同步自选池', on_click=lambda: self.run_task(self.updater.run_watchlist_backfill)) \
                        .props('flat color=primary').classes('px-4 border border-slate-200')

            # 极简日志区
            ui.label('📡 实时日志').classes('text-sm font-medium text-slate-500 mt-12 mb-2')
            with ui.card().props('flat').classes('w-full bg-slate-900 overflow-hidden rounded-lg'):
                self.log_view = ui.log().classes('w-full h-80 text-emerald-400 font-mono text-[11px] p-6')