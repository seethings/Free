# FILE PATH: ui/pages/console.py

from nicegui import ui
from engine.updater import DataUpdater
import asyncio
from datetime import datetime

class ConsolePage:
    def __init__(self):
        self.updater = DataUpdater()
        self.log_view = None

    async def run_task(self, task_func, *args):
        """通用任务运行器，支持异步和生成器"""
        if self.log_view:
            self.log_view.push(f"[{datetime.now().strftime('%H:%M:%S')}] 🚀 启动任务...")
        
        try:
            # 检查是否为生成器函数 (yield)
            result = task_func(*args)
            if hasattr(result, '__iter__'):
                for message in result:
                    self.log_view.push(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
                    await asyncio.sleep(0.01)
            else:
                # 普通函数直接执行
                self.log_view.push(f"[{datetime.now().strftime('%H:%M:%S')}] 执行中...")
                # 注意：此处暂不处理耗时极长的同步函数，未来可放入线程池
        except Exception as e:
            self.log_view.push(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ 错误: {str(e)}")

    def content(self):
        with ui.column().classes('w-full p-6'):
            ui.label('⚙️ 系统控制台').classes('text-2xl font-bold mb-4')
            
            # 操作区：Grid 布局对齐 PRD 3.2
            with ui.row().classes('w-full gap-4'):
                
                # 磁贴 1: 日常同步 (S3/S4)
                with ui.card().classes('p-4 w-64 hover:shadow-lg border-l-4 border-blue-500'):
                    ui.label('收盘自动化 (S3/S4)').classes('font-bold text-gray-700')
                    ui.button('一键日更', on_click=lambda: self.run_task(self.updater.run_daily_routine)) \
                        .props('unelevated color=blue')

                # 磁贴 2: 成分股维护
                with ui.card().classes('p-4 w-64 hover:shadow-lg border-l-4 border-teal-500'):
                    ui.label('指数成分股同步').classes('font-bold text-gray-700')
                    ui.button('同步 CSI800', on_click=lambda: self.run_task(self.updater.sync_stock_list)) \
                        .props('unelevated color=teal')

                # 磁贴 3: 自选股修补 (S1/S2)
                with ui.card().classes('p-4 w-64 hover:shadow-lg border-l-4 border-orange-500'):
                    ui.label('自选池修补 (S1/S2)').classes('font-bold text-gray-700')
                    # 这里封装一个简单的逻辑来遍历自选股并同步
                    ui.button('修补自选数据', on_click=lambda: ui.notify('该功能将调用 sync_stock_history')) \
                        .props('outline color=orange')

                # 磁贴 4: 初始化 (S5)
                with ui.card().classes('p-4 w-64 hover:shadow-lg border-l-4 border-red-500'):
                    ui.label('全量初始化 (S5)').classes('font-bold text-gray-700')
                    ui.button('开始回溯', on_click=lambda: self.run_task(self.updater.run_full_backfill)) \
                        .props('unelevated color=red')

            # 日志终端
            ui.label('📡 实时执行日志').classes('text-lg font-semibold mt-8')
            with ui.card().classes('w-full p-0 overflow-hidden'):
                self.log_view = ui.log().classes('w-full h-96 bg-gray-900 text-green-400 font-mono text-xs p-4')