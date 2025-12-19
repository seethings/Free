# FILE PATH: main.py

import os
import sys
from nicegui import ui

# 路径防御：确保根目录在搜索路径中
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from ui.layout import theme_setup, shared_menu
from ui.pages.console import ConsolePage

@ui.page('/')
def index():
    theme_setup()
    shared_menu()
    # 每次进入页面时实例化，确保获取最新的数据库会话
    page = ConsolePage()
    page.content()

# 预留其他页面路由
@ui.page('/radar')
def radar_page():
    theme_setup()
    shared_menu()
    ui.label('选股雷达 - 正在炼制中...').classes('text-h4 m-6')

# 核心修复：修改启动守护条件
if __name__ in {"__main__", "__mp_main__", "nicegui"}:
    ui.run(
        title='Invest System V7.3',
        port=8080,
        reload=True,   # 开发模式开启热重载
        dark=False,    # 强制浅色模式以对齐设计稿
        show=True      # 启动后自动打开浏览器
    )