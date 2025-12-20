# FILE PATH: main.py
from nicegui import ui
from ui.layout import theme_setup, shared_menu
from ui.pages.console import ConsolePage
from ui.pages.watchlist import WatchlistPage
from ui.pages.radar import RadarPage

# --- 注意：全局作用域严禁出现 ui.xxx 组件调用 ---

@ui.page('/')
def index_page():
    theme_setup()   # 移动到函数内部
    shared_menu()
    # 实例化并渲染“数据维护”页面内容
    ConsolePage().content()

@ui.page('/radar')
def radar_page():
    theme_setup()   # 移动到函数内部
    shared_menu()
    # 实例化并渲染“选股雷达”页面内容
    RadarPage().content()

@ui.page('/watchlist')
def watchlist_page():
    theme_setup()   # 移动到函数内部
    shared_menu()
    # 实例化并渲染“自选管理”页面内容
    WatchlistPage().content()

# --- 核心修复：修改启动守卫 ---
# 允许 NiceGUI 的多进程 (Multiprocessing) 和重载机制正常运行
if __name__ in {"__main__", "__mp_main__"}:
    ui.run(
        title='Invest System V7.3',
        port=8080,
        reload=True,  # 开启热重载，方便我们实时调试 UI
        dark=False
    )