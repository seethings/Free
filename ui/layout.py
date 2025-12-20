from nicegui import ui

def theme_setup():
    # 采用更冷静的极简配色：深灰蓝 (#37474f)
    ui.colors(primary='#37474f', secondary='#eceff1', accent='#607d8b')
    ui.query('body').style('font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: #f8fafc;')

def shared_menu():
    """侧边栏导航组件 - 边距增强版"""
    with ui.left_drawer(value=True).classes('bg-slate-50').props('bordered'):
        # 标志区
        ui.label('🏗️ INVEST SYSTEM').classes('text-lg font-bold mt-8 mb-4 ml-10 text-blue-900 tracking-tight')
        ui.separator().classes('mx-6')
        
        # 导航菜单
        with ui.column().classes('w-full mt-6 gap-2'):
            # 增加 pl-10 (约 40px) 的左侧内边距，确保文字左对齐且有呼吸感
            ui.button('数据维护', icon='settings', on_click=lambda: ui.navigate.to('/')) \
                .props('flat no-caps').classes('w-full justify-start pl-10 text-slate-600 hover:text-blue-700 font-medium')
            
            ui.button('选股雷达', icon='radar', on_click=lambda: ui.navigate.to('/radar')) \
                .props('flat no-caps').classes('w-full justify-start pl-10 text-slate-600 hover:text-blue-700 font-medium')
            
            ui.button('自选管理', icon='star_border', on_click=lambda: ui.navigate.to('/watchlist')) \
                .props('flat no-caps').classes('w-full justify-start pl-10 text-slate-600 hover:text-blue-700 font-medium')
            
            ui.button('个股透视', icon='insights', on_click=lambda: ui.navigate.to('/stock')) \
                .props('flat no-caps').classes('w-full justify-start pl-10 text-slate-600 hover:text-blue-700 font-medium')
        
        # 底部状态
        with ui.column().classes('absolute-bottom w-full p-6 text-slate-400 text-[10px]'):
            ui.label('V7.3 Architect Edition')
            ui.label('DB FRESHNESS: 2025-12-19') # 动态日期可后续实装