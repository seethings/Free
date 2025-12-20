from nicegui import ui

def theme_setup():
    # 采用更冷静的极简配色：深灰蓝 (#37474f)
    ui.colors(primary='#37474f', secondary='#eceff1', accent='#607d8b')
    ui.query('body').style('font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: #f8fafc;')

def shared_menu():
    """侧边栏导航组件"""
    with ui.left_drawer(value=True).classes('bg-slate-100').props('bordered'):
        ui.label('🏗️ INVEST SYSTEM').classes('text-xl font-bold m-4 text-blue-800')
        ui.separator()
        with ui.column().classes('w-full p-2 gap-2'):
            ui.button('控制台', icon='dashboard', on_click=lambda: ui.navigate.to('/')).props('flat').classes('w-full justify-start')
            ui.button('选股雷达', icon='radar', on_click=lambda: ui.navigate.to('/radar')).props('flat').classes('w-full justify-start')
            ui.button('自选管理', icon='list', on_click=lambda: ui.navigate.to('/watchlist')).props('flat').classes('w-full justify-start')
            ui.button('个股透视', icon='analytics', on_click=lambda: ui.navigate.to('/stock')).props('flat').classes('w-full justify-start')
        
        with ui.column().classes('absolute-bottom w-full p-4 text-slate-400 text-xs'):
            ui.label('V7.3 Architect Edition')
            ui.label('Tushare 2000+ pts Locked')