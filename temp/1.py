import sys
import os

# 1. 将项目根目录 (ROOT) 添加到 Python 的搜索路径中
# 这行代码会获取当前脚本的父目录的父目录，即 /Users/ze/Projects/Free/
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 2. 现在可以安全导入了
from engine.updater import DataUpdater

# 你的测试代码...


# 临时运行一个测试同步
from engine.updater import DataUpdater
up = DataUpdater()
up.sync_stock_history("600519.SH") # 工业代表
up.sync_stock_history("600036.SH") # 银行代表