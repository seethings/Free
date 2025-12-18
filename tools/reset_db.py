import sys
import os

# 将项目根目录添加到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import engine, Base, init_db

def perform_reset():
    print("⚠️ 正在准备重置数据库...")
    confirm = input("确定要删除所有数据表并重建吗？(输入 'yes' 确认): ")
    
    if confirm.lower() == 'yes':
        try:
            # 1. 物理删除所有表
            print("正在删除旧表...")
            Base.metadata.drop_all(bind=engine)
            
            # 2. 调用模型中的初始化函数创建新表 
            print("正在根据新模型创建表结构...")
            init_db()
            
            print("✅ 数据库重置完成！新的主键约束已生效。")
        except Exception as e:
            print(f"❌ 重置失败: {e}")
    else:
        print("操作已取消。")

if __name__ == "__main__":
    perform_reset()