# FILE PATH: test_radar.py
import sys
import os

# 1. 路径防御：确保脚本能识别根目录下的模块
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from engine.radar import RadarEngine

def run_radar_smoke_test():
    print("📡 === 选股雷达后端引擎 (B阶段) 冒烟测试 ===")
    
    # 2. 初始化引擎
    engine = RadarEngine()
    
    try:
        # 3. 传入 Coach 设定的典型参数进行回测
        # 参数含义：ROE > 10%, PE < 25, 仅限中证800池
        print("🔍 正在扫描中证800成份股 (基于 T-1 数据)...")
        picks = engine.query(
            min_roe=10.0, 
            max_pe=25.0, 
            pool='CSI800',
            trend_up=True  # 过滤掉 20 日均线以下的标的
        )
        
        # 4. 结果验证
        if picks.empty:
            print("⚠️ 未找到符合条件的标的，请检查：")
            print("   - 数据库 DWS 表是否有数据？")
            print("   - 筛选标准是否过严？")
        else:
            print(f"🎯 扫描成功！发现 {len(picks)} 只符合标准的“黄金标的”：")
            # 打印前 10 名，查看关键指标对齐情况
            print("-" * 80)
            print(picks[['ts_code', 'name', 'roe', 'pe_ttm', 'total_mv_unit', 'last_report']].head(10))
            print("-" * 80)

    except Exception as e:
        print(f"❌ 引擎运行报错: {str(e)}")
    finally:
        engine.close()

if __name__ == "__main__":
    run_radar_smoke_test()