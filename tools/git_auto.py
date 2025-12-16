import os
import sys
import subprocess
from datetime import datetime

# --- 配置 ---
BRANCH = "main"
DOC_GEN_SCRIPT = "tools/doc_generator.py"

def run_cmd(cmd, desc, ignore_error=False):
    """执行系统命令"""
    try:
        # capture_output=False 让命令输出直接显示在屏幕上，更有掌控感
        result = subprocess.run(cmd, shell=True, check=True, text=True, capture_output=False)
        return True
    except subprocess.CalledProcessError as e:
        if not ignore_error:
            print(f"❌ 执行失败 [{desc}]: {e}")
        return False

def get_cmd_output(cmd):
    """获取命令返回的文本 (静默执行)"""
    try:
        result = subprocess.run(cmd, shell=True, check=True, text=True, capture_output=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return ""

def auto_save():
    """功能 1: 保存进度"""
    print("\n💾 --- 正在保存进度 ---")
    
    # 1. 生成快照
    if os.path.exists(DOC_GEN_SCRIPT):
        print("1️⃣ 更新 AI 上下文快照...")
        run_cmd(f"python3 {DOC_GEN_SCRIPT}", "生成文档", ignore_error=True)
    
    # 2. Add
    run_cmd("git add .", "添加文件(git add)")
    
    # 3. Commit
    status = get_cmd_output("git status --porcelain")
    if not status:
        print("⚠️ 当前没有文件变动，无需提交。")
        return

    # --- 修改点：默认中文备注 ---
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
    default_msg = f"自动存档: {timestamp}"
    
    user_msg = input(f"✍️ 提交备注 (回车默认: '{default_msg}'): ").strip()
    commit_msg = user_msg if user_msg else default_msg
    
    run_cmd(f'git commit -m "{commit_msg}"', "提交代码(git commit)")
    
    # 4. Push
    print("☁️ 同步到 GitHub...")
    run_cmd(f"git push origin {BRANCH}", "推送到云端(git push)")
    print(f"✅ 保存成功！时间: {timestamp}")

def show_history():
    """功能 2: 查看历史"""
    print("\n📜 --- 最近 10 次存档记录 ---")
    # 格式: Hash | 时间 | 备注 (使用颜色高亮)
    # %C(yellow)%h: 黄色Hash
    # %C(cyan)%cd: 青色时间
    # %s: 提交信息
    cmd = 'git log -n 10 --pretty=format:"%C(yellow)%h%Creset | %C(cyan)%cd%Creset | %s" --date=format:"%m-%d %H:%M"'
    os.system(cmd) 
    print("\n")

def time_travel():
    """功能 3: 时光倒流 (带后悔药机制)"""
    print("\n⏳ --- 时光倒流 (危险区) ---")
    print("此功能可以将项目重置到过去的状态。")
    print("⚠️ 放心：我会先把当前所有文件备份到一个新分支，绝不直接删除！")
    
    # 1. 确认
    confirm = input("确定要回滚吗？(输入 y 确认): ").lower()
    if confirm != 'y':
        return

    # 2. 备份当前烂摊子
    # 分支名只能用英文/数字，但在 commit message 里我们可以写中文
    timestamp_str = datetime.now().strftime('%m%d_%H%M%S')
    broken_branch = f"backup/mess_{timestamp_str}"
    
    print(f"\n🛡️ 正在创建救援备份分支: {broken_branch} ...")
    run_cmd("git add .", "备份当前状态")
    
    # --- 修改点：中文备份备注 ---
    backup_msg = f"[系统] 重置前自动备份 (时间: {datetime.now().strftime('%H:%M:%S')})"
    run_cmd(f'git commit -m "{backup_msg}"', "提交备份", ignore_error=True)
    
    run_cmd(f"git branch {broken_branch}", "创建备份分支")
    print(f"✅ 当前状态已安全保存在分支 [{broken_branch}]。")

    # 3. 选择回滚点
    show_history()
    target_hash = input("\n🎯 请输入你要回到的那个 [Hash码] (例如 a1b2c3d): ").strip()
    
    if not target_hash:
        print("❌ 未输入 Hash，操作取消。")
        return

    # 4. 执行重置
    print(f"\n🚀 正在穿越回 {target_hash} ...")
    if run_cmd(f"git reset --hard {target_hash}", "硬重置(Hard Reset)"):
        print(f"\n✅ 穿越成功！你现在的代码状态已经完全变回了 {target_hash} 的时候。")
        print("⚠️ 注意：如果你修改后要推送到 GitHub，可能需要使用 'git push -f' (强制推送)。")

def main_menu():
    while True:
        print("\n🤖 === Git 智能助理 (Invest System) ===")
        print("1. 💾 保存进度 (Save)")
        print("2. 📜 查看历史 (Log)")
        print("3. 🔙 时光倒流 (Reset)")
        print("0. 🚪 退出 (Exit)")
        
        choice = input("👉 请选择: ").strip()
        
        if choice == '1':
            auto_save()
        elif choice == '2':
            show_history()
        elif choice == '3':
            time_travel()
        elif choice == '0':
            print("Bye! 👋")
            break
        else:
            print("无效选项")

if __name__ == "__main__":
    if not os.path.exists(".gitignore"):
        print("⚠️ 错误：请在项目根目录下运行此脚本！")
    else:
        main_menu()