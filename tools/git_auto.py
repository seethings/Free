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
        # capture_output=False 让命令输出直接显示在屏幕上
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
    
    # 3. Commit 前的检查
    status_short = get_cmd_output("git status --short")
    if not status_short:
        print("⚠️ 当前没有文件变动，无需提交。")
        # 即使没有变动，如果云端滞后，用户可能只想 push，所以不直接 return
        # 但通常 save 是为了存新东西。这里我们继续走，方便单纯的 push 操作。
    else:
        print("\n📝 检测到以下文件变动：")
        print("-" * 30)
        print(status_short)
        print("-" * 30)

        # 4. 获取备注
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
        default_msg = f"自动存档: {timestamp}"
        
        print(f"💡 提示：输入具体修改内容可方便日后回溯")
        user_msg = input(f"✍️ 提交备注 (直接回车 = '{default_msg}'): ").strip()
        commit_msg = user_msg if user_msg else default_msg
        
        # 5. 执行提交
        run_cmd(f'git commit -m "{commit_msg}"', "提交代码(git commit)")
    
    # 6. Push
    print("☁️ 同步到 GitHub...")
    if run_cmd(f"git push origin {BRANCH}", "推送到云端(git push)", ignore_error=True):
        print(f"✅ 保存成功！")
    else:
        print("⚠️ 普通推送失败！这通常是因为你回滚过版本。")
        print("💡 建议：请尝试使用主菜单的 [4] 强制同步。")

def show_history():
    """功能 2: 查看历史"""
    print("\n📜 --- 最近 10 次存档记录 ---")
    cmd = 'git log -n 10 --pretty=format:"%C(yellow)%h%Creset | %C(cyan)%cd%Creset | %s" --date=format:"%m-%d %H:%M"'
    os.system(cmd) 
    print("\n")

def time_travel():
    """功能 3: 时光倒流"""
    print("\n⏳ --- 时光倒流 (危险区) ---")
    print("此功能可以将项目重置到过去的状态。")
    
    confirm = input("确定要回滚吗？(输入 y 确认): ").lower()
    if confirm != 'y':
        return

    timestamp_str = datetime.now().strftime('%m%d_%H%M%S')
    broken_branch = f"backup/mess_{timestamp_str}"
    
    print(f"\n🛡️ 正在创建救援备份分支: {broken_branch} ...")
    run_cmd("git add .", "备份当前状态")
    
    backup_msg = f"[系统] 重置前自动备份 (时间: {datetime.now().strftime('%H:%M:%S')})"
    run_cmd(f'git commit -m "{backup_msg}"', "提交备份", ignore_error=True)
    
    run_cmd(f"git branch {broken_branch}", "创建备份分支")
    print(f"✅ 当前状态已安全保存在分支 [{broken_branch}]。")

    show_history()
    target_hash = input("\n🎯 请输入你要回到的那个 [Hash码] (例如 a1b2c3d): ").strip()
    
    if not target_hash:
        print("❌ 未输入 Hash，操作取消。")
        return

    print(f"\n🚀 正在穿越回 {target_hash} ...")
    if run_cmd(f"git reset --hard {target_hash}", "硬重置(Hard Reset)"):
        print(f"\n✅ 穿越成功！")
        print("⚠️ 注意：你需要使用主菜单的 [4] 强制同步 才能把这个变更推送到云端。")

def force_sync():
    """功能 4: 强制同步 (新增)"""
    print("\n☢️ --- 暴力同步 (强制覆盖云端) ---")
    print("⚠️ 警告：这会强制将 GitHub 上的代码替换为你现在本地的样子。")
    print("⚠️ 适用场景：当你执行过 [时光倒流] 后，普通保存报错时。")
    
    confirm = input("❓ 确定要执行吗？(输入 yes 确认): ").strip()
    if confirm != "yes":
        print("已取消。")
        return

    print(f"🚀 正在强制推送 (Force Push) 到 {BRANCH} 分支...")
    if run_cmd(f"git push -f origin {BRANCH}", "强制推送"):
        print("\n✅ 云端已强制同步！现在 GitHub 和你本地完全一致了。")

def main_menu():
    while True:
        print("\n🤖 === Git 智能助理 (Invest System) ===")
        print("1. 💾 保存进度 (Save)  -> 日常使用")
        print("2. 📜 查看历史 (Log)   -> 看看干了啥")
        print("3. 🔙 时光倒流 (Reset) -> 救命用的")
        print("4. ☢️ 强制同步 (Force) -> 专治报错")
        print("0. 🚪 退出 (Exit)")
        
        choice = input("👉 请选择: ").strip()
        
        if choice == '1':
            auto_save()
        elif choice == '2':
            show_history()
        elif choice == '3':
            time_travel()
        elif choice == '4':
            force_sync()
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