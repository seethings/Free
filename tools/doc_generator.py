import os
import datetime

# --- 配置区域 ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'note')

# 1. 忽略目录
IGNORE_DIRS = {
    'venv', '__pycache__', '.git', '.idea', '.vscode', 
    'data', 'temp', 'note', 'dist', 'build', 'logs'
}

# 2. 忽略文件 (.env 绝对不能上传!)
IGNORE_FILES = {
    '.DS_Store', 'poetry.lock', 'package-lock.json', 'LICENSE', 
    '.env', 'requirements.txt.bak'
}

# 3. 允许读取后缀
INCLUDE_EXTENSIONS = ('.py', '.txt', '.md', '.gitignore', '.ini', '.yaml', '.yml', '.sh', '.json')

# 文件注释字典
FILE_META = {
    "requirements.txt": "Python依赖清单 [核心]",
    ".gitignore": "Git忽略规则 [核心]",
    "core/config.py": "全局配置加载器 [核心]",
    "core/mapping.py": "中英文映射字典 [核心]",
    "database/models.py": "SQLAlchemy数据库模型(ODS/DWS) [核心]",
    "interface/tushare_client.py": "Tushare接口封装(带重试) [核心]",
    "engine/updater.py": "数据更新引擎 [核心]",
    "tools/doc_generator.py": "上下文生成工具 [工具]",
    "tools/git_auto.py": "Git自动助理 [工具]",
    "tools/db_inspector.py": "数据库体检工具 [工具]",
}

def get_daily_filename():
    """
    生成每日唯一的上下文文件名
    格式: context_mm-dd.txt
    策略: 每日仅生成一份，多次运行直接覆盖，避免文件爆炸
    """
    today = datetime.datetime.now().strftime("%m-%d")
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    return f"context_{today}.txt"

def get_tree_str():
    """生成目录树字符串"""
    lines = []
    lines.append(f"📦 PROJECT STRUCTURE (Ignored: .env, temp/, note/, data/)")
    lines.append(f"{'='*50}")
    
    for root, dirs, files in os.walk(PROJECT_ROOT):
        # 过滤目录
        dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
        
        level = root.replace(PROJECT_ROOT, '').count(os.sep)
        indent = ' ' * 4 * level
        folder_name = os.path.basename(root)
        if folder_name == os.path.basename(PROJECT_ROOT):
            folder_name = "ROOT"
            
        lines.append(f"{indent}📂 {folder_name}/")
        
        for f in sorted(files):
            if f in IGNORE_FILES:
                continue
            
            # 简单的后缀过滤
            if not any(f.endswith(ext) for ext in INCLUDE_EXTENSIONS) and f not in FILE_META:
                continue

            rel_path = os.path.relpath(os.path.join(root, f), PROJECT_ROOT)
            meta = FILE_META.get(rel_path, "")
            desc = f"  # {meta}" if meta else ""
            
            lines.append(f"{indent}    📄 {f}{desc}")
            
    lines.append(f"{'='*50}\n\n")
    return "\n".join(lines)

def generate_context_dump():
    """生成单一整合文件"""
    filename = get_daily_filename()
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    tree_str = get_tree_str()
    
    with open(filepath, 'w', encoding='utf-8') as outfile:
        # Header
        outfile.write(f"# INVEST SYSTEM CONTEXT DUMP\n")
        outfile.write(f"# Timestamp: {datetime.datetime.now()}\n")
        outfile.write(f"# Security: Sensitive files (.env) and temp dirs are EXCLUDED.\n\n")
        
        # Part 1: Tree
        outfile.write(tree_str)
        
        # Part 2: Content
        outfile.write(f"💻 CODE CONTENT\n")
        outfile.write(f"{'='*50}\n")
        
        file_count = 0
        for root, dirs, files in os.walk(PROJECT_ROOT):
            dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
            
            for f in sorted(files):
                if f in IGNORE_FILES or not f.endswith(INCLUDE_EXTENSIONS):
                    continue
                
                abs_path = os.path.join(root, f)
                rel_path = os.path.relpath(abs_path, PROJECT_ROOT)
                
                outfile.write(f"\n{'-'*60}\n")
                outfile.write(f"FILE PATH: {rel_path}\n")
                outfile.write(f"{'-'*60}\n")
                
                try:
                    with open(abs_path, 'r', encoding='utf-8') as infile:
                        content = infile.read()
                        outfile.write(content)
                        outfile.write("\n")
                        file_count += 1
                except Exception as e:
                    outfile.write(f"[Error reading file: {e}]\n")

    print(f"✅ 上下文快照已更新: note/{filename}")
    print(f"🛡️ 已屏蔽 .env 及临时目录 | 包含 {file_count} 个核心文件")

if __name__ == "__main__":
    generate_context_dump()