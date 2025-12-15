import os
import datetime

# --- 配置区域 ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'note')

# 忽略规则
IGNORE_DIRS = {'venv', '__pycache__', '.git', '.idea', '.vscode', 'data', 'temp', 'note', 'dist', 'build'}
IGNORE_FILES = {'.DS_Store', 'poetry.lock', 'package-lock.json', 'LICENSE'}
# 只读取文本代码文件
INCLUDE_EXTENSIONS = ('.py', '.txt', '.md', '.env', '.gitignore', '.ini', '.yaml', '.yml', '.sh')

# 文件注释字典 (辅助 AI 理解文件用途)
FILE_META = {
    "requirements.txt": "Python依赖清单 [核心]",
    ".env": "环境变量/密钥 [核心]",
    ".gitignore": "Git忽略规则 [核心]",
    "core/config.py": "全局配置加载器 [核心]",
    "core/mapping.py": "中英文映射字典 [核心]",
    "database/models.py": "SQLAlchemy数据库模型(ODS/DWS) [核心]",
    "interface/tushare_client.py": "Tushare接口封装(带重试) [核心]",
    "engine/updater.py": "数据更新引擎 [核心]",
    "tools/doc_generator.py": "上下文生成工具 [工具]",
    "tools/db_inspector.py": "数据库体检工具 [工具]",
}

def get_today_seq():
    """生成 mm-dd-NN 格式的编号"""
    today = datetime.datetime.now().strftime("%m-%d")
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    # 查找当日已有的最大序号
    max_seq = 0
    for f in os.listdir(OUTPUT_DIR):
        if f.startswith(f"context_{today}"):
            try:
                # 假设格式: context_12-15-01.txt
                parts = f.split('.')[0].split('-')
                seq = int(parts[-1])
                if seq > max_seq:
                    max_seq = seq
            except:
                pass
    return f"{today}-{max_seq + 1:02d}"

def get_tree_str():
    """生成目录树字符串"""
    lines = []
    lines.append(f"📦 PROJECT STRUCTURE")
    lines.append(f"{'='*30}")
    
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
            
            # 添加注释
            rel_path = os.path.relpath(os.path.join(root, f), PROJECT_ROOT)
            meta = FILE_META.get(rel_path, "")
            desc = f"  # {meta}" if meta else ""
            
            lines.append(f"{indent}    📄 {f}{desc}")
            
    lines.append(f"{'='*30}\n\n")
    return "\n".join(lines)

def generate_context_dump(seq):
    """生成单一整合文件"""
    filename = f"context_{seq}.txt"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    tree_str = get_tree_str()
    
    with open(filepath, 'w', encoding='utf-8') as outfile:
        # Header
        outfile.write(f"# INVEST SYSTEM CONTEXT DUMP\n")
        outfile.write(f"# Date: {datetime.datetime.now()}\n")
        outfile.write(f"# Note: This file contains both the project structure and full code content.\n\n")
        
        # Part 1: Tree
        outfile.write(tree_str)
        
        # Part 2: Content
        outfile.write(f"💻 CODE CONTENT\n")
        outfile.write(f"{'='*30}\n")
        
        file_count = 0
        for root, dirs, files in os.walk(PROJECT_ROOT):
            dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
            
            for f in sorted(files):
                if f in IGNORE_FILES or not f.endswith(INCLUDE_EXTENSIONS):
                    continue
                
                abs_path = os.path.join(root, f)
                rel_path = os.path.relpath(abs_path, PROJECT_ROOT)
                
                # 分隔符：让 AI 容易识别文件边界
                outfile.write(f"\n{'-'*60}\n")
                outfile.write(f"FILE PATH: {rel_path}\n")
                outfile.write(f"{'-'*60}\n")
                
                try:
                    with open(abs_path, 'r', encoding='utf-8') as infile:
                        content = infile.read()
                        outfile.write(content)
                        outfile.write("\n") # 确保文件末尾有换行
                        file_count += 1
                except Exception as e:
                    outfile.write(f"[Error reading file: {e}]\n")

    print(f"✅ 上下文快照已生成: note/{filename}")
    print(f"📊 包含目录结构 + {file_count} 个核心代码文件")

if __name__ == "__main__":
    seq_str = get_today_seq()
    generate_context_dump(seq_str)