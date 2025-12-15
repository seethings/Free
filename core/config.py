import os
from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv()

class Config:
    # 基础鉴权
    TS_TOKEN = os.getenv("TS_TOKEN")
    DB_URL = os.getenv("DB_URL")
    
    # 系统常量 (PRD 1.3)
    START_DATE = "20150101"
    
    # 完整性检查
    if not TS_TOKEN:
        raise ValueError("❌ 错误: 未在 .env 中找到 TS_TOKEN，请检查配置文件。")
    if not DB_URL:
        raise ValueError("❌ 错误: 未在 .env 中找到 DB_URL，请检查配置文件。")

settings = Config()