"""
配置文件 - 部署到服务器后修改此文件
"""

# ==================== WorldQuant Brain 登录配置 ====================

# 方式1: 直接在这里配置（不推荐，安全性低）
WQ_EMAIL = ""           # 您的WorldQuant Brain邮箱
WQ_PASSWORD = ""        # 您的WorldQuant Brain密码

# 方式2: 使用环境变量（推荐）
# export WQ_EMAIL="your_email@example.com"
# export WQ_PASSWORD="your_password"

# 方式3: 使用.brain_credentials文件（最推荐）
# 在当前目录创建.brain_credentials文件，内容：
# email=your_email@example.com
# password=your_password


# ==================== 数据集配置 ====================

DATASET_ID = 'news20'                    # 数据集ID
REGION = 'GLB'                           # 区域: 'USA', 'EUR', 'CHN', 'GLB', 'ASI'
UNIVERSE = 'MINVOL1M'                    # 宇宙: 'TOP3000', 'TOP2500', 'MINVOL1M'等
DELAY = 1                                # 延迟: 0或1
DATA_TYPE = 'VECTOR'                     # 数据类型: 'MATRIX'或'VECTOR'


# ==================== 模拟配置 ====================

# 中性化配置（数组，可配置多个，会逐个执行）
NEUTRALIZATIONS = ["SUBINDUSTRY", "INDUSTRY", "SECTOR", "MARKET", "STATISTICAL"]
# 可选值:
# - "SUBINDUSTRY": 子行业中性化（最常用）
# - "INDUSTRY": 行业中性化
# - "SECTOR": 板块中性化
# - "MARKET": 市场中性化
# - "STATISTICAL": 统计中性化

INIT_DECAY = 6                           # 初始衰减值
TASK_POOL_SIZE = 2                       # 任务池数量（建议≥9）
CONCURRENT_SIMS = 6                      # 并发模拟数（建议≥8，每批至少2个）


# ==================== 字段范围配置 ====================

START_FIELD_INDEX = 0                    # 起始字段索引
END_FIELD_INDEX = 100                    # 结束字段索引


# ==================== 日志配置 ====================

LOG_LEVEL = 'INFO'                       # 日志级别: DEBUG, INFO, WARNING, ERROR
LOG_FILE = 'brain_simulation.log'       # 日志文件名


# ==================== 高级配置 ====================

# 是否使用代理
USE_PROXY = False
HTTP_PROXY = ''
HTTPS_PROXY = ''

# API请求超时（秒）
API_TIMEOUT = 300

# 重试次数
MAX_RETRIES = 3

