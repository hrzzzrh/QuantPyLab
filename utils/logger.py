import logging
import sys
from pathlib import Path
from config.settings import LOG_DIR

def setup_logger(name: str = "QuantPyLab", log_file: str = "app.log", level=logging.INFO):
    """设置项目全局日志器"""
    logger = logging.getLogger(name)
    
    if logger.handlers:
        return logger
        
    logger.setLevel(level)
    
    # 格式化
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 输出到控制台
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 输出到文件 (全量)
    file_handler = logging.FileHandler(LOG_DIR / log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # 输出到文件 (仅 Error)
    error_file_handler = logging.FileHandler(LOG_DIR / "error.log")
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(formatter)
    logger.addHandler(error_file_handler)
    
    return logger

# 创建默认实例
logger = setup_logger()
