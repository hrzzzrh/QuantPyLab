import logging
import sys
from pathlib import Path
from tqdm import tqdm
from config.settings import LOG_DIR

class TqdmLoggingHandler(logging.Handler):
    """
    自定义日志处理器，通过 tqdm.write 输出，避免打断进度条渲染。
    """
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)

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
    
    # 输出到控制台 (使用 tqdm 兼容的 Handler)
    console_handler = TqdmLoggingHandler()
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
