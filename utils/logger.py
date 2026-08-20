import logging
import os
from logging.handlers import TimedRotatingFileHandler

from tqdm import tqdm

from config.settings import LOG_DIR, LOG_RETENTION_DAYS


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


def setup_logger(
    name: str = "QuantPyLab",
    log_file: str = "app.log",
    level=logging.INFO,
    enable_file_handlers: bool | None = None,
):
    """设置项目全局日志器"""
    logger = logging.getLogger(name)

    logger.setLevel(level)

    if enable_file_handlers is None:
        enable_file_handlers = os.getenv("QUANTPYLAB_DISABLE_FILE_LOGGING") != "1"

    if not logger.handlers:
        # 输出到控制台 (使用 tqdm 兼容的 Handler)
        console_handler = TqdmLoggingHandler()
        logger.addHandler(console_handler)

    for handler in list(logger.handlers):
        if isinstance(handler, TimedRotatingFileHandler):
            logger.removeHandler(handler)
            handler.close()

    # 格式化
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    for handler in logger.handlers:
        if isinstance(handler, TqdmLoggingHandler):
            handler.setFormatter(formatter)

    if not enable_file_handlers:
        return logger

    # 输出到文件 (全量, 按天轮转, 保留最近 LOG_RETENTION_DAYS 天)
    file_handler = TimedRotatingFileHandler(
        LOG_DIR / log_file,
        when="midnight",
        backupCount=LOG_RETENTION_DAYS,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 输出到文件 (仅 Error, 按天轮转)
    error_file_handler = TimedRotatingFileHandler(
        LOG_DIR / "error.log",
        when="midnight",
        backupCount=LOG_RETENTION_DAYS,
        encoding="utf-8",
    )
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(formatter)
    logger.addHandler(error_file_handler)

    return logger


# 创建默认实例
logger = setup_logger()
