import time
import functools
from utils.logger import logger

def retry(max_retries: int = 2, delay: float = 1.0, exceptions: tuple = (Exception,)):
    """
    通用重试装饰器
    :param max_retries: 最大重试次数 (不含首次执行，默认 2 次，即最多执行 3 次)
    :param delay: 重试间隔 (秒)
    :param exceptions: 触发重试的异常类型
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_retries + 2): # attempt 1 是首次执行
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt <= max_retries:
                        logger.warning(
                            f"执行 {func.__name__} 失败 (第 {attempt} 次尝试): {e}. "
                            f"{delay}s 后进行第 {attempt + 1} 次尝试..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(f"执行 {func.__name__} 最终失败 (已尝试 {attempt} 次): {e}")
            
            if last_exception:
                raise last_exception
        return wrapper
    return decorator
