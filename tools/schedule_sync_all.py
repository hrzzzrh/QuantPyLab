"""launchd 定时调度入口: 每日 20:30 执行 sync-all 全流程数据同步。

判定与执行逻辑:
1. 项目根目录不存在 (外置卷未挂载) -> 记日志退出
2. 安装新浪源请求保护层 (幂等, 含交易日历请求)
3. 前一天 (today-1) 非交易日 -> 退出 (零同步请求)
4. 前一天是交易日且已记录 sync-all 成功 (last_sync_date >= 前一天) -> 退出
5. 执行 sync-all 流水线; 未全部成功时整体重试, 重试次数与间隔可配置
   (增量机制自愈: 重跑只补失败部分, 成功部分自动跳过/续传)
6. 全部成功 -> 记录成功 (sync_status 表, 日期=前一天数据日);
   失败/中止/异常 -> 不记录, 次日补跑

由 launchd 以项目 venv 的 python 绝对路径启动, 故本脚本自行把项目根加入 sys.path。
"""

import sys
import time
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import main as sync_main  # noqa: E402
from config.settings import (  # noqa: E402
    SYNC_ALL_MAX_RETRIES,
    SYNC_ALL_RETRY_INTERVAL_SECONDS,
)
from storage.database.manager import db_manager  # noqa: E402
from storage.database.sync_status import (  # noqa: E402
    DATASET_SYNC_ALL,
    SYMBOL_SYNC_ALL,
    get_last_sync_date,
    record_sync_success,
)
from utils.logger import logger  # noqa: E402
from utils.requests_protection import install_requests_protection  # noqa: E402
from utils.trade_date import (  # noqa: E402
    TradeCalendarUnavailableError,
    is_trade_date,
)


def run_sync_all_with_retry() -> int:
    """执行 sync-all 流水线并处理重试, 返回进程退出码 (0=成功, 1=失败/中止)。"""
    if not PROJECT_ROOT.exists():
        logger.error(f"项目根目录不存在 (外置卷未挂载?): {PROJECT_ROOT}, 本轮跳过")
        return 1

    # 对齐 CLI 入口: 安装新浪源伪装头/冷却/IP 风控止损保护层 (幂等),
    # 置于交易日历判定之前, 使交易日历请求同样受保护
    install_requests_protection()

    prev_day = date.today() - timedelta(days=1)
    try:
        prev_day_is_trade_day = is_trade_date(prev_day)
    except TradeCalendarUnavailableError:
        logger.exception("交易日历不可用, 本轮 sync-all 判定失败")
        return 1
    if not prev_day_is_trade_day:
        logger.info(f"前一天 {prev_day} 非交易日, 跳过本轮 sync-all")
        return 0

    status_read_ok, last = _execute_sync_status_with_retry(
        lambda: get_last_sync_date(DATASET_SYNC_ALL, SYMBOL_SYNC_ALL),
        "读取 sync-all 同步状态",
    )
    if not status_read_ok:
        return 1
    if last is not None and last >= prev_day:
        logger.info(f"前一天 {prev_day} 已记录 sync-all 成功 ({last}), 本轮跳过")
        return 0

    max_attempts = SYNC_ALL_MAX_RETRIES + 1
    for attempt in range(1, max_attempts + 1):
        logger.info(f">>> 定时 sync-all 第 {attempt}/{max_attempts} 次尝试 <<<")
        try:
            status = sync_main.sync_all_data_flow()
        except Exception:
            logger.exception("sync-all 流水线异常中止 (视为未成功)")
            status = None
        if status == sync_main.SYNC_ALL_SUCCESS:
            # 记录数据日 (前一天): 保证下一个交易日定时判定时 last < 新 prev_day,
            # 每日数据零延迟入库; 同日重复触发时 last == prev_day 正常跳过
            status_write_ok, _ = _execute_sync_status_with_retry(
                lambda: record_sync_success(
                    DATASET_SYNC_ALL, SYMBOL_SYNC_ALL, prev_day
                ),
                "记录 sync-all 同步状态",
            )
            if not status_write_ok:
                logger.error(
                    ">>> sync-all 已全部完成, 但同步状态写入失败, 本轮判定失败 <<<"
                )
                return 1
            logger.info(">>> 定时 sync-all 全部环节成功, 已记录同步状态 <<<")
            return 0
        if status == sync_main.SYNC_ALL_BLOCKED:
            logger.error(
                ">>> 新浪 IP 风控中止, 本轮不再重试 (等待解封), 未记录成功 <<<"
            )
            return 1
        if attempt < max_attempts:
            logger.warning(
                f">>> 第 {attempt} 次尝试未全部成功, "
                f"{SYNC_ALL_RETRY_INTERVAL_SECONDS}s 后整体重试 <<<"
            )
            time.sleep(SYNC_ALL_RETRY_INTERVAL_SECONDS)
        else:
            logger.error(
                ">>> 已达最大重试次数, 本轮 sync-all 判定失败, 未记录成功 (次日补跑) <<<"
            )
            return 1
    return 1


def _execute_sync_status_with_retry(
    operation: Callable[[], object], action: str
) -> tuple[bool, object]:
    """对 sync_status 的 SQLite 读写执行与流水线一致的有界重试。"""
    max_attempts = SYNC_ALL_MAX_RETRIES + 1
    for attempt in range(1, max_attempts + 1):
        try:
            return True, operation()
        except Exception:
            logger.exception(f"{action}失败 (第 {attempt}/{max_attempts} 次)")
            if attempt < max_attempts:
                time.sleep(SYNC_ALL_RETRY_INTERVAL_SECONDS)
    logger.error(f"{action}达到最大重试次数, 本轮判定失败")
    return False, None


def main() -> int:
    exit_code = run_sync_all_with_retry()
    logger.info(f"定时 sync-all 调度结束, 退出码 {exit_code}")
    return exit_code


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        db_manager.close_all()
