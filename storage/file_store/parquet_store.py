import pandas as pd
import os
from pathlib import Path
from utils.logger import logger
from config.settings import WAREHOUSE_DIR

class ParquetStore:
    """
    Parquet 存储中心：负责数据的原子性写入与分片管理。
    支持 Hive-style 分区，确保并发读写不冲突。
    """

    def __init__(self):
        self.base_dir = Path(WAREHOUSE_DIR)

    def save_partition(self, df: pd.DataFrame, category: str, symbol: str):
        """
        原子性保存一个 symbol 的分区数据。
        :param df: 数据 Dataframe
        :param category: 类别路径 (例如: 'financial_statements/type=balance')
        :param symbol: 股票代码
        """
        if df.empty:
            return

        # 1. 准备目录 (Hive-style: category/symbol=XXXXXX/)
        target_dir = self.base_dir / category / f"symbol={symbol}"
        target_dir.mkdir(parents=True, exist_ok=True)
        
        target_path = target_dir / "data.parquet"
        temp_path = target_dir / f".tmp_{symbol}.parquet"

        try:
            # 如果 symbol 列在 DF 中，导出时排除它（因为它已在目录名中）
            cols_to_save = [c for c in df.columns if c != 'symbol']
            
            # 2. 写入临时文件
            df[cols_to_save].to_parquet(
                temp_path, 
                engine='pyarrow', 
                compression='snappy', 
                index=False
            )

            # 3. 原子替换
            os.replace(temp_path, target_path)
            
        except Exception:
            logger.exception(f"写入 Parquet 失败 [{symbol}]")
            if temp_path.exists():
                temp_path.unlink()
            raise

    def get_path(self, category: str) -> str:
        """获取某个类别的通配符路径，用于 DuckDB 读取"""
        return str(self.base_dir / category / "*/*.parquet")
