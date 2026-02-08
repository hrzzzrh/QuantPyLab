# 技术方案 RFC：存储层并发访问优化 (Parquet Data Lake)

| 属性 | 内容 |
| :--- | :--- |
| **状态** | Draft (待评审) |
| **目标** | 解决 DuckDB 单进程写锁导致的并发阻塞问题，实现“边同步、边分析”。 |
| **涉及模块** | `storage`, `data_ingestion`, `analysis` |

## 1. 背景与挑战 (Context)

当前系统使用 DuckDB (`.duckdb` 文件) 作为核心存储。虽然 DuckDB 查询性能极佳，但其作为嵌入式数据库，默认采用**文件级独占锁 (File-level Locking)**。

-   **痛点**：当 `data_ingestion` 进程（写入者）启动时，它会锁定数据库文件。此时，用户无法启动 Jupyter Notebook 或运行策略脚本（读取者）进行分析，必须等待同步完全结束。
-   **需求**：实现**读写分离**与**非阻塞并发**。同步进程在更新数据时，不应影响分析进程的读取操作。

## 2. 架构决策 (Architecture Decision)

我们将从“单体数据库模式”迁移至“基于文件系统的**数据湖 (Data Lake)** 模式”。

| 维度 | 当前架构 (DuckDB Monolith) | 目标架构 (Parquet Data Lake) | 优势 |
| :--- | :--- | :--- | :--- |
| **存储介质** | 单个/少数 `.duckdb` 文件 | 分散的 `.parquet` 文件群 | 彻底解耦，OS 级并发控制 |
| **写模式** | SQL `INSERT/UPDATE` (需持锁) | 文件覆盖 (Atomic Replace) | 写操作不阻塞读操作 |
| **读模式** | SQL Query (需持锁) | DuckDB `read_parquet` (无锁) | 支持无限个并发读取者 |
| **索引管理** | 数据库内部 B-Tree | 目录分区 (`symbol=000001/`) | 利用文件系统目录做天然索引 |

## 3. 详细设计 (Detailed Design)

### 3.1 总体架构图

```plantuml
@startuml
skinparam componentStyle uml2

package "Write Path (同步进程)" {
    [Data Collector] as Collector
    [ParquetStore] as Store
}

package "Storage Layer (文件系统)" {
    folder "data/" {
        folder "financial/" {
            file "symbol=000001.parquet"
            file "symbol=000002.parquet"
        }
        folder "indicators/" {
            file "symbol=000001.parquet"
        }
    }
}

package "Read Path (分析进程)" {
    [Strategy Engine] as Strategy
    [Jupyter Notebook] as Notebook
    database "DuckDB (In-Memory)" as DuckEngine
}

Collector -> Store : DataFrame
Store -> "data/" : 1. Write Temp File
2. Atomic Rename
"data/" .up.> DuckEngine : Read
Strategy -> DuckEngine : SQL Query
Notebook -> DuckEngine : SQL Query

note right of "data/"
  Parquet 文件是不可变的 (Immutable)。
  "修改"数据 = "替换"文件。
end note

@enduml
```

### 3.2 读写并发时序图 (关键路径)

此图展示了为何该方案不会产生锁冲突，以及如何保证数据一致性。

```plantuml
@startuml
participant "Sync Process
(Writer)" as Writer
participant "File System
(OS)" as FS
participant "Analysis Process
(Reader)" as Reader

== T0: 初始状态 ==
FS -> FS: 存在文件 000001.parquet (V1)

== T1: 分析进程读取 ==
Reader -> FS: DuckDB 读取 '000001.parquet'
activate Reader
Reader -> Reader: 加载 V1 数据到内存...

== T2: 同步进程更新 (并发) ==
Writer -> Writer: 抓取最新数据 (V2)
Writer -> FS: 写入 000001.parquet.tmp (新文件)
note right: 写临时文件完全不影响
正在被读取的原文件

== T3: 原子提交 ==
Writer -> FS: **os.replace(.tmp, .original)**
note right: Linux/Mac 下 Rename 是原子操作。
原文件句柄被替换，但在读进程
释放前，旧 inode 依然有效。

== T4: 分析进程再次读取 ==
deactivate Reader
Reader -> FS: DuckDB 读取 '000001.parquet'
activate Reader
Reader -> Reader: 此时读到的是 V2 数据
deactivate Reader

@enduml
```

### 3.3 目录结构 (Schema Design)

采用 Hive-style 分区结构，便于 DuckDB 利用 `Hive Partitioning` 自动识别列。

```text
data/
├── warehouse/                 # 核心数仓
│   ├── financial_statements/  # 财务报表 (原 fin_balance_sheet 等)
│   │   ├── type=balance/
│   │   │   ├── symbol=000001.parquet
│   │   │   └── symbol=600519.parquet
│   │   └── type=income/ ...
│   │
│   └── indicators/            # 财务指标 (原 fin_indicator)
│       ├── symbol=000001.parquet  <-- 按股票代码物理隔离
│       ├── symbol=000002.parquet
│       └── ...
└── analysis.duckdb            # (可选) 仅存储 View 定义，不存数据
```

## 4. 核心代码实现 (Pseudo-code)

### 4.1 原子写入器 (`storage/file_store/parquet_writer.py`)

```python
import pandas as pd
import os
from pathlib import Path

class ParquetStore:
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)

    def write_atomic(self, df: pd.DataFrame, partition_col: str, partition_val: str, category: str):
        """
        原子写入：先写 temp，再 rename。
        确保分析进程永远读不到"写了一半"的损坏文件。
        """
        # 1. 确定目标路径
        target_dir = self.base_dir / category
        target_dir.mkdir(parents=True, exist_ok=True)
        
        filename = f"{partition_col}={partition_val}.parquet"
        target_path = target_dir / filename
        temp_path = target_dir / f".tmp_{filename}"

        # 2. 写入临时文件
        # engine='pyarrow' 性能通常优于 fastparquet
        df.to_parquet(temp_path, engine='pyarrow', compression='snappy')

        # 3. 原子替换 (Critical Section)
        # 在 POSIX 系统上，os.replace 是原子的
        os.replace(temp_path, target_path)
```

### 4.2 动态视图挂载 (`storage/database/view_manager.py`)

在分析端，我们不需要手动加载每个文件，而是使用 DuckDB 强大的正则匹配和视图功能。

```python
class AnalysisEngine:
    def init_views(self):
        """
        将 Parquet 目录映射为 DuckDB 视图，操作体验与原表一致。
        """
        # hive_partitioning=1 允许 DuckDB 自动从文件名/目录名提取 symbol 列
        sql = """
        CREATE OR REPLACE VIEW fin_indicators AS 
        SELECT * 
        FROM read_parquet('data/warehouse/indicators/*.parquet', hive_partitioning=1);
        """
        self.conn.execute(sql)
```

## 5. 迁移计划 (Migration Plan)

为了平滑过渡，建议分两步走：

1.  **Phase 1 (双写期)**：
    *   保留现有 DuckDB 写入逻辑。
    *   新增 `ParquetStore`，同步脚本同时写入 Parquet 文件。
    *   用户开始尝试连接 Parquet 进行分析验证。

2.  **Phase 2 (切换期)**：
    *   将 `storage/database/manager.py` 的默认读取源切换为 Parquet 视图。
    *   停止向 DuckDB `.duckdb` 写入数据，仅保留其作为元数据（SQLite）的挂载点。

## 6. FAQ

**Q: 只有一万多只股票，文件会不会太多？**
A: A 股约 5000+ 股票。5000 个 Parquet 文件对于现代文件系统（APFS/EXT4）是九牛一毛。且按 `symbol` 分割能最大程度减少同步时的写冲突（每只股票只锁自己的文件）。

**Q: 查询性能会下降吗？**
A: 对于全量扫描（如“计算全市场平均 PE”），Parquet 性能略低于已预热的 DuckDB 内部表，但在 SSD 上差异极小（毫秒级）。对于单股查询（“查茅台过去10年营收”），Parquet 配合 Partition Pruning（分区裁剪）甚至更快，因为只需读取一个微型文件。

**Q: 如何处理 Schema 变更（如新财务指标）？**
A: DuckDB 的 `read_parquet` 支持 `union_by_name=True` 选项。如果不同 Parquet 文件的列不一致，DuckDB 会自动取并集并填补 NULL，完美兼容该项目的动态指标特性。
