"""Build a lightweight, deterministic data snapshot for research reports."""

import hashlib
import json
from pathlib import Path

from config.settings import WAREHOUSE_DIR

_SNAPSHOT_DATASET_ROOTS = (
    "daily_kline",
    "etf_kline",
    "financial",
    "indicators",
    "industry_classification_sw",
    "share_capital",
)


class ResearchDataSnapshotChangedError(RuntimeError):
    """Raised when source data changes during one research evaluation."""


def build_research_data_snapshot(database_manager) -> dict[str, object]:
    """Return provenance without recursively scanning the data lake.

    The metadata database hash captures stock metadata and sync status. Known
    warehouse root statistics add a cheap signal for atomic partition changes;
    the function intentionally does not enumerate the potentially huge
    partition tree or hash every Parquet file.
    """

    metadata_path = _get_metadata_path(database_manager)
    if metadata_path is None or not metadata_path.is_file():
        return {
            "snapshot_id": "unavailable",
            "status": "unavailable",
            "reason": "metadata.db 不存在或数据库管理器未提供 sqlite_path",
        }

    metadata_digest = _sha256_file(metadata_path)
    sync_status, sync_status_digest, sync_status_row_count = _read_sync_status(
        database_manager
    )
    warehouse_roots = _read_warehouse_root_stats(database_manager)
    fingerprint_payload = {
        "metadata_db_sha256": metadata_digest,
        "sync_status_sha256": sync_status_digest,
        "sync_status_row_count": sync_status_row_count,
        "sync_status": sync_status,
        "warehouse_roots": warehouse_roots,
    }
    fingerprint = hashlib.sha256(
        json.dumps(
            fingerprint_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "snapshot_id": f"data-{fingerprint[:16]}",
        "status": "best_effort",
        "content_addressed": False,
        "reason": (
            "当前仓库没有逐 Parquet 内容清单；快照绑定 metadata.db、完整 "
            "sync_status 状态和已知数据集根目录，只能检测受管同步与目录切换"
        ),
        "metadata_db_sha256": metadata_digest,
        "sync_status_sha256": sync_status_digest,
        "sync_status_row_count": sync_status_row_count,
        "sync_status": sync_status,
        "warehouse_roots": warehouse_roots,
    }


def verify_research_data_snapshot_unchanged(
    start_snapshot: dict[str, object],
    end_snapshot: dict[str, object],
) -> dict[str, object]:
    """Fail closed when the observable data version changes during a run."""

    start_id = start_snapshot.get("snapshot_id")
    end_id = end_snapshot.get("snapshot_id")
    if start_id != end_id:
        raise ResearchDataSnapshotChangedError(
            "研究评估期间数据快照发生变化: "
            f"start={start_id or 'missing'}, end={end_id or 'missing'}"
        )
    verified = dict(start_snapshot)
    verified["verified_unchanged_during_run"] = start_id not in {None, "unavailable"}
    verified["end_snapshot_id"] = end_id
    return verified


def _get_metadata_path(database_manager) -> Path | None:
    value = getattr(database_manager, "sqlite_path", None)
    if value is None:
        return None
    return Path(value)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_sync_status(
    database_manager,
) -> tuple[list[dict[str, object]], str | None, int]:
    get_connection = getattr(database_manager, "get_sqlite_conn", None)
    if not callable(get_connection):
        return (
            [{"status": "unavailable", "reason": "缺少 get_sqlite_conn"}],
            None,
            0,
        )
    try:
        connection = get_connection()
        detail_rows = connection.execute(
            """
            SELECT dataset, symbol, last_sync_date, updated_at
            FROM sync_status
            ORDER BY dataset, symbol
            """
        ).fetchall()
        rows = connection.execute(
            """
            SELECT dataset,
                   COUNT(*) AS symbol_count,
                   MAX(last_sync_date) AS latest_sync_date,
                   MAX(updated_at) AS latest_updated_at
            FROM sync_status
            GROUP BY dataset
            ORDER BY dataset
            """
        ).fetchall()
    except Exception as error:
        return ([{"status": "unavailable", "reason": str(error)}], None, 0)
    detail_digest = hashlib.sha256()
    for row in detail_rows:
        detail_digest.update(
            json.dumps(
                [None if value is None else str(value) for value in row],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        detail_digest.update(b"\n")
    summary = [
        {
            "dataset": str(dataset),
            "symbol_count": int(symbol_count),
            "latest_sync_date": latest_sync_date,
            "latest_updated_at": latest_updated_at,
        }
        for dataset, symbol_count, latest_sync_date, latest_updated_at in rows
    ]
    return summary, detail_digest.hexdigest(), len(detail_rows)


def _read_warehouse_root_stats(database_manager) -> dict[str, dict[str, object]]:
    warehouse_value = getattr(database_manager, "warehouse_dir", WAREHOUSE_DIR)
    warehouse_dir = Path(warehouse_value)
    stats = {}
    for dataset in _SNAPSHOT_DATASET_ROOTS:
        path = warehouse_dir / dataset
        try:
            stat = path.stat()
        except OSError:
            stats[dataset] = {"exists": False}
            continue
        stats[dataset] = {
            "exists": True,
            "size": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
        }
    return stats
