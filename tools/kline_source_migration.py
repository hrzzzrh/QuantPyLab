"""Safe staging and migration support for stock K-line source changes."""

from __future__ import annotations

import base64
import binascii
import csv
import errno
import fcntl
import hashlib
import json
import os
import random
import re
import shutil
import time
import uuid
from collections.abc import Callable, Iterable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from config.settings import MIN_KLINE_START_DATE, WAREHOUSE_DIR
from storage.database.manager import db_manager
from utils.financial import to_sina_symbol
from utils.kline_policy import normalize_kline_start_date
from utils.kline_validation import (
    STORED_COLUMNS,
    KlineQualityError,
    KlineValidationError,
)
from utils.kline_validation import (
    quality_metrics as _shared_quality_metrics,
)
from utils.kline_validation import (
    validate_frame as _shared_validate_frame,
)
from utils.kline_validation import (
    validate_kline_frame as _shared_validate_kline_frame,
)
from utils.logger import logger
from utils.requests_protection import (
    SinaBlockedError,
    install_requests_protection,
)
from utils.sina_klc import SinaHfqFetchError, SinaKlcFetcher, SinaKlcFetchError
from utils.tencent_kline import TencentKlineFetcher
from utils.trade_date import get_latest_trade_date

MIGRATION_ROOT_NAME = ".migrations/kline-source"
MANIFEST_FILENAME = "manifest.csv"
METADATA_FILENAME = "run.json"
LOCK_FILENAME = "migration.lock"
UPGRADE_JOURNAL_FILENAME = "manifest_upgrade.json"

SAMPLE_GROUPS = ("600", "000", "002", "300", "688", "920")
PRIORITY_SYMBOLS = ("000011", "600009", "002005", "300008")
SYMBOL_PATTERN = re.compile(r"^[0-9]{6}$")
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
MIN_FREE_BYTES = 100 * 1024 * 1024

ManifestFetcher = Callable[[str, str, str], pd.DataFrame]

MANIFEST_FIELDS = (
    "symbol",
    "name",
    "source_used",
    "status",
    "error_type",
    "error_message",
    "cleanup_failed",
    "stage_path",
    "new_rows",
    "weekend_rows",
    "weekend_rows_filtered",
    "known_bad_rows_filtered",
    "zero_volume_rows",
    "zero_amount_rows",
    "factor_one_rows",
    "factor_min",
    "factor_max",
    "hfq_source_rows",
    "hfq_forward_filled_rows",
    "hfq_relation_mismatch_count",
    "hfq_relation_max_abs_error",
    "new_start_date",
    "new_end_date",
    "stage_sha256",
    "updated_at",
)
MANIFEST_SCHEMA_VERSION = 6
PREVIOUS_MANIFEST_SCHEMA_VERSION = 5
INTERMEDIATE_MANIFEST_SCHEMA_VERSION = 4
OLDER_MANIFEST_SCHEMA_VERSION = 3
LEGACY_MANIFEST_SCHEMA_VERSION = 2
SUPPORTED_MANIFEST_SCHEMA_VERSIONS = {
    LEGACY_MANIFEST_SCHEMA_VERSION,
    OLDER_MANIFEST_SCHEMA_VERSION,
    INTERMEDIATE_MANIFEST_SCHEMA_VERSION,
    PREVIOUS_MANIFEST_SCHEMA_VERSION,
    MANIFEST_SCHEMA_VERSION,
}
UPGRADABLE_MANIFEST_SCHEMA_VERSIONS = SUPPORTED_MANIFEST_SCHEMA_VERSIONS - {
    MANIFEST_SCHEMA_VERSION
}
MANIFEST_SOURCE_VALUES = frozenset({"sina-klc", "tencent-newfq"})
STAGE_EVIDENCE_METADATA_KEY = b"quantpylab.stage_evidence.v1"
PREVIOUS_MANIFEST_FIELDS = tuple(
    field for field in MANIFEST_FIELDS if field != "known_bad_rows_filtered"
)
INTERMEDIATE_MANIFEST_FIELDS = tuple(
    field for field in PREVIOUS_MANIFEST_FIELDS if field != "weekend_rows_filtered"
)
OLDER_MANIFEST_FIELDS = tuple(
    field for field in INTERMEDIATE_MANIFEST_FIELDS if field != "source_used"
)
LEGACY_MANIFEST_FIELDS = tuple(
    field for field in OLDER_MANIFEST_FIELDS if field != "cleanup_failed"
)
MANIFEST_FIELDS_BY_VERSION = {
    MANIFEST_SCHEMA_VERSION: MANIFEST_FIELDS,
    PREVIOUS_MANIFEST_SCHEMA_VERSION: PREVIOUS_MANIFEST_FIELDS,
    INTERMEDIATE_MANIFEST_SCHEMA_VERSION: INTERMEDIATE_MANIFEST_FIELDS,
    OLDER_MANIFEST_SCHEMA_VERSION: OLDER_MANIFEST_FIELDS,
    LEGACY_MANIFEST_SCHEMA_VERSION: LEGACY_MANIFEST_FIELDS,
}
BASE_RUN_METADATA_FIELDS = frozenset(
    {
        "run_id",
        "manifest_schema_version",
        "source",
        "start_date",
        "end_date",
        "dry_run",
        "created_at",
    }
)
RUN_METADATA_FIELDS = BASE_RUN_METADATA_FIELDS | {
    "target_count",
    "target_symbols_sha256",
}
UPGRADE_JOURNAL_FIELDS = frozenset(
    {
        "run_id",
        "from_version",
        "to_version",
        "old_manifest_b64",
        "old_metadata_b64",
        "new_manifest_b64",
        "new_metadata_b64",
    }
)
MANIFEST_STATUSES = frozenset({"pending", "fetching", "staged", "failed"})


class MigrationValidationError(KlineValidationError):
    """Raised when a staged K-line partition violates migration checks."""


class MigrationQualityError(MigrationValidationError):
    """Raised when a fetched frame fails an auditable quality gate."""


class MigrationSelectionError(ValueError):
    """Raised when the requested migration target cannot be selected."""


class MigrationLockError(RuntimeError):
    """Raised when another migration process already owns the lock."""


@dataclass(frozen=True)
class MigrationTarget:
    symbol: str
    name: str


@dataclass(frozen=True)
class MigrationResult:
    run_id: str
    run_dir: Path
    staged_symbols: tuple[str, ...]
    failed_symbols: tuple[str, ...]
    stopped_by_sina_block: bool = False


def get_migration_root(warehouse_dir: Path | str | None = None) -> Path:
    """Return the isolated migration workspace below the data warehouse."""

    base_dir = Path(warehouse_dir) if warehouse_dir is not None else Path(WAREHOUSE_DIR)
    base_resolved = base_dir.resolve()
    root = base_dir / MIGRATION_ROOT_NAME
    if not root.resolve(strict=False).is_relative_to(base_resolved):
        raise MigrationSelectionError(f"迁移目录越出数据仓: {root}")
    return root


def _validate_symbol(symbol: str) -> None:
    if not SYMBOL_PATTERN.fullmatch(symbol):
        raise MigrationSelectionError(f"股票代码必须是 6 位数字: {symbol!r}")


def _run_path(root: Path, run_id: str) -> Path:
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(f"非法迁移 run_id: {run_id!r}")
    root_resolved = root.resolve()
    if root.is_symlink():
        raise ValueError(f"迁移根目录不得是符号链接: {root}")
    path = root / run_id
    if path.parent != root or path.is_symlink():
        raise ValueError(f"迁移 run_id 越界: {run_id!r}")
    if not path.resolve(strict=False).is_relative_to(root_resolved):
        raise ValueError(f"迁移 run_id 越界: {run_id!r}")
    return path


def _safe_stage_path(
    run_dir: Path,
    raw_path: str,
    expected_symbol: str | None = None,
    *,
    create_dirs: bool = True,
) -> Path:
    if not raw_path:
        raise MigrationValidationError("manifest 缺少 stage_path")
    if run_dir.is_symlink() or run_dir.parent.is_symlink():
        raise MigrationValidationError("迁移目录或父目录不得是符号链接")
    staged_dir = run_dir / "staged"
    if staged_dir.is_symlink():
        raise MigrationValidationError("staging 根目录不得是符号链接")
    if create_dirs:
        staged_dir.mkdir(parents=False, exist_ok=True)
    if staged_dir.is_symlink():
        raise MigrationValidationError("staging 根目录不得是符号链接")
    stage_root = staged_dir.resolve()
    if stage_root != staged_dir.absolute():
        raise MigrationValidationError("staging 根目录解析越界")
    path = Path(raw_path)
    if path.is_symlink():
        raise MigrationValidationError("manifest stage_path 不得是符号链接")
    resolved = path.resolve(strict=False)
    if not resolved.is_relative_to(stage_root):
        raise MigrationValidationError("manifest stage_path 越出迁移目录")
    if expected_symbol is not None:
        expected_path = stage_root / f"symbol={expected_symbol}" / "data.parquet"
        if resolved != expected_path:
            raise MigrationValidationError(
                "manifest stage_path 与 symbol 不匹配或不是标准路径"
            )
    symbol_dir = path.parent
    if symbol_dir.exists() and symbol_dir.is_symlink():
        raise MigrationValidationError("symbol staging 目录不得是符号链接")
    if create_dirs:
        symbol_dir.mkdir(parents=False, exist_ok=True)
    if symbol_dir.is_symlink() or symbol_dir.resolve() != symbol_dir.absolute():
        raise MigrationValidationError("symbol staging 目录解析越界")
    return resolved


def _is_valid_sha256(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{64}", value))


def _new_run_id() -> str:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{timestamp}-sina-klc-{uuid.uuid4().hex[:8]}"


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temp_path.write_bytes(content)
        os.replace(temp_path, path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _atomic_write_bytes(path, _json_bytes(payload))


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=True, indent=2) + "\n").encode("utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, dict):
        raise ValueError(f"迁移元数据格式错误: {path}")
    return payload


def _target_symbols_sha256(symbols: Iterable[str]) -> str:
    payload = "\n".join(symbols).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


def _target_evidence(symbols: Iterable[str]) -> dict[str, int | str]:
    symbol_list = list(symbols)
    return {
        "target_count": len(symbol_list),
        "target_symbols_sha256": _target_symbols_sha256(symbol_list),
    }


def _assert_run_files_safe(run_dir: Path, *, require_complete: bool = True) -> None:
    for filename in (METADATA_FILENAME, MANIFEST_FILENAME, UPGRADE_JOURNAL_FILENAME):
        path = run_dir / filename
        if path.is_symlink():
            raise MigrationValidationError(f"迁移 run 文件不得是符号链接: {filename}")
        if (
            require_complete
            and filename != UPGRADE_JOURNAL_FILENAME
            and not path.exists()
        ):
            raise MigrationValidationError(f"迁移 run 缺少文件: {filename}")


def _validate_run_metadata(
    payload: dict[str, Any],
    *,
    expected_run_id: str,
    expected_schema_version: int | None,
    expected_source: str | None = None,
    allow_missing_target_evidence: bool = False,
) -> None:
    expected_fields = (
        RUN_METADATA_FIELDS
        if expected_schema_version in {None, MANIFEST_SCHEMA_VERSION}
        else BASE_RUN_METADATA_FIELDS
    )
    allowed_fields = (
        {RUN_METADATA_FIELDS, BASE_RUN_METADATA_FIELDS}
        if expected_schema_version is None
        else {expected_fields}
    )
    if (
        allow_missing_target_evidence
        and expected_schema_version == MANIFEST_SCHEMA_VERSION
    ):
        allowed_fields.add(BASE_RUN_METADATA_FIELDS)
    if set(payload) not in allowed_fields:
        if (
            expected_schema_version == MANIFEST_SCHEMA_VERSION
            and set(payload) == BASE_RUN_METADATA_FIELDS
            and not allow_missing_target_evidence
        ):
            raise MigrationValidationError(
                "迁移 run 缺少不可变股票清单证据，请新建 run"
            )
        raise MigrationValidationError("迁移元数据字段不完整或包含未知字段")
    if payload["run_id"] != expected_run_id:
        raise MigrationValidationError("迁移元数据 run_id 不匹配")
    if type(payload["manifest_schema_version"]) is not int or (
        expected_schema_version is not None
        and payload["manifest_schema_version"] != expected_schema_version
    ):
        raise MigrationValidationError("迁移元数据版本错误")
    if not isinstance(payload["source"], str) or (
        expected_source is not None and payload["source"] != expected_source
    ):
        raise MigrationValidationError("迁移元数据 source 错误")
    if not isinstance(payload["start_date"], str):
        raise MigrationValidationError("迁移元数据 start_date 类型错误")
    try:
        datetime.strptime(payload["start_date"], "%Y%m%d")
    except ValueError as exc:
        raise MigrationValidationError("迁移元数据 start_date 格式错误") from exc
    if not isinstance(payload["end_date"], str):
        raise MigrationValidationError("迁移元数据 end_date 类型错误")
    if type(payload["dry_run"]) is not bool:
        raise MigrationValidationError("迁移元数据 dry_run 类型错误")
    if not payload["end_date"]:
        if not payload["dry_run"]:
            raise MigrationValidationError("迁移元数据 end_date 不能为空")
    else:
        try:
            datetime.strptime(payload["end_date"], "%Y%m%d")
        except ValueError as exc:
            raise MigrationValidationError("迁移元数据 end_date 格式错误") from exc
        if payload["start_date"] > payload["end_date"]:
            raise MigrationValidationError("迁移元数据 start_date 不能晚于 end_date")
    if not isinstance(payload["created_at"], str):
        raise MigrationValidationError("迁移元数据 created_at 类型错误")
    try:
        datetime.fromisoformat(payload["created_at"])
    except ValueError as exc:
        raise MigrationValidationError("迁移元数据 created_at 格式错误") from exc


def _decode_journal_bytes(payload: dict[str, Any], field: str) -> bytes:
    value = payload.get(field)
    if not isinstance(value, str):
        raise ValueError(f"迁移升级 journal 缺少 {field}")
    try:
        return base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError(f"迁移升级 journal 的 {field} 无效") from exc


def _recover_manifest_upgrade(run_dir: Path) -> None:
    journal_path = run_dir / UPGRADE_JOURNAL_FILENAME
    if journal_path.is_symlink():
        raise MigrationValidationError("迁移升级 journal 不得是符号链接")
    if not journal_path.exists():
        return
    _assert_run_files_safe(run_dir)
    payload = _read_json(journal_path)
    if set(payload) != UPGRADE_JOURNAL_FIELDS:
        raise MigrationValidationError("迁移升级 journal 字段不完整或包含未知字段")
    if payload.get("run_id") != run_dir.name:
        raise MigrationValidationError("迁移升级 journal 的 run_id 不匹配")
    from_version = payload.get("from_version")
    if (
        type(from_version) is not int
        or from_version not in UPGRADABLE_MANIFEST_SCHEMA_VERSIONS
    ):
        raise MigrationValidationError("迁移升级 journal 的源版本不支持")
    if (
        type(payload.get("to_version")) is not int
        or payload.get("to_version") != MANIFEST_SCHEMA_VERSION
    ):
        raise MigrationValidationError("迁移升级 journal 的目标版本不支持")
    old_manifest_bytes = _decode_journal_bytes(payload, "old_manifest_b64")
    old_metadata_bytes = _decode_journal_bytes(payload, "old_metadata_b64")
    new_manifest = _decode_journal_bytes(payload, "new_manifest_b64")
    new_metadata = _decode_journal_bytes(payload, "new_metadata_b64")
    try:
        metadata = json.loads(new_metadata)
        old_metadata = json.loads(old_metadata_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MigrationValidationError("迁移升级 journal 的 metadata 无效") from exc
    _validate_run_metadata(
        metadata,
        expected_run_id=run_dir.name,
        expected_schema_version=MANIFEST_SCHEMA_VERSION,
        expected_source="sina-klc",
    )
    _validate_run_metadata(
        old_metadata,
        expected_run_id=run_dir.name,
        expected_schema_version=from_version,
        expected_source=metadata["source"],
    )
    if any(
        old_metadata[field] != metadata[field]
        for field in BASE_RUN_METADATA_FIELDS - {"manifest_schema_version"}
    ):
        raise MigrationValidationError("迁移升级 journal 的新旧 metadata 不一致")
    new_manifest_obj = MigrationManifest._from_csv_bytes(
        run_dir / MANIFEST_FILENAME,
        new_manifest,
        expected_schema_version=MANIFEST_SCHEMA_VERSION,
    )
    MigrationManifest._from_csv_bytes(
        run_dir / MANIFEST_FILENAME,
        old_manifest_bytes,
        expected_schema_version=from_version,
    )
    _validate_target_evidence(metadata, new_manifest_obj)
    manifest_path = run_dir / MANIFEST_FILENAME
    metadata_path = run_dir / METADATA_FILENAME
    current_manifest = manifest_path.read_bytes()
    current_metadata = metadata_path.read_bytes()
    if current_manifest not in {
        old_manifest_bytes,
        new_manifest,
    } or current_metadata not in {
        old_metadata_bytes,
        new_metadata,
    }:
        raise MigrationValidationError("迁移升级 journal 与当前文件状态不匹配")
    try:
        _atomic_write_bytes(manifest_path, new_manifest)
        _atomic_write_bytes(metadata_path, new_metadata)
        if (
            manifest_path.read_bytes() != new_manifest
            or metadata_path.read_bytes() != new_metadata
        ):
            raise MigrationValidationError("迁移升级后的文件校验失败")
    except Exception:
        try:
            _atomic_write_bytes(manifest_path, old_manifest_bytes)
            _atomic_write_bytes(metadata_path, old_metadata_bytes)
        except Exception as rollback_exc:
            logger.error("迁移升级回滚失败: %s", rollback_exc)
        raise
    journal_path.unlink()


def _upgrade_legacy_run(
    run_dir: Path, metadata: dict[str, Any], manifest: MigrationManifest
) -> None:
    if manifest.schema_version not in UPGRADABLE_MANIFEST_SCHEMA_VERSIONS:
        return
    raise MigrationValidationError(
        "旧迁移 run 缺少不可变股票清单证据，无法安全升级，请新建 run"
    )


def _upgrade_current_run_metadata(
    run_dir: Path, metadata: dict[str, Any], manifest: MigrationManifest
) -> None:
    """Reject schema-6 runs created before immutable target evidence existed."""
    if set(metadata) == BASE_RUN_METADATA_FIELDS:
        raise MigrationValidationError(
            "schema-6 迁移 run 缺少不可变股票清单证据，无法安全补齐，请新建 run"
        )


def _new_manifest_row(target: MigrationTarget) -> dict[str, str]:
    row = {field: "" for field in MANIFEST_FIELDS}
    row.update(
        {
            "symbol": target.symbol,
            "name": target.name,
            "status": "pending",
        }
    )
    return row


def _validate_manifest_row_state(
    row: dict[str, str], path: Path, *, allow_legacy_source: bool = False
) -> None:
    status = row["status"]
    if status not in MANIFEST_STATUSES:
        raise MigrationValidationError(
            f"迁移 manifest status 无效: {row['symbol']}: {status!r}"
        )
    if status == "staged":
        if not row["source_used"] and not allow_legacy_source:
            raise MigrationValidationError(
                f"staged manifest evidence 不完整，缺少 source_used: {row['symbol']}"
            )
        if not row["stage_path"]:
            raise MigrationValidationError(
                f"staged manifest evidence 不完整，缺少 stage_path: {row['symbol']}"
            )
        if not _is_valid_sha256(row["stage_sha256"]):
            raise MigrationValidationError(
                f"staged manifest evidence 不完整，缺少有效 stage_sha256: {row['symbol']}"
            )
    if row["cleanup_failed"] not in {"", "1"}:
        raise MigrationValidationError(
            f"cleanup_failed 值无效: {row['symbol']}: {row['cleanup_failed']!r}"
        )
    if status == "staged" and row["cleanup_failed"]:
        raise MigrationValidationError(
            f"staged manifest 不得标记 cleanup_failed: {row['symbol']}"
        )
    if row["source_used"] and row["source_used"] not in MANIFEST_SOURCE_VALUES:
        raise MigrationValidationError(
            f"source_used 值无效: {row['symbol']}: {row['source_used']!r}"
        )
    if row["cleanup_failed"] and not row["stage_path"]:
        raise MigrationValidationError(
            f"cleanup_failed manifest 缺少 stage_path: {row['symbol']}"
        )
    if status != "staged" and row["stage_sha256"]:
        raise MigrationValidationError(
            f"非 staged manifest 不得包含 stage_sha256: {row['symbol']}"
        )


class MigrationManifest:
    """Atomically persisted per-symbol migration state."""

    def __init__(
        self,
        path: Path,
        rows: list[dict[str, str]],
        schema_version: int = MANIFEST_SCHEMA_VERSION,
    ):
        self.path = path
        self.rows = rows
        self.schema_version = schema_version
        self._rows_by_symbol = {row["symbol"]: row for row in rows}

    @classmethod
    def create(
        cls, path: Path, targets: Iterable[MigrationTarget]
    ) -> MigrationManifest:
        manifest = cls(
            path,
            [_new_manifest_row(target) for target in targets],
            schema_version=MANIFEST_SCHEMA_VERSION,
        )
        manifest.save()
        return manifest

    @classmethod
    def load(cls, path: Path) -> MigrationManifest:
        if not path.exists():
            raise FileNotFoundError(f"找不到迁移 manifest: {path}")
        return cls._from_csv_bytes(path, path.read_bytes())

    @classmethod
    def _from_csv_bytes(
        cls,
        path: Path,
        content: bytes,
        expected_schema_version: int | None = None,
    ) -> MigrationManifest:
        from io import StringIO

        reader = csv.DictReader(StringIO(content.decode("utf-8")))
        schema_version = next(
            (
                version
                for version, fields in MANIFEST_FIELDS_BY_VERSION.items()
                if reader.fieldnames == list(fields)
            ),
            None,
        )
        if schema_version is None:
            raise ValueError(f"迁移 manifest 字段错误: {path}")
        if (
            expected_schema_version is not None
            and schema_version != expected_schema_version
        ):
            raise ValueError(f"迁移 manifest 版本错误: {path}")
        rows = []
        for raw_row in reader:
            expected_fields = set(MANIFEST_FIELDS_BY_VERSION[schema_version])
            if set(raw_row) != expected_fields or any(
                raw_row[field] is None for field in expected_fields
            ):
                raise ValueError(f"迁移 manifest 行字段错误: {path}")
            row = {field: raw_row.get(field, "") or "" for field in MANIFEST_FIELDS}
            if not row["symbol"]:
                raise ValueError(f"迁移 manifest 包含空 symbol: {path}")
            _validate_symbol(row["symbol"])
            _validate_manifest_row_state(
                row,
                path,
                allow_legacy_source=schema_version != MANIFEST_SCHEMA_VERSION,
            )
            if row["status"] == "staged" and not _is_valid_sha256(row["stage_sha256"]):
                raise MigrationValidationError(
                    f"staged manifest 缺少有效 stage_sha256: {row['symbol']}"
                )
            if row["stage_path"]:
                _safe_stage_path(
                    path.parent,
                    row["stage_path"],
                    row["symbol"],
                    create_dirs=False,
                )
            if any(existing["symbol"] == row["symbol"] for existing in rows):
                raise ValueError(f"迁移 manifest 包含重复 symbol: {row['symbol']}")
            rows.append(row)
        if not rows:
            raise ValueError(f"迁移 manifest 不得为空: {path}")
        return cls(path, rows, schema_version=schema_version)

    def save(self) -> None:
        self.schema_version = MANIFEST_SCHEMA_VERSION
        _atomic_write_bytes(self.path, _manifest_csv_bytes(self))

    def update(self, symbol: str, **changes: Any) -> dict[str, str]:
        row = self._rows_by_symbol[symbol]
        for key, value in changes.items():
            if key not in MANIFEST_FIELDS:
                raise KeyError(f"未知 manifest 字段: {key}")
            row[key] = "" if value is None else str(value)
        row["updated_at"] = datetime.now().isoformat(timespec="seconds")
        self.save()
        return row

    def get(self, symbol: str) -> dict[str, str]:
        return self._rows_by_symbol[symbol]


def _validate_target_evidence(
    metadata: dict[str, Any], manifest: MigrationManifest
) -> None:
    target_count = metadata.get("target_count")
    target_hash = metadata.get("target_symbols_sha256")
    if type(target_count) is not int or target_count <= 0:
        raise MigrationValidationError("迁移元数据 target_count 无效")
    if not isinstance(target_hash, str) or not _is_valid_sha256(target_hash):
        raise MigrationValidationError("迁移元数据 target_symbols_sha256 无效")
    actual = _target_evidence(row["symbol"] for row in manifest.rows)
    if target_count != actual["target_count"]:
        raise MigrationValidationError("迁移 manifest 股票数量与元数据不一致")
    if target_hash != actual["target_symbols_sha256"]:
        raise MigrationValidationError("迁移 manifest 股票清单摘要与元数据不一致")


def _manifest_csv_bytes(manifest: MigrationManifest) -> bytes:
    from io import StringIO

    buffer = StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=MANIFEST_FIELDS, lineterminator="\n")
    writer.writeheader()
    for row in manifest.rows:
        writer.writerow({field: row.get(field, "") for field in MANIFEST_FIELDS})
    return buffer.getvalue().encode("utf-8")


def _parse_lock_pid(content: bytes) -> int | None:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        return None
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        if key == "pid" and separator:
            try:
                return int(value)
            except ValueError:
                return None
    return None


def _is_process_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


class MigrationLock(AbstractContextManager[None]):
    """Create an exclusive lock without deleting another process's lock."""

    def __init__(self, root: Path, run_id: str, recover_stale: bool = False):
        self.path = root / LOCK_FILENAME
        self.run_id = run_id
        self.recover_stale = recover_stale
        self._owned = False

    def __enter__(self) -> None:
        if self.path.parent.is_symlink():
            raise MigrationLockError(f"迁移锁父目录不得是符号链接: {self.path.parent}")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.is_symlink():
            raise MigrationLockError(f"迁移锁不得是符号链接: {self.path}")
        no_follow = getattr(os, "O_NOFOLLOW", None)
        if no_follow is None:
            raise MigrationLockError("当前平台不支持安全打开迁移锁")
        directory_flag = getattr(os, "O_DIRECTORY", 0)
        payload = f"run_id={self.run_id}\npid={os.getpid()}\n".encode()
        try:
            parent_fd = os.open(
                self.path.parent,
                os.O_RDONLY | directory_flag | no_follow,
            )
        except OSError as exc:
            if exc.errno == errno.ELOOP or self.path.parent.is_symlink():
                raise MigrationLockError(
                    f"迁移锁父目录不得是符号链接: {self.path.parent}"
                ) from exc
            raise
        lock_fd = None
        try:
            try:
                lock_fd = os.open(
                    LOCK_FILENAME,
                    os.O_RDWR | os.O_CREAT | no_follow,
                    0o600,
                    dir_fd=parent_fd,
                )
            except OSError as exc:
                if exc.errno == errno.ELOOP or self.path.is_symlink():
                    raise MigrationLockError(
                        f"迁移锁不得是符号链接: {self.path}"
                    ) from exc
                raise
        finally:
            try:
                os.close(parent_fd)
            except Exception:
                if lock_fd is not None:
                    os.close(lock_fd)
                raise
        self._fd = lock_fd
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(self._fd)
            raise MigrationLockError(f"已有迁移正在运行，锁文件: {self.path}") from exc
        except Exception:
            os.close(self._fd)
            raise

        try:
            os.lseek(self._fd, 0, os.SEEK_SET)
            existing_content = os.read(self._fd, 4096)
            if existing_content:
                if not self.recover_stale:
                    raise MigrationLockError(f"已有迁移正在运行，锁文件: {self.path}")
                pid = _parse_lock_pid(existing_content)
                if pid is None:
                    raise MigrationLockError(
                        f"迁移锁缺少有效 PID，拒绝恢复: {self.path}"
                    )
                if _is_process_alive(pid):
                    raise MigrationLockError(f"已有迁移正在运行，锁文件: {self.path}")
            os.ftruncate(self._fd, 0)
            os.lseek(self._fd, 0, os.SEEK_SET)
            offset = 0
            while offset < len(payload):
                written = os.write(self._fd, payload[offset:])
                remaining = len(payload) - offset
                if written <= 0 or written > remaining:
                    raise OSError("迁移锁写入长度非法")
                offset += written
            os.fsync(self._fd)
            os.lseek(self._fd, 0, os.SEEK_SET)
            if os.read(self._fd, len(payload)) != payload:
                raise OSError("迁移锁写入校验失败")
        except Exception:
            os.close(self._fd)
            raise
        self._owned = True

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self._owned:
            try:
                os.ftruncate(self._fd, 0)
                os.fsync(self._fd)
            finally:
                try:
                    fcntl.flock(self._fd, fcntl.LOCK_UN)
                finally:
                    os.close(self._fd)
                    self._owned = False


def _parse_date(value: str, field_name: str) -> str:
    try:
        parsed = datetime.strptime(value, "%Y%m%d")
    except ValueError as exc:
        raise ValueError(f"{field_name} 必须是 YYYYMMDD: {value!r}") from exc
    return parsed.strftime("%Y%m%d")


def _validate_frame(
    df: pd.DataFrame, *, require_close_hfq: bool = False
) -> pd.DataFrame:
    try:
        return _shared_validate_frame(df, require_close_hfq=require_close_hfq)
    except KlineValidationError as exc:
        raise MigrationValidationError(str(exc)) from exc


def _validate_requested_date_range(
    df: pd.DataFrame, start_date: str, end_date: str
) -> None:
    start = datetime.strptime(start_date, "%Y%m%d").date()
    end = datetime.strptime(end_date, "%Y%m%d").date()
    if df["date"].min() < start or df["date"].max() > end:
        raise MigrationValidationError(f"K 线日期超出请求范围: {start_date}~{end_date}")


def _quality_metrics(df: pd.DataFrame) -> dict[str, int | float]:
    return _shared_quality_metrics(df)


def validate_kline_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Validate a KLC frame before it is persisted to a stock partition."""
    try:
        return _shared_validate_kline_frame(df)
    except KlineQualityError as exc:
        raise MigrationQualityError(str(exc)) from exc
    except KlineValidationError as exc:
        raise MigrationValidationError(str(exc)) from exc


_STAGE_EVIDENCE_FIELDS = (
    "new_rows",
    "weekend_rows",
    "weekend_rows_filtered",
    "known_bad_rows_filtered",
    "zero_volume_rows",
    "zero_amount_rows",
    "factor_one_rows",
    "factor_min",
    "factor_max",
    "hfq_source_rows",
    "hfq_forward_filled_rows",
    "hfq_relation_mismatch_count",
    "hfq_relation_max_abs_error",
    "new_start_date",
    "new_end_date",
    "stage_sha256",
)


def _clear_stage_evidence() -> dict[str, str]:
    return {field: "" for field in _STAGE_EVIDENCE_FIELDS}


def _expected_stage_path(run_dir: Path, symbol: str) -> str:
    return str(run_dir.absolute() / "staged" / f"symbol={symbol}" / "data.parquet")


def _set_fetcher_source_from_exception(
    manifest: MigrationManifest, symbol: str, exc: BaseException
) -> None:
    source_used = getattr(exc, "source_used", "")
    if source_used in MANIFEST_SOURCE_VALUES:
        manifest.update(symbol, source_used=source_used)


def _discard_staged_file(run_dir: Path, symbol: str) -> bool:
    """Remove a symbol's staging file and report whether cleanup succeeded."""
    try:
        stage_path = _safe_stage_path(
            run_dir,
            _expected_stage_path(run_dir, symbol),
            symbol,
        )
        stage_path.unlink(missing_ok=True)
        return not stage_path.exists()
    except (MigrationValidationError, OSError) as exc:
        logger.warning("无法清理 staging 文件 %s: %s", symbol, exc)
        return False


def _stage_cleanup_changes(run_dir: Path, row: dict[str, str]) -> dict[str, str]:
    cleaned = _discard_staged_file(run_dir, row["symbol"])
    expected_path = _expected_stage_path(run_dir, row["symbol"])
    return {
        "stage_path": "" if cleaned else expected_path,
        "cleanup_failed": "" if cleaned else "1",
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _open_directory_no_follow(path: Path) -> int:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    absolute_path = path.absolute()
    directory_fd = os.open(os.sep, flags)
    try:
        for part in absolute_path.parts[1:]:
            next_fd = os.open(part, flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
        return directory_fd
    except OSError:
        os.close(directory_fd)
        raise


def _write_staged_frame(
    df: pd.DataFrame,
    path: Path,
    *,
    evidence: dict[str, int | float] | None = None,
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    stored = df[list(STORED_COLUMNS)].copy()
    metrics = evidence or _quality_metrics(df)
    evidence = {
        field: str(metrics[field])
        for field in _STAGE_EVIDENCE_FIELDS
        if field not in {"new_start_date", "new_end_date", "stage_sha256"}
    }
    evidence["new_start_date"] = str(pd.to_datetime(stored["date"]).min().date())
    evidence["new_end_date"] = str(pd.to_datetime(stored["date"]).max().date())
    table = pa.Table.from_pandas(stored, preserve_index=False)
    metadata = dict(table.schema.metadata or {})
    metadata[STAGE_EVIDENCE_METADATA_KEY] = json.dumps(
        evidence, ensure_ascii=True, sort_keys=True
    ).encode("utf-8")
    table = table.replace_schema_metadata(metadata)
    path.parent.mkdir(parents=True, exist_ok=True)
    directory_fd = _open_directory_no_follow(path.parent)
    temp_name = f".{path.name}.{uuid.uuid4().hex}.tmp"
    temp_fd = None
    try:
        temp_fd = os.open(
            temp_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o644,
            dir_fd=directory_fd,
        )
        with os.fdopen(temp_fd, "wb") as temp_file:
            temp_fd = None
            pq.write_table(table, temp_file, compression="snappy")
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(
            temp_name,
            path.name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        os.fsync(directory_fd)
    except Exception:
        if temp_fd is not None:
            os.close(temp_fd)
        try:
            os.unlink(temp_name, dir_fd=directory_fd)
        except FileNotFoundError:
            pass
        raise
    finally:
        os.close(directory_fd)


def _read_and_validate(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise MigrationValidationError(f"找不到待校验 Parquet: {path}")
    return _validate_frame(pd.read_parquet(path))


def _is_delisted_sina_symbol(sina_symbol: str) -> bool:
    symbol = sina_symbol[2:]
    _validate_symbol(symbol)
    row = (
        db_manager.get_sqlite_conn()
        .execute("SELECT is_active FROM stocks WHERE symbol = ?", (symbol,))
        .fetchone()
    )
    return row is not None and row[0] == 0


def _default_fetcher(sina_symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch KLC first and use Tencent only for failed delisted symbols."""
    is_delisted = _is_delisted_sina_symbol(sina_symbol)
    try:
        frame = SinaKlcFetcher.fetch_klc_data(
            sina_symbol, start_date=start_date, end_date=end_date
        )
        if not is_delisted:
            return frame
        return validate_kline_frame(frame)
    except SinaBlockedError:
        raise
    except (SinaHfqFetchError, SinaKlcFetchError, KlineValidationError) as sina_error:
        if not is_delisted:
            raise
        logger.warning(
            "%s 退市股新浪 KLC staging 失败，切换腾讯整段重建: %s",
            sina_symbol[2:],
            sina_error,
        )
        return TencentKlineFetcher.fetch_full(
            sina_symbol[2:], start_date=start_date, end_date=end_date
        )


def select_migration_symbols(
    limit: int | None = 20,
    symbol: str | None = None,
    conn=None,
) -> list[MigrationTarget]:
    """Select stocks, including delisted symbols, for KLC rebuilding."""

    conn = conn or db_manager.get_sqlite_conn()
    if symbol is not None:
        _validate_symbol(symbol)
        row = conn.execute(
            """
            SELECT symbol, name, is_active, last_trade_date
            FROM stocks
            WHERE symbol = ?
            """,
            (symbol,),
        ).fetchone()
        if row is None:
            raise MigrationSelectionError(f"股票不存在: {symbol}")
        if (
            row[2] == 0
            and row[3]
            and str(row[3]).replace("-", "") < MIN_KLINE_START_DATE
        ):
            raise MigrationSelectionError(
                f"{symbol} 最后交易日在 {MIN_KLINE_START_DATE} 前，范围内无 K 线数据"
            )
        return [MigrationTarget(str(row[0]), str(row[1]))]

    if limit is not None and limit <= 0:
        raise MigrationSelectionError("limit 必须大于 0，或使用 None 选择全部股票")

    rows = conn.execute(
        """
        SELECT symbol, name, is_active, last_trade_date
        FROM stocks
        ORDER BY symbol
        """
    ).fetchall()
    targets = []
    for raw_symbol, raw_name, is_active, last_trade_date in rows:
        normalized_symbol = str(raw_symbol)
        _validate_symbol(normalized_symbol)
        if (
            is_active == 0
            and last_trade_date
            and str(last_trade_date).replace("-", "") < MIN_KLINE_START_DATE
        ):
            continue
        targets.append(MigrationTarget(normalized_symbol, str(raw_name)))
    if limit is None:
        return targets
    target_by_symbol = {target.symbol: target for target in targets}
    selected = [
        target_by_symbol[priority_symbol]
        for priority_symbol in PRIORITY_SYMBOLS
        if priority_symbol in target_by_symbol
    ]
    seen = {target.symbol for target in selected}
    grouped = {
        prefix: [target for target in targets if target.symbol.startswith(prefix)]
        for prefix in SAMPLE_GROUPS
    }
    while len(selected) < limit:
        added = False
        for prefix in SAMPLE_GROUPS:
            candidate = next(
                (target for target in grouped[prefix] if target.symbol not in seen),
                None,
            )
            if candidate is not None:
                selected.append(candidate)
                seen.add(candidate.symbol)
                added = True
            if len(selected) >= limit:
                break
        if not added:
            break
    return selected[:limit]


def _assert_writable(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    probe = path / f".write-test-{uuid.uuid4().hex}"
    try:
        probe.write_text("ok", encoding="ascii")
    finally:
        probe.unlink(missing_ok=True)


def _check_disk_space(warehouse_dir: Path, targets: Iterable[MigrationTarget]) -> None:
    estimated_bytes = MIN_FREE_BYTES
    for _target in targets:
        # KLC returns full history; reserve a conservative 8 MiB per symbol.
        estimated_bytes += 8 * 1024 * 1024
    free_bytes = shutil.disk_usage(warehouse_dir).free
    if free_bytes < estimated_bytes:
        raise MigrationValidationError(
            f"磁盘剩余空间不足: free={free_bytes}, required={estimated_bytes}"
        )


def _check_unfinished_runs(root: Path, source: str) -> None:
    if not root.exists():
        return
    active_statuses = {"pending", "fetching"}
    for candidate in root.iterdir():
        if candidate.is_symlink():
            raise MigrationLockError(f"迁移根目录包含不安全符号链接: {candidate.name}")
        if candidate.name.startswith(".") and candidate.name.endswith(".tmp"):
            continue
        if not candidate.is_dir():
            continue
        metadata_path = candidate / METADATA_FILENAME
        manifest_path = candidate / MANIFEST_FILENAME
        journal_path = candidate / UPGRADE_JOURNAL_FILENAME
        if metadata_path.is_symlink():
            raise MigrationLockError(f"迁移 run 包含不安全符号链接: {candidate.name}")
        if metadata_path.exists():
            metadata = _read_json(metadata_path)
            if metadata.get("source") != source:
                _validate_run_metadata(
                    metadata,
                    expected_run_id=candidate.name,
                    expected_schema_version=None,
                )
                continue
        elif (
            manifest_path.is_symlink()
            or journal_path.is_symlink()
            or manifest_path.exists()
            or journal_path.exists()
        ):
            raise MigrationLockError(
                f"迁移 run 缺少 metadata，请先 resume/recover: {candidate.name}"
            )
        else:
            continue
        if manifest_path.is_symlink() or journal_path.is_symlink():
            raise MigrationLockError(f"迁移 run 包含不安全符号链接: {candidate.name}")
        if journal_path.exists() and (
            journal_path.is_symlink()
            or not metadata_path.exists()
            or not manifest_path.exists()
        ):
            raise MigrationLockError(
                f"迁移 run 残留不完整 upgrade journal，请先 resume/recover: {candidate.name}"
            )
        if not metadata_path.exists() or not manifest_path.exists():
            if metadata_path.exists() or manifest_path.exists():
                raise MigrationLockError(
                    f"迁移 run 残留不完整，请先 resume/recover: {candidate.name}"
                )
            continue
        metadata_version = metadata.get("manifest_schema_version")
        if metadata_version not in SUPPORTED_MANIFEST_SCHEMA_VERSIONS:
            raise ValueError(f"不支持的迁移 manifest 版本: {candidate.name}")
        _validate_run_metadata(
            metadata,
            expected_run_id=candidate.name,
            expected_schema_version=metadata_version,
            expected_source=source,
        )
        manifest = MigrationManifest.load(manifest_path)
        if journal_path.exists() or metadata_version != manifest.schema_version:
            raise MigrationLockError(
                f"迁移 run 需要先 resume/recover: {candidate.name}"
            )
        if manifest.schema_version == MANIFEST_SCHEMA_VERSION:
            _validate_target_evidence(metadata, manifest)
        if metadata.get("dry_run"):
            continue
        if metadata["start_date"] < MIN_KLINE_START_DATE:
            logger.warning(
                "忽略最低日期前的旧 staging run，不阻塞新口径迁移: %s",
                candidate.name,
            )
            continue
        if any(row["status"] in active_statuses for row in manifest.rows):
            raise MigrationLockError(
                f"已有未完成的 {source} staging run，请使用 --resume: {candidate.name}"
            )


def _build_run(
    root: Path,
    run_id: str,
    targets: list[MigrationTarget],
    source: str,
    start_date: str,
    end_date: str,
    dry_run: bool,
) -> tuple[str, Path, MigrationManifest]:
    run_dir = _run_path(root, run_id)
    temp_dir = root / f".{run_id}.{uuid.uuid4().hex}.tmp"
    temp_dir.mkdir(parents=True, exist_ok=False)
    try:
        _write_json(
            temp_dir / METADATA_FILENAME,
            {
                "run_id": run_id,
                "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
                "source": source,
                "start_date": start_date,
                "end_date": end_date,
                "dry_run": dry_run,
                "created_at": datetime.now().isoformat(timespec="seconds"),
                **_target_evidence(target.symbol for target in targets),
            },
        )
        manifest = MigrationManifest.create(temp_dir / MANIFEST_FILENAME, targets)
        os.rename(temp_dir, run_dir)
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    manifest.path = run_dir / MANIFEST_FILENAME
    return run_id, run_dir, manifest


def _load_run(
    root: Path, run_id: str, *, allow_legacy: bool = False
) -> tuple[Path, dict[str, Any], MigrationManifest]:
    run_dir = _run_path(root, run_id)
    _assert_run_files_safe(run_dir)
    metadata = _read_json(run_dir / METADATA_FILENAME)
    if metadata.get("run_id") != run_id:
        raise ValueError(f"迁移 run_id 与元数据不一致: {run_id}")
    if (
        metadata.get("manifest_schema_version")
        not in SUPPORTED_MANIFEST_SCHEMA_VERSIONS
    ):
        raise ValueError(f"不支持的迁移 manifest 版本: {run_id}")
    manifest = MigrationManifest.load(run_dir / MANIFEST_FILENAME)
    if metadata["manifest_schema_version"] != manifest.schema_version:
        if not (allow_legacy and (run_dir / UPGRADE_JOURNAL_FILENAME).exists()):
            raise ValueError(f"迁移元数据与 manifest 版本不一致: {run_id}")
    if manifest.schema_version == LEGACY_MANIFEST_SCHEMA_VERSION and not allow_legacy:
        raise ValueError(f"legacy 迁移 run 必须在锁内升级: {run_id}")
    _validate_run_metadata(
        metadata,
        expected_run_id=run_id,
        expected_schema_version=manifest.schema_version,
        allow_missing_target_evidence=allow_legacy,
    )
    if manifest.schema_version == MANIFEST_SCHEMA_VERSION:
        if set(metadata) == RUN_METADATA_FIELDS:
            _validate_target_evidence(metadata, manifest)
        elif not (allow_legacy and set(metadata) == BASE_RUN_METADATA_FIELDS):
            raise MigrationValidationError("迁移元数据缺少股票清单证据")
    return run_dir, metadata, manifest


def _format_status_result(
    run_id: str, run_dir: Path, manifest: MigrationManifest, stopped: bool
) -> MigrationResult:
    staged = []
    failed = []
    for row in manifest.rows:
        if row["status"] == "staged":
            staged.append(row["symbol"])
        if row["status"] == "failed":
            failed.append(row["symbol"])
    return MigrationResult(
        run_id=run_id,
        run_dir=run_dir,
        staged_symbols=tuple(staged),
        failed_symbols=tuple(failed),
        stopped_by_sina_block=stopped,
    )


def _stage_symbol(
    row: dict[str, str],
    manifest: MigrationManifest,
    run_dir: Path,
    start_date: str,
    end_date: str,
    fetcher: ManifestFetcher,
) -> None:
    symbol = row["symbol"]
    stage_path = _safe_stage_path(
        run_dir,
        _expected_stage_path(run_dir, symbol),
        symbol,
    )
    cleanup_changes = _stage_cleanup_changes(run_dir, row)
    manifest.update(
        symbol,
        status="fetching",
        source_used="",
        stage_path=cleanup_changes["stage_path"] or stage_path,
        cleanup_failed=cleanup_changes["cleanup_failed"],
        error_type="",
        error_message="",
        **_clear_stage_evidence(),
    )
    if cleanup_changes["cleanup_failed"]:
        raise MigrationValidationError(f"无法清理旧 staging 文件: {symbol}")

    try:
        klc_frame = fetcher(to_sina_symbol(symbol), start_date, end_date)
    except Exception as exc:
        _set_fetcher_source_from_exception(manifest, symbol, exc)
        raise
    source_used = klc_frame.attrs.get("source") or "sina-klc"
    if not isinstance(source_used, str) or source_used not in MANIFEST_SOURCE_VALUES:
        raise MigrationValidationError(f"K 线数据 source 无效: {source_used!r}")
    manifest.update(symbol, source_used=source_used)
    normalized = _validate_frame(klc_frame, require_close_hfq=True)
    _validate_requested_date_range(normalized, start_date, end_date)
    normalized.attrs["weekend_rows_filtered"] = int(
        klc_frame.attrs.get("weekend_rows_filtered", 0) or 0
    )
    normalized.attrs["known_bad_rows_filtered"] = int(
        klc_frame.attrs.get("known_bad_rows_filtered", 0) or 0
    )
    metrics = _quality_metrics(normalized)
    if metrics["weekend_rows"]:
        quality_error = MigrationQualityError("K 线包含周末日期")
        manifest.update(
            symbol,
            status="failed",
            **metrics,
            error_type=type(quality_error).__name__,
            error_message=str(quality_error),
        )
        raise quality_error
    if metrics["hfq_relation_mismatch_count"]:
        quality_error = MigrationQualityError("close_hfq 与 close * adj_factor 不一致")
        manifest.update(
            symbol,
            status="failed",
            **metrics,
            error_type=type(quality_error).__name__,
            error_message=str(quality_error),
        )
        raise quality_error
    _write_staged_frame(normalized, stage_path, evidence=metrics)
    staged_frame = _read_and_validate(stage_path)
    manifest.update(
        symbol,
        status="staged",
        **metrics,
        new_start_date=staged_frame["date"].min(),
        new_end_date=staged_frame["date"].max(),
        stage_sha256=_sha256(stage_path),
    )


def _resume_staged_symbol(
    row: dict[str, str],
    manifest: MigrationManifest,
    run_dir: Path,
    start_date: str,
    end_date: str,
) -> None:
    stage_path = _safe_stage_path(run_dir, row["stage_path"], row["symbol"])
    expected_stage_sha256 = row["stage_sha256"]
    if not _is_valid_sha256(expected_stage_sha256):
        raise MigrationValidationError("staged manifest 缺少有效 stage_sha256")
    if _sha256(stage_path) != expected_stage_sha256:
        raise MigrationValidationError("staging 文件校验和发生变化")
    staged_frame = _read_and_validate(stage_path)
    _validate_requested_date_range(staged_frame, start_date, end_date)
    metrics = _quality_metrics(staged_frame)
    if metrics["weekend_rows"] or metrics["hfq_relation_mismatch_count"]:
        raise MigrationValidationError("staging K 线质量校验失败")
    metrics["weekend_rows_filtered"] = int(row["weekend_rows_filtered"] or 0)
    metrics["known_bad_rows_filtered"] = int(row["known_bad_rows_filtered"] or 0)
    metrics["hfq_source_rows"] = int(row["hfq_source_rows"] or 0)
    metrics["hfq_forward_filled_rows"] = int(row["hfq_forward_filled_rows"] or 0)
    metrics["hfq_relation_mismatch_count"] = int(
        row["hfq_relation_mismatch_count"] or 0
    )
    metrics["hfq_relation_max_abs_error"] = float(
        row["hfq_relation_max_abs_error"] or 0
    )
    _write_staged_frame(staged_frame, stage_path, evidence=metrics)
    structural_metrics = {
        key: value
        for key, value in metrics.items()
        if key not in {"hfq_relation_mismatch_count", "hfq_relation_max_abs_error"}
    }
    manifest.update(
        row["symbol"],
        status="staged",
        source_used=row["source_used"],
        cleanup_failed="",
        **structural_metrics,
        new_start_date=staged_frame["date"].min(),
        new_end_date=staged_frame["date"].max(),
        stage_sha256=_sha256(stage_path),
    )


def run_kline_source_migration(
    *,
    source: str,
    symbol: str | None = None,
    limit: int | None = 20,
    start_date: str = MIN_KLINE_START_DATE,
    end_date: str | None = None,
    dry_run: bool = False,
    stage_only: bool = True,
    resume_run_id: str | None = None,
    recover_stale_lock: bool = False,
    inter_symbol_delay: tuple[float, float] = (2.0, 4.0),
    warehouse_dir: Path | str | None = None,
    fetcher: ManifestFetcher | None = None,
) -> MigrationResult:
    """Run a serial, manifest-backed KLC staging operation."""

    if not stage_only:
        raise ValueError("当前迁移仅支持 --stage-only staging 模式")
    if source != "sina-klc":
        raise ValueError(f"不支持的迁移源: {source!r}")
    if resume_run_id and (symbol is not None or limit != 20):
        raise ValueError("resume 不能同时指定 symbol 或 limit")
    if dry_run and resume_run_id:
        raise ValueError("dry-run 不能恢复既有迁移")
    if (
        len(inter_symbol_delay) != 2
        or inter_symbol_delay[0] <= 0
        or inter_symbol_delay[1] < inter_symbol_delay[0]
    ):
        raise ValueError("inter_symbol_delay 必须是正数 (最小值, 最大值)")

    install_requests_protection()

    warehouse_path = (
        Path(warehouse_dir) if warehouse_dir is not None else Path(WAREHOUSE_DIR)
    )
    root = get_migration_root(warehouse_path)
    _assert_writable(root)

    if resume_run_id:
        run_id = resume_run_id
        run_dir = _run_path(root, run_id)
        metadata = None
        manifest = None
    else:
        start_date = normalize_kline_start_date(start_date)
        if end_date is None:
            if dry_run:
                end_date = ""
            else:
                end_date = get_latest_trade_date().strftime("%Y%m%d")
        else:
            end_date = _parse_date(end_date, "end_date")
        if end_date and start_date > end_date:
            raise ValueError("start_date 不能晚于 end_date")
        targets = select_migration_symbols(limit=limit, symbol=symbol)
        if not targets:
            raise MigrationSelectionError("没有找到符合条件的股票")
        run_id = _new_run_id()
        run_dir = None
        manifest = None

    if dry_run:
        _check_disk_space(warehouse_path, targets)
        run_id, run_dir, manifest = _build_run(
            root, run_id, targets, source, start_date, end_date, dry_run
        )
        logger.info("迁移 dry-run 完成: %s", run_dir)
        return _format_status_result(run_id, run_dir, manifest, stopped=False)

    active_fetcher = fetcher or _default_fetcher
    stopped_by_sina_block = False
    with MigrationLock(root, run_id, recover_stale=recover_stale_lock):
        if resume_run_id:
            _assert_run_files_safe(run_dir)
            _recover_manifest_upgrade(run_dir)
            run_dir, metadata, manifest = _load_run(root, run_id, allow_legacy=True)
            if metadata.get("source") != source:
                raise ValueError(f"迁移源与既有 run 不一致: {run_id}")
            _validate_run_metadata(
                metadata,
                expected_run_id=run_id,
                expected_schema_version=manifest.schema_version,
                expected_source=source,
                allow_missing_target_evidence=True,
            )
            if metadata.get("dry_run"):
                raise ValueError(f"不能恢复 dry-run: {run_id}")
            if (
                manifest.schema_version == MANIFEST_SCHEMA_VERSION
                and set(metadata) == BASE_RUN_METADATA_FIELDS
            ):
                _upgrade_current_run_metadata(run_dir, metadata, manifest)
                run_dir, metadata, manifest = _load_run(root, run_id)
            elif manifest.schema_version in UPGRADABLE_MANIFEST_SCHEMA_VERSIONS:
                _upgrade_legacy_run(run_dir, metadata, manifest)
                run_dir, metadata, manifest = _load_run(root, run_id)
            stored_start_date = _parse_date(str(metadata["start_date"]), "start_date")
            start_date = normalize_kline_start_date(stored_start_date)
            if start_date != stored_start_date:
                raise ValueError(
                    "既有迁移 run 起始日期早于最低日期，请新建 2010 年起始的 run"
                )
            end_date = str(metadata.get("end_date") or "")
            if not end_date:
                end_date = get_latest_trade_date().strftime("%Y%m%d")
        if not resume_run_id:
            _check_unfinished_runs(root, source)
            _check_disk_space(warehouse_path, targets)
            run_id, run_dir, manifest = _build_run(
                root, run_id, targets, source, start_date, end_date, dry_run
            )
        for row in manifest.rows:
            if row["status"] == "staged":
                stage_path = _safe_stage_path(run_dir, row["stage_path"], row["symbol"])
            else:
                stage_path = None
            if stage_path is not None and stage_path.exists():
                try:
                    _resume_staged_symbol(row, manifest, run_dir, start_date, end_date)
                    continue
                except Exception:
                    logger.warning("staging 校验失败，重新抓取: %s", row["symbol"])
            try:
                _stage_symbol(
                    row,
                    manifest,
                    run_dir,
                    start_date,
                    end_date,
                    active_fetcher,
                )
            except SinaBlockedError as exc:
                stopped_by_sina_block = True
                cleanup_changes = _stage_cleanup_changes(run_dir, row)
                manifest.update(
                    row["symbol"],
                    status="failed",
                    **cleanup_changes,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    **_clear_stage_evidence(),
                )
                logger.error("新浪风控触发，停止迁移: %s", row["symbol"])
                break
            except MigrationQualityError as exc:
                cleanup_changes = _stage_cleanup_changes(run_dir, row)
                manifest.update(
                    row["symbol"],
                    status="failed",
                    **cleanup_changes,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                logger.error("K 线质量门禁失败: %s", row["symbol"])
            except MigrationValidationError as exc:
                cleanup_changes = _stage_cleanup_changes(run_dir, row)
                manifest.update(
                    row["symbol"],
                    status="failed",
                    **cleanup_changes,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    **_clear_stage_evidence(),
                )
                logger.exception("K 线迁移校验失败: %s", row["symbol"])
            except Exception as exc:
                cleanup_changes = _stage_cleanup_changes(run_dir, row)
                manifest.update(
                    row["symbol"],
                    status="failed",
                    **cleanup_changes,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    **_clear_stage_evidence(),
                )
                logger.exception("K 线迁移失败: %s", row["symbol"])
            if not stopped_by_sina_block:
                delay = random.uniform(*inter_symbol_delay)
                if delay:
                    time.sleep(delay)

    return _format_status_result(run_id, run_dir, manifest, stopped_by_sina_block)


def iter_manifest_rows(run_dir: Path) -> Iterator[dict[str, str]]:
    """Yield manifest rows for external audit scripts and tests."""

    yield from MigrationManifest.load(run_dir / MANIFEST_FILENAME).rows
