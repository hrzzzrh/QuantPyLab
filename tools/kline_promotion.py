"""Promote validated K-line staging partitions to canonical storage safely."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import os
import re
import shutil
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from config.settings import MIN_KLINE_START_DATE, WAREHOUSE_DIR
from tools import kline_source_migration as staging
from utils.canonical_write_lock import CanonicalWriteLock
from utils.kline_policy import KNOWN_BAD_KLINE_ROWS

PROMOTION_ROOT_NAME = ".migrations/kline-promotion"
PROMOTION_MANIFEST_FILENAME = "manifest.csv"
PROMOTION_METADATA_FILENAME = "run.json"
PROMOTION_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
PROMOTION_STATE_FILENAME_PATTERN = re.compile(r"^symbol=([0-9]{6})\.json$")
PROMOTION_STATE_TEMP_PATTERN = re.compile(
    r"^\.symbol=([0-9]{6})\.json\.[0-9a-f]{32}\.tmp$"
)
PROMOTION_SCHEMA_VERSION = 1
MIN_PROMOTION_FREE_BYTES = 100 * 1024 * 1024

PROMOTION_MANIFEST_FIELDS = (
    "symbol",
    "source_stage_path",
    "canonical_path",
    "canonical_existed",
    "original_canonical_sha256",
    "backup_path",
    "source_stage_sha256",
    "backup_sha256",
    "canonical_sha256",
    "status",
    "error_type",
    "error_message",
    "updated_at",
)
PROMOTION_METADATA_FIELDS = frozenset(
    {
        "promotion_run_id",
        "promotion_schema_version",
        "staging_run_id",
        "target_count",
        "target_symbols_sha256",
        "status",
        "created_at",
        "updated_at",
    }
)
PROMOTION_STATUSES = frozenset(
    {
        "pending",
        "validated",
        "backing_up",
        "backed_up",
        "committing",
        "committed",
        "failed",
        "rollback-required",
        "rolled_back",
    }
)


class KlinePromotionError(RuntimeError):
    """Raised when a canonical promotion cannot safely continue."""


@dataclass(frozen=True)
class PromotionResult:
    promotion_run_id: str
    staging_run_id: str
    promoted_symbols: tuple[str, ...]
    failed_symbols: tuple[str, ...]
    validated_symbols: tuple[str, ...] = ()
    status: str = "completed"
    dry_run: bool = False


def get_promotion_root(warehouse_dir: Path | str | None = None) -> Path:
    base_dir = Path(warehouse_dir) if warehouse_dir is not None else Path(WAREHOUSE_DIR)
    base_resolved = base_dir.resolve()
    root = base_dir / PROMOTION_ROOT_NAME
    if not root.resolve(strict=False).is_relative_to(base_resolved):
        raise KlinePromotionError(f"晋级目录越出数据仓: {root}")
    return root


def _promotion_path(root: Path, promotion_run_id: str) -> Path:
    if not PROMOTION_ID_PATTERN.fullmatch(promotion_run_id):
        raise KlinePromotionError(f"非法 promotion run id: {promotion_run_id!r}")
    root_resolved = root.resolve()
    if root.is_symlink():
        raise KlinePromotionError("晋级根目录不得是符号链接")
    path = root / promotion_run_id
    if path.is_symlink() or path.parent != root:
        raise KlinePromotionError(f"晋级 run 路径不安全: {promotion_run_id!r}")
    if not path.resolve(strict=False).is_relative_to(root_resolved):
        raise KlinePromotionError(f"晋级 run 路径越界: {promotion_run_id!r}")
    return path


def _new_promotion_run_id() -> str:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{timestamp}-kline-promotion-{uuid.uuid4().hex[:8]}"


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return staging._json_bytes(payload)


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise KlinePromotionError(f"晋级文件不得是符号链接: {path}")
    parent_fd = _open_directory_no_follow(path.parent)
    temp_name = f".{path.name}.{uuid.uuid4().hex}.tmp"
    temp_fd = None
    try:
        temp_fd = os.open(
            temp_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o644,
            dir_fd=parent_fd,
        )
        with os.fdopen(temp_fd, "wb") as file:
            temp_fd = None
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        os.replace(
            temp_name,
            path.name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
        os.fsync(parent_fd)
    except Exception:
        if temp_fd is not None:
            os.close(temp_fd)
        try:
            os.unlink(temp_name, dir_fd=parent_fd)
        except FileNotFoundError:
            pass
        raise
    finally:
        os.close(parent_fd)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _atomic_write_bytes(path, _json_bytes(payload))


def _read_json(path: Path) -> dict[str, Any]:
    fd = _open_file_no_follow(path, os.O_RDONLY)
    with os.fdopen(fd, "rb") as file:
        payload = json.loads(file.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise KlinePromotionError(f"晋级 JSON 必须是对象: {path}")
    return payload


def _sha256(path: Path) -> str:
    fd = _open_file_no_follow(path, os.O_RDONLY)
    digest = hashlib.sha256()
    with os.fdopen(fd, "rb") as file:
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
    except OSError as exc:
        os.close(directory_fd)
        raise KlinePromotionError(f"无法安全打开目录: {path}") from exc


def _open_file_no_follow(path: Path, flags: int, mode: int = 0o644) -> int:
    parent_fd = _open_directory_no_follow(path.parent)
    try:
        return os.open(
            path.name,
            flags | getattr(os, "O_NOFOLLOW", 0),
            mode,
            dir_fd=parent_fd,
        )
    except OSError as exc:
        raise KlinePromotionError(f"无法安全打开文件: {path}") from exc
    finally:
        os.close(parent_fd)


def _is_valid_sha256(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{64}", value))


def _target_symbols_sha256(symbols: Iterable[str]) -> str:
    return hashlib.sha256("\n".join(symbols).encode("ascii")).hexdigest()


def _target_evidence(symbols: Iterable[str]) -> dict[str, int | str]:
    values = list(symbols)
    return {
        "target_count": len(values),
        "target_symbols_sha256": _target_symbols_sha256(values),
    }


def _manifest_csv_bytes(rows: list[dict[str, str]]) -> bytes:
    from io import StringIO

    buffer = StringIO(newline="")
    writer = csv.DictWriter(
        buffer, fieldnames=PROMOTION_MANIFEST_FIELDS, lineterminator="\n"
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {field: row.get(field, "") for field in PROMOTION_MANIFEST_FIELDS}
        )
    return buffer.getvalue().encode("utf-8")


class PromotionManifest:
    """Atomically persisted per-symbol canonical promotion state."""

    def __init__(
        self,
        path: Path,
        rows: list[dict[str, str]],
        state_dir: Path | None = None,
    ) -> None:
        self.path = path
        self.rows = rows
        self._rows_by_symbol = {row["symbol"]: row for row in rows}
        self.state_dir = state_dir or path.parent / "state"

    @classmethod
    def create(cls, path: Path, rows: list[dict[str, str]]) -> PromotionManifest:
        if not rows:
            raise KlinePromotionError("晋级 manifest 不得为空")
        manifest = cls(path, rows)
        manifest.save()
        return manifest

    @classmethod
    def load(cls, path: Path) -> PromotionManifest:
        if path.is_symlink() or not path.exists():
            raise KlinePromotionError(f"找不到晋级 manifest: {path}")
        fd = _open_file_no_follow(path, os.O_RDONLY)
        with os.fdopen(fd, "r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            if reader.fieldnames != list(PROMOTION_MANIFEST_FIELDS):
                raise KlinePromotionError(f"晋级 manifest 字段错误: {path}")
            rows = []
            for raw_row in reader:
                if set(raw_row) != set(PROMOTION_MANIFEST_FIELDS) or any(
                    value is None for value in raw_row.values()
                ):
                    raise KlinePromotionError(f"晋级 manifest 行字段错误: {path}")
                row = {
                    field: raw_row.get(field, "") or ""
                    for field in PROMOTION_MANIFEST_FIELDS
                }
                if any(existing["symbol"] == row["symbol"] for existing in rows):
                    raise KlinePromotionError(
                        f"晋级 manifest 重复股票: {row['symbol']}"
                    )
                rows.append(row)
        if not rows:
            raise KlinePromotionError(f"晋级 manifest 不得为空: {path}")
        state_dir = path.parent / "state"
        if state_dir.exists():
            if state_dir.is_symlink():
                raise KlinePromotionError("晋级状态目录不得是符号链接")
            by_symbol = {row["symbol"]: row for row in rows}
            for state_path in state_dir.iterdir():
                match = PROMOTION_STATE_FILENAME_PATTERN.fullmatch(state_path.name)
                if state_path.is_symlink() or match is None:
                    raise KlinePromotionError(f"晋级状态文件不安全: {state_path}")
                state_row = _read_json(state_path)
                if set(state_row) != set(PROMOTION_MANIFEST_FIELDS):
                    raise KlinePromotionError(f"晋级状态字段错误: {state_path}")
                symbol = state_row.get("symbol")
                if symbol != match.group(1) or symbol not in by_symbol:
                    raise KlinePromotionError(f"晋级状态股票不存在: {state_path}")
                by_symbol[symbol] = {
                    field: str(state_row.get(field, "") or "")
                    for field in PROMOTION_MANIFEST_FIELDS
                }
            rows = list(by_symbol.values())
        for row in rows:
            staging._validate_symbol(row["symbol"])
            if row["status"] not in PROMOTION_STATUSES:
                raise KlinePromotionError(
                    f"晋级 manifest 状态无效: {row['symbol']}: {row['status']}"
                )
            if not _is_valid_sha256(row["source_stage_sha256"]):
                raise KlinePromotionError(
                    f"晋级 manifest 缺少 staged 摘要: {row['symbol']}"
                )
            if row["canonical_existed"] not in {"0", "1"}:
                raise KlinePromotionError(
                    f"晋级 manifest canonical_existed 无效: {row['symbol']}"
                )
            if not row["source_stage_path"] or not row["canonical_path"]:
                raise KlinePromotionError(f"晋级 manifest 路径不完整: {row['symbol']}")
            if row["canonical_existed"] == "1" and not _is_valid_sha256(
                row["original_canonical_sha256"]
            ):
                raise KlinePromotionError(
                    f"晋级 manifest 缺少旧 canonical 摘要: {row['symbol']}"
                )
            if row["canonical_existed"] == "0" and row["original_canonical_sha256"]:
                raise KlinePromotionError(
                    f"无旧 canonical 不得包含旧摘要: {row['symbol']}"
                )
            if row["backup_sha256"] and (
                not row["backup_path"] or not _is_valid_sha256(row["backup_sha256"])
            ):
                raise KlinePromotionError(
                    f"晋级 manifest backup 证据不完整: {row['symbol']}"
                )
            if (
                row["backup_path"]
                and not row["backup_sha256"]
                and row["status"]
                in {
                    "backed_up",
                    "committing",
                    "committed",
                    "rollback-required",
                    "rolled_back",
                }
            ):
                raise KlinePromotionError(
                    f"晋级 manifest 缺少 backup 摘要: {row['symbol']}"
                )
            if row["status"] in {"committed", "rolled_back"} and not _is_valid_sha256(
                row["canonical_sha256"]
            ):
                raise KlinePromotionError(
                    f"晋级 manifest 缺少 canonical 摘要: {row['symbol']}"
                )
            try:
                datetime.fromisoformat(row["updated_at"])
            except ValueError as exc:
                raise KlinePromotionError(
                    f"晋级 manifest updated_at 无效: {row['symbol']}"
                ) from exc
        return cls(path, rows, state_dir=state_dir)

    def save(self) -> None:
        _atomic_write_bytes(self.path, _manifest_csv_bytes(self.rows))

    def update(self, symbol: str, **changes: Any) -> dict[str, str]:
        if symbol not in self._rows_by_symbol:
            raise KlinePromotionError(f"晋级 manifest 不存在股票: {symbol}")
        row = self._rows_by_symbol[symbol]
        for key, value in changes.items():
            if key not in PROMOTION_MANIFEST_FIELDS:
                raise KeyError(f"未知晋级 manifest 字段: {key}")
            row[key] = "" if value is None else str(value)
        row["updated_at"] = datetime.now().isoformat(timespec="seconds")
        state_path = self.state_dir / f"symbol={symbol}.json"
        _atomic_write_bytes(state_path, _json_bytes(dict(row)))
        return row

    def compact(self) -> None:
        self.save()
        if not self.state_dir.exists():
            return
        if self.state_dir.is_symlink():
            raise KlinePromotionError("晋级状态目录不得是符号链接")
        for state_path in self.state_dir.iterdir():
            if (
                state_path.is_symlink()
                or not PROMOTION_STATE_FILENAME_PATTERN.fullmatch(state_path.name)
            ):
                raise KlinePromotionError(f"晋级状态文件不安全: {state_path}")
            _unlink_file_no_follow(state_path)
        _fsync_directory(self.state_dir)

    def get(self, symbol: str) -> dict[str, str]:
        return self._rows_by_symbol[symbol]


def _validate_metadata(metadata: dict[str, Any], promotion_run_id: str) -> None:
    if set(metadata) != PROMOTION_METADATA_FIELDS:
        raise KlinePromotionError("晋级 run.json 字段不完整或包含未知字段")
    if metadata["promotion_run_id"] != promotion_run_id:
        raise KlinePromotionError("晋级 run id 不匹配")
    if metadata["promotion_schema_version"] != PROMOTION_SCHEMA_VERSION:
        raise KlinePromotionError("不支持的晋级 schema 版本")
    if metadata["status"] not in {
        "pending",
        "validated",
        "partial",
        "committing",
        "completed",
        "failed",
        "rolled_back",
    }:
        raise KlinePromotionError("晋级 run 状态无效")
    if type(metadata["target_count"]) is not int or metadata["target_count"] <= 0:
        raise KlinePromotionError("晋级目标数量无效")
    if not _is_valid_sha256(metadata["target_symbols_sha256"]):
        raise KlinePromotionError("晋级目标摘要无效")
    if type(metadata["staging_run_id"]) is not str or not metadata["staging_run_id"]:
        raise KlinePromotionError("晋级缺少 staging run id")
    for field in ("created_at", "updated_at"):
        try:
            datetime.fromisoformat(metadata[field])
        except (KeyError, TypeError, ValueError) as exc:
            raise KlinePromotionError(f"晋级 {field} 无效") from exc


def _safe_canonical_path(
    warehouse_dir: Path, symbol: str, *, create_dirs: bool = True
) -> Path:
    staging._validate_symbol(symbol)
    base_dir = warehouse_dir.resolve()
    daily_dir = warehouse_dir / "daily_kline"
    if daily_dir.exists() and daily_dir.is_symlink():
        raise KlinePromotionError("canonical daily_kline 目录不得是符号链接")
    if create_dirs:
        daily_dir.mkdir(parents=True, exist_ok=True)
    daily_resolved = daily_dir.resolve(strict=False)
    if daily_resolved != base_dir / "daily_kline":
        raise KlinePromotionError("canonical daily_kline 目录解析越界")
    symbol_dir = daily_dir / f"symbol={symbol}"
    if symbol_dir.exists() and symbol_dir.is_symlink():
        raise KlinePromotionError("canonical symbol 目录不得是符号链接")
    if create_dirs:
        symbol_dir.mkdir(parents=True, exist_ok=True)
    symbol_resolved = symbol_dir.resolve(strict=False)
    if symbol_resolved != daily_resolved / f"symbol={symbol}":
        raise KlinePromotionError("canonical symbol 目录解析越界")
    path = symbol_dir / "data.parquet"
    if path.is_symlink() or not path.resolve(strict=False).is_relative_to(base_dir):
        raise KlinePromotionError("canonical 文件路径不安全")
    return path.resolve(strict=False)


def _safe_promotion_file(run_dir: Path, relative_path: Path) -> Path:
    if relative_path.is_absolute() or any(part == ".." for part in relative_path.parts):
        raise KlinePromotionError("晋级文件路径越界")
    path = run_dir / relative_path
    root = run_dir.resolve()
    if path.is_symlink() or not path.resolve(strict=False).is_relative_to(root):
        raise KlinePromotionError("晋级文件路径不安全")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.parent.is_symlink():
        raise KlinePromotionError("晋级文件父目录不得是符号链接")
    return path


def _fsync_directory(path: Path) -> None:
    fd = _open_directory_no_follow(path)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _copy_file_atomic(source: Path, destination: Path) -> None:
    if source.is_symlink() or not source.is_file():
        raise KlinePromotionError(f"源文件不存在或为符号链接: {source}")
    if destination.is_symlink():
        raise KlinePromotionError(f"目标文件不得是符号链接: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.parent.is_symlink():
        raise KlinePromotionError(f"目标父目录不得是符号链接: {destination.parent}")
    source_fd = None
    temp_fd = None
    destination_dir_fd = None
    temp_name = f".{destination.name}.{uuid.uuid4().hex}.tmp"
    temp_path = destination.parent / temp_name
    try:
        source_fd = _open_file_no_follow(source, os.O_RDONLY)
        destination_dir_fd = _open_directory_no_follow(destination.parent)
        temp_fd = os.open(
            temp_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o644,
            dir_fd=destination_dir_fd,
        )
        with os.fdopen(source_fd, "rb") as source_file:
            source_fd = None
            with os.fdopen(temp_fd, "wb") as destination_file:
                temp_fd = None
                shutil.copyfileobj(source_file, destination_file)
                destination_file.flush()
                os.fsync(destination_file.fileno())
        os.replace(
            temp_name,
            destination.name,
            src_dir_fd=destination_dir_fd,
            dst_dir_fd=destination_dir_fd,
        )
        os.fsync(destination_dir_fd)
    except Exception:
        if source_fd is not None:
            os.close(source_fd)
        if temp_fd is not None:
            os.close(temp_fd)
        if destination_dir_fd is not None:
            try:
                os.unlink(temp_name, dir_fd=destination_dir_fd)
            except FileNotFoundError:
                pass
        else:
            temp_path.unlink(missing_ok=True)
        if destination_dir_fd is not None:
            os.close(destination_dir_fd)
        raise
    else:
        if destination_dir_fd is not None:
            os.close(destination_dir_fd)


def _unlink_file_no_follow(path: Path) -> None:
    if path.is_symlink():
        raise KlinePromotionError(f"文件不得是符号链接: {path}")
    parent_fd = _open_directory_no_follow(path.parent)
    try:
        try:
            os.unlink(path.name, dir_fd=parent_fd)
        except FileNotFoundError:
            return
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)


def _read_file_bytes_no_follow(path: Path) -> bytes:
    fd = _open_file_no_follow(path, os.O_RDONLY)
    with os.fdopen(fd, "rb") as file:
        return file.read()


def _read_parquet_no_follow(path: Path) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(_read_file_bytes_no_follow(path)))


def _read_and_validate_stage(path: Path, row: dict[str, str]) -> pd.DataFrame:
    if not path.exists() or path.is_symlink():
        raise KlinePromotionError(f"staging 文件不存在或为符号链接: {path}")
    raw_bytes = _read_file_bytes_no_follow(path)
    if hashlib.sha256(raw_bytes).hexdigest() != row["stage_sha256"]:
        raise KlinePromotionError(f"staging 文件摘要变化: {row['symbol']}")
    import pyarrow.parquet as pq

    parquet_metadata = pq.ParquetFile(io.BytesIO(raw_bytes)).schema_arrow.metadata or {}
    raw_evidence = parquet_metadata.get(staging.STAGE_EVIDENCE_METADATA_KEY)
    if raw_evidence is None:
        raise KlinePromotionError(f"staging 缺少不可变质量证据: {row['symbol']}")
    try:
        evidence = json.loads(raw_evidence.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise KlinePromotionError(f"staging 质量证据格式无效: {row['symbol']}") from exc
    for field in staging._STAGE_EVIDENCE_FIELDS:
        if field == "stage_sha256":
            continue
        if str(evidence.get(field, "")) != row[field]:
            raise KlinePromotionError(
                f"staging 不可变质量证据不一致: {row['symbol']}: {field}"
            )
    frame = pd.read_parquet(io.BytesIO(raw_bytes))
    if "date" not in frame.columns:
        raise KlinePromotionError(f"staging 缺少日期字段: {row['symbol']}")
    frame_with_hfq = frame.copy()
    frame_with_hfq["close_hfq"] = frame_with_hfq["close"] * frame_with_hfq["adj_factor"]
    try:
        normalized = staging.validate_kline_frame(frame_with_hfq)
    except staging.KlineValidationError as exc:
        raise KlinePromotionError(
            f"staging 质量校验失败: {row['symbol']}: {exc}"
        ) from exc
    actual_metrics = staging._quality_metrics(normalized)
    for field in (
        "new_rows",
        "weekend_rows",
        "zero_volume_rows",
        "zero_amount_rows",
        "factor_one_rows",
        "hfq_relation_mismatch_count",
    ):
        try:
            expected = int(row[field])
        except (KeyError, TypeError, ValueError) as exc:
            raise KlinePromotionError(
                f"staging 质量证据无效: {row['symbol']}: {field}"
            ) from exc
        if actual_metrics[field] != expected:
            raise KlinePromotionError(
                f"staging 质量证据不一致: {row['symbol']}: {field}"
            )
    for field in ("factor_min", "factor_max", "hfq_relation_max_abs_error"):
        try:
            expected_float = float(row[field])
        except (KeyError, TypeError, ValueError) as exc:
            raise KlinePromotionError(
                f"staging 质量证据无效: {row['symbol']}: {field}"
            ) from exc
        if not math.isclose(
            actual_metrics[field], expected_float, rel_tol=1e-12, abs_tol=1e-12
        ):
            raise KlinePromotionError(
                f"staging 质量证据不一致: {row['symbol']}: {field}"
            )
    for field in (
        "weekend_rows_filtered",
        "known_bad_rows_filtered",
        "hfq_source_rows",
        "hfq_forward_filled_rows",
    ):
        try:
            if int(row[field]) < 0:
                raise ValueError
        except (KeyError, TypeError, ValueError) as exc:
            raise KlinePromotionError(
                f"staging 质量证据无效: {row['symbol']}: {field}"
            ) from exc
    if str(normalized["date"].min()) != row["new_start_date"]:
        raise KlinePromotionError(f"staging 起始日期证据不一致: {row['symbol']}")
    if str(normalized["date"].max()) != row["new_end_date"]:
        raise KlinePromotionError(f"staging 结束日期证据不一致: {row['symbol']}")
    known_bad_dates = {
        bad_date
        for bad_symbol, bad_date in KNOWN_BAD_KLINE_ROWS
        if bad_symbol == row["symbol"]
    }
    actual_dates = set(normalized["date"].astype(str))
    if actual_dates & known_bad_dates:
        raise KlinePromotionError(f"staging 仍含已知坏日期: {row['symbol']}")
    return normalized.drop(columns=["close_hfq"])


def _read_and_validate_canonical(path: Path, symbol: str) -> pd.DataFrame:
    if not path.exists() or path.is_symlink():
        raise KlinePromotionError(
            f"恢复后的 canonical 文件不存在或为符号链接: {symbol}"
        )
    frame = _read_parquet_no_follow(path)
    frame_with_hfq = frame.copy()
    frame_with_hfq["close_hfq"] = frame_with_hfq["close"] * frame_with_hfq["adj_factor"]
    try:
        normalized = staging.validate_kline_frame(frame_with_hfq)
    except staging.KlineValidationError as exc:
        raise KlinePromotionError(
            f"恢复后的 canonical 质量校验失败: {symbol}: {exc}"
        ) from exc
    return normalized.drop(columns=["close_hfq"])


def _load_staging_targets(
    staging_run_id: str,
    warehouse_dir: Path,
    symbol: str | None = None,
) -> tuple[Path, dict[str, Any], staging.MigrationManifest, list[dict[str, str]]]:
    source_root = staging.get_migration_root(warehouse_dir)
    run_dir, metadata, manifest = staging._load_run(source_root, staging_run_id)
    if manifest.schema_version != staging.MANIFEST_SCHEMA_VERSION:
        raise KlinePromotionError("晋级只支持 schema 6 staging run")
    if metadata.get("source") != "sina-klc":
        raise KlinePromotionError("晋级只支持 sina-klc staging run")
    try:
        start_date = datetime.strptime(metadata["start_date"], "%Y%m%d").date()
        end_date = datetime.strptime(metadata["end_date"], "%Y%m%d").date()
    except (KeyError, TypeError, ValueError) as exc:
        raise KlinePromotionError("staging run 日期窗口无效") from exc
    minimum_date = datetime.strptime(MIN_KLINE_START_DATE, "%Y%m%d").date()
    if start_date < minimum_date or start_date > end_date:
        raise KlinePromotionError("staging run 起止日期不符合最低日期策略")
    staging._validate_target_evidence(metadata, manifest)
    if any(row["status"] != "staged" for row in manifest.rows):
        raise KlinePromotionError("staging run 存在未完成或失败股票，禁止晋级")
    selected = manifest.rows
    if symbol is not None:
        staging._validate_symbol(symbol)
        selected = [manifest.get(symbol)]
    for row in selected:
        stage_path = staging._safe_stage_path(
            run_dir,
            row["stage_path"],
            row["symbol"],
            create_dirs=False,
        )
        frame = _read_and_validate_stage(stage_path, row)
        if frame["date"].min() < start_date or frame["date"].max() > end_date:
            raise KlinePromotionError(f"staging 日期超出冻结窗口: {row['symbol']}")
        if frame["date"].min() < minimum_date:
            raise KlinePromotionError(f"staging 日期早于最低起始日: {row['symbol']}")
    return run_dir, metadata, manifest, selected


def _new_promotion_row(
    row: dict[str, str], warehouse_dir: Path, run_dir: Path
) -> dict[str, str]:
    canonical_path = _safe_canonical_path(warehouse_dir, row["symbol"])
    source_path = staging._safe_stage_path(run_dir, row["stage_path"], row["symbol"])
    canonical_existed = canonical_path.exists()
    return {
        "symbol": row["symbol"],
        "source_stage_path": str(source_path),
        "canonical_path": str(canonical_path),
        "canonical_existed": "1" if canonical_existed else "0",
        "original_canonical_sha256": _sha256(canonical_path)
        if canonical_existed
        else "",
        "backup_path": "",
        "source_stage_sha256": row["stage_sha256"],
        "backup_sha256": "",
        "canonical_sha256": "",
        "status": "pending",
        "error_type": "",
        "error_message": "",
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


def _write_promotion_metadata(
    run_dir: Path, metadata: dict[str, Any], *, status: str
) -> dict[str, Any]:
    updated = {
        **metadata,
        "status": status,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    _validate_metadata(updated, run_dir.name)
    _write_json(run_dir / PROMOTION_METADATA_FILENAME, updated)
    return updated


def _recover_promotion_state_journal(run_dir: Path) -> None:
    state_dir = run_dir / "state"
    if not state_dir.exists():
        return
    if state_dir.is_symlink():
        raise KlinePromotionError("晋级状态目录不得是符号链接")
    removed_temp = False
    for state_path in state_dir.iterdir():
        if PROMOTION_STATE_FILENAME_PATTERN.fullmatch(state_path.name):
            continue
        if PROMOTION_STATE_TEMP_PATTERN.fullmatch(state_path.name):
            _unlink_file_no_follow(state_path)
            removed_temp = True
            continue
        raise KlinePromotionError(f"晋级状态文件不安全: {state_path}")
    if removed_temp:
        _fsync_directory(state_dir)


def _load_promotion_run(
    promotion_run_id: str, warehouse_dir: Path
) -> tuple[Path, dict[str, Any], PromotionManifest]:
    root = get_promotion_root(warehouse_dir)
    run_dir = _promotion_path(root, promotion_run_id)
    if not run_dir.is_dir() or run_dir.is_symlink():
        raise KlinePromotionError(f"找不到晋级 run: {promotion_run_id}")
    if (run_dir / PROMOTION_METADATA_FILENAME).is_symlink():
        raise KlinePromotionError("晋级 run.json 不得是符号链接")
    metadata = _read_json(run_dir / PROMOTION_METADATA_FILENAME)
    _validate_metadata(metadata, promotion_run_id)
    _recover_promotion_state_journal(run_dir)
    manifest = PromotionManifest.load(run_dir / PROMOTION_MANIFEST_FILENAME)
    symbols = [row["symbol"] for row in manifest.rows]
    actual = _target_evidence(symbols)
    if (
        metadata["target_count"] != actual["target_count"]
        or metadata["target_symbols_sha256"] != actual["target_symbols_sha256"]
    ):
        raise KlinePromotionError("晋级 manifest 股票清单与 run.json 不一致")
    row_statuses = {row["status"] for row in manifest.rows}
    if metadata["status"] == "completed" and row_statuses != {"committed"}:
        raise KlinePromotionError("completed 晋级 run 存在未 committed 股票")
    if metadata["status"] == "rolled_back" and not row_statuses.issubset(
        {"rolled_back", "pending", "failed"}
    ):
        raise KlinePromotionError("rolled_back 晋级 run 存在未恢复股票")
    for row in manifest.rows:
        expected_canonical = _safe_canonical_path(warehouse_dir, row["symbol"])
        if Path(row["canonical_path"]).resolve(strict=False) != expected_canonical:
            raise KlinePromotionError(f"晋级 canonical 路径不匹配: {row['symbol']}")
        if row["backup_path"]:
            expected_backup = (
                run_dir / "backup" / f"symbol={row['symbol']}" / "data.parquet"
            )
            if (
                Path(row["backup_path"]).resolve(strict=False)
                != expected_backup.resolve()
            ):
                raise KlinePromotionError(f"晋级 backup 路径不匹配: {row['symbol']}")
    return run_dir, metadata, manifest


def _restore_backup(row: dict[str, str]) -> None:
    canonical = Path(row["canonical_path"])
    backup = Path(row["backup_path"]) if row["backup_path"] else None
    _assert_backup_state(row)
    if backup is not None:
        _copy_file_atomic(backup, canonical)
        return
    if row["canonical_existed"] != "0":
        raise KlinePromotionError(f"已有 canonical 缺少 backup: {row['symbol']}")
    if canonical.exists():
        if canonical.is_symlink():
            raise KlinePromotionError(f"新建 canonical 不得是符号链接: {row['symbol']}")
        expected_hash = row["canonical_sha256"] or row["source_stage_sha256"]
        if _sha256(canonical) != expected_hash:
            raise KlinePromotionError(f"新建 canonical 摘要未知: {row['symbol']}")
        _unlink_file_no_follow(canonical)


def _file_hash_or_none(path: Path) -> str | None:
    if path.is_symlink():
        raise KlinePromotionError(f"文件不得是符号链接: {path}")
    if not path.exists():
        return None
    return _sha256(path)


def _assert_original_canonical_state(row: dict[str, str], canonical: Path) -> None:
    current_hash = _file_hash_or_none(canonical)
    expected_hash = row["original_canonical_sha256"] or None
    if current_hash != expected_hash:
        raise KlinePromotionError(f"canonical 在晋级期间发生变化: {row['symbol']}")


def _assert_backup_state(row: dict[str, str]) -> None:
    if row["canonical_existed"] == "0":
        if row["backup_path"] or row["backup_sha256"]:
            raise KlinePromotionError(
                f"无旧 canonical 不得存在 backup: {row['symbol']}"
            )
        return
    backup = Path(row["backup_path"]) if row["backup_path"] else None
    if backup is None or not backup.exists() or backup.is_symlink():
        raise KlinePromotionError(f"旧 canonical backup 缺失: {row['symbol']}")
    if row["backup_sha256"] != row["original_canonical_sha256"]:
        raise KlinePromotionError(f"backup 与旧 canonical 摘要不一致: {row['symbol']}")
    if not row["backup_sha256"] or _sha256(backup) != row["backup_sha256"]:
        raise KlinePromotionError(f"旧 canonical backup 摘要不一致: {row['symbol']}")


def _mark_promotion_failure(
    manifest: PromotionManifest,
    row: dict[str, str],
    canonical: Path,
    exc: BaseException,
) -> None:
    try:
        if row["status"] == "rollback-required":
            manifest.update(
                row["symbol"],
                status="rollback-required",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            return
        current_hash = _file_hash_or_none(canonical)
        expected_original = row["original_canonical_sha256"] or None
        if current_hash == expected_original or (
            current_hash is None and expected_original is None
        ):
            status = "failed"
        else:
            status = "rollback-required"
        manifest.update(
            row["symbol"],
            status=status,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
    except Exception:
        manifest.update(
            row["symbol"],
            status="rollback-required",
            error_type=type(exc).__name__,
            error_message=str(exc),
        )


def _promote_row(
    manifest: PromotionManifest, row: dict[str, str], run_dir: Path
) -> None:
    symbol = row["symbol"]
    canonical = Path(row["canonical_path"])
    stage_path = Path(row["source_stage_path"])
    backup = _safe_promotion_file(
        run_dir, Path("backup") / f"symbol={symbol}" / "data.parquet"
    )
    backup_path = Path(row["backup_path"]) if row["backup_path"] else backup
    try:
        status = row["status"]
        if status == "committed":
            _assert_backup_state(row)
            if _file_hash_or_none(canonical) == row["canonical_sha256"]:
                return
            manifest.update(symbol, status="rollback-required")
            raise KlinePromotionError(f"已提交 canonical 摘要不一致: {symbol}")
        if status in {"rolled_back", "rollback-required"}:
            raise KlinePromotionError(f"状态不允许继续晋级: {symbol}: {status}")

        if (
            stage_path.is_symlink()
            or not stage_path.exists()
            or _sha256(stage_path) != row["source_stage_sha256"]
        ):
            raise KlinePromotionError(f"staging 文件在晋级期间发生变化: {symbol}")

        if canonical.is_symlink():
            raise KlinePromotionError(f"canonical 文件不得是符号链接: {symbol}")
        if status in {"pending", "failed"}:
            _assert_original_canonical_state(row, canonical)
        elif status in {"backing_up", "backed_up"}:
            _assert_original_canonical_state(row, canonical)
        elif status == "committing":
            _assert_backup_state(row)
            current_hash = _file_hash_or_none(canonical)
            if current_hash == row["source_stage_sha256"]:
                manifest.update(
                    symbol,
                    status="committed",
                    canonical_sha256=current_hash,
                    error_type="",
                    error_message="",
                )
                return
            if current_hash not in {row["original_canonical_sha256"] or None}:
                manifest.update(symbol, status="rollback-required")
                raise KlinePromotionError(f"committing 状态文件摘要未知: {symbol}")

        if row["status"] in {"backing_up", "backed_up"}:
            if row["backup_sha256"]:
                _assert_backup_state(row)
            elif row["canonical_existed"] == "1":
                _copy_file_atomic(canonical, backup_path)
                row = manifest.update(
                    symbol,
                    status="backed_up",
                    backup_path=str(backup_path),
                    backup_sha256=_sha256(backup_path),
                )
            else:
                row = manifest.update(symbol, status="backed_up")
        elif row["status"] in {"pending", "failed"}:
            if row["canonical_existed"] == "1":
                manifest.update(
                    symbol, status="backing_up", backup_path=str(backup_path)
                )
                _copy_file_atomic(canonical, backup_path)
                row = manifest.update(
                    symbol,
                    status="backed_up",
                    backup_path=str(backup_path),
                    backup_sha256=_sha256(backup_path),
                )
            else:
                row = manifest.update(symbol, status="backed_up")

        manifest.update(symbol, status="committing")
        _copy_file_atomic(stage_path, canonical)
        canonical_sha256 = _sha256(canonical)
        if canonical_sha256 != row["source_stage_sha256"]:
            raise KlinePromotionError(f"canonical 替换后摘要不一致: {symbol}")
        manifest.update(
            symbol,
            status="committed",
            canonical_sha256=canonical_sha256,
            error_type="",
            error_message="",
        )
    except Exception as exc:
        if _file_hash_or_none(canonical) == row["source_stage_sha256"]:
            try:
                _assert_backup_state(row)
            except Exception:
                _mark_promotion_failure(manifest, row, canonical, exc)
                raise
            manifest.update(
                symbol,
                status="committed",
                canonical_sha256=row["source_stage_sha256"],
                error_type="",
                error_message="",
            )
            return
        _mark_promotion_failure(manifest, row, canonical, exc)
        raise


def _promotion_result(
    promotion_run_id: str,
    staging_run_id: str,
    manifest: PromotionManifest,
    *,
    status: str,
    validated_symbols: tuple[str, ...] = (),
    dry_run: bool = False,
) -> PromotionResult:
    return PromotionResult(
        promotion_run_id=promotion_run_id,
        staging_run_id=staging_run_id,
        promoted_symbols=tuple(
            row["symbol"] for row in manifest.rows if row["status"] == "committed"
        ),
        failed_symbols=tuple(
            row["symbol"]
            for row in manifest.rows
            if row["status"] in {"failed", "rollback-required"}
        ),
        validated_symbols=validated_symbols,
        status=status,
        dry_run=dry_run,
    )


def _validate_committed_rows(manifest: PromotionManifest) -> None:
    for row in manifest.rows:
        if row["status"] != "committed":
            continue
        _assert_backup_state(row)
        if _file_hash_or_none(Path(row["canonical_path"])) != row["canonical_sha256"]:
            raise KlinePromotionError(f"已提交 canonical 摘要不一致: {row['symbol']}")


def _execute_promotion_locked(
    run_dir: Path,
    metadata: dict[str, Any],
    manifest: PromotionManifest,
    symbols: set[str] | None = None,
) -> PromotionResult:
    _write_promotion_metadata(run_dir, metadata, status="committing")
    for row in manifest.rows:
        if symbols is not None and row["symbol"] not in symbols:
            continue
        if row["status"] == "committed":
            continue
        try:
            _promote_row(manifest, row, run_dir)
        except Exception:
            _write_promotion_metadata(run_dir, metadata, status="failed")
            break
    manifest.compact()
    if all(row["status"] == "committed" for row in manifest.rows):
        metadata = _write_promotion_metadata(run_dir, metadata, status="completed")
    elif any(row["status"] in {"failed", "rollback-required"} for row in manifest.rows):
        metadata = _write_promotion_metadata(run_dir, metadata, status="failed")
    else:
        metadata = _write_promotion_metadata(run_dir, metadata, status="partial")
    return _promotion_result(
        run_dir.name,
        metadata["staging_run_id"],
        manifest,
        status=metadata["status"],
    )


def _assert_no_overlapping_promotion(staging_run_id: str, promotion_root: Path) -> None:
    if not promotion_root.exists():
        return
    for candidate in promotion_root.iterdir():
        if candidate.name.startswith("."):
            continue
        if not candidate.is_dir() or candidate.is_symlink():
            continue
        metadata_path = candidate / PROMOTION_METADATA_FILENAME
        if not metadata_path.exists() or metadata_path.is_symlink():
            raise KlinePromotionError(f"发现不完整 promotion run: {candidate}")
        metadata = _read_json(metadata_path)
        if metadata.get("staging_run_id") == staging_run_id:
            raise KlinePromotionError(
                f"staging run 已存在 promotion，请使用 --resume: {candidate.name}"
            )


def _build_promotion_run(
    promotion_root: Path,
    promotion_run_id: str,
    metadata: dict[str, Any],
    rows: list[dict[str, str]],
) -> tuple[Path, PromotionManifest]:
    run_dir = _promotion_path(promotion_root, promotion_run_id)
    temp_dir = promotion_root / f".{promotion_run_id}.{uuid.uuid4().hex}.tmp"
    temp_dir.mkdir(parents=True, exist_ok=False)
    try:
        _validate_metadata(metadata, promotion_run_id)
        _write_json(temp_dir / PROMOTION_METADATA_FILENAME, metadata)
        manifest = PromotionManifest.create(
            temp_dir / PROMOTION_MANIFEST_FILENAME,
            rows,
        )
        promotion_root_fd = _open_directory_no_follow(promotion_root)
        try:
            os.rename(
                temp_dir.name,
                run_dir.name,
                src_dir_fd=promotion_root_fd,
                dst_dir_fd=promotion_root_fd,
            )
            os.fsync(promotion_root_fd)
        finally:
            os.close(promotion_root_fd)
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    manifest.path = run_dir / PROMOTION_MANIFEST_FILENAME
    manifest.state_dir = run_dir / "state"
    return run_dir, manifest


def _preflight_promotion_targets(
    staging_run_id: str,
    selected: list[dict[str, str]],
    warehouse_dir: Path,
    promotion_root: Path,
) -> None:
    _assert_no_overlapping_promotion(staging_run_id, promotion_root)
    estimated_bytes = MIN_PROMOTION_FREE_BYTES
    for row in selected:
        canonical = _safe_canonical_path(
            warehouse_dir, row["symbol"], create_dirs=False
        )
        if canonical.is_symlink():
            raise KlinePromotionError(f"canonical 文件不得是符号链接: {row['symbol']}")
        stage_path = Path(row["stage_path"])
        if not stage_path.is_file() or stage_path.is_symlink():
            raise KlinePromotionError(f"staging 文件不可用于晋级: {row['symbol']}")
        estimated_bytes += stage_path.stat().st_size
        if canonical.exists():
            if canonical.is_symlink():
                raise KlinePromotionError(
                    f"canonical 文件不得是符号链接: {row['symbol']}"
                )
            estimated_bytes += canonical.stat().st_size
    if shutil.disk_usage(warehouse_dir).free < estimated_bytes:
        raise KlinePromotionError("磁盘剩余空间不足以保留 backup 和临时文件")


def _preflight_resume_space(manifest: PromotionManifest, warehouse_dir: Path) -> None:
    estimated_bytes = MIN_PROMOTION_FREE_BYTES
    for row in manifest.rows:
        if row["status"] in {"committed", "rolled_back"}:
            continue
        stage_path = Path(row["source_stage_path"])
        if not stage_path.is_file() or stage_path.is_symlink():
            raise KlinePromotionError(f"resume staging 文件不可用: {row['symbol']}")
        estimated_bytes += stage_path.stat().st_size
        if row["canonical_existed"] == "1" and not row["backup_sha256"]:
            canonical = Path(row["canonical_path"])
            if canonical.is_file() and not canonical.is_symlink():
                estimated_bytes += canonical.stat().st_size
    if shutil.disk_usage(warehouse_dir).free < estimated_bytes:
        raise KlinePromotionError("resume 磁盘剩余空间不足以保留 backup 和临时文件")


def promote_kline_staging(
    staging_run_id: str,
    *,
    symbol: str | None = None,
    dry_run: bool = False,
    warehouse_dir: Path | str | None = None,
    recover_stale_lock: bool = False,
) -> PromotionResult:
    warehouse_path = (
        Path(warehouse_dir) if warehouse_dir is not None else Path(WAREHOUSE_DIR)
    )
    promotion_root = get_promotion_root(warehouse_path)
    execution_symbols = {symbol} if symbol is not None else None
    with CanonicalWriteLock(
        warehouse_path,
        operation="promote-kline-staging",
        run_id=staging_run_id,
    ):
        with staging.MigrationLock(
            staging.get_migration_root(warehouse_path),
            staging_run_id,
            recover_stale=recover_stale_lock,
        ):
            staging_dir, _staging_metadata, manifest, selected = _load_staging_targets(
                staging_run_id, warehouse_path
            )
            if symbol is not None and symbol not in {row["symbol"] for row in selected}:
                raise KlinePromotionError(f"staging run 不存在股票: {symbol}")
            _preflight_promotion_targets(
                staging_run_id,
                selected,
                warehouse_path,
                promotion_root,
            )
            if dry_run:
                return PromotionResult(
                    promotion_run_id="",
                    staging_run_id=staging_run_id,
                    promoted_symbols=(),
                    failed_symbols=(),
                    validated_symbols=tuple(
                        row["symbol"]
                        for row in selected
                        if execution_symbols is None
                        or row["symbol"] in execution_symbols
                    ),
                    status="validated",
                    dry_run=True,
                )
            promotion_root.mkdir(parents=True, exist_ok=True)
            promotion_run_id = _new_promotion_run_id()
            evidence = _target_evidence(row["symbol"] for row in selected)
            metadata = {
                "promotion_run_id": promotion_run_id,
                "promotion_schema_version": PROMOTION_SCHEMA_VERSION,
                "staging_run_id": staging_run_id,
                **evidence,
                "status": "pending",
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
            rows = [
                _new_promotion_row(row, warehouse_path, staging_dir) for row in selected
            ]
            run_dir, promotion_manifest = _build_promotion_run(
                promotion_root,
                promotion_run_id,
                metadata,
                rows,
            )
            metadata = _write_promotion_metadata(run_dir, metadata, status="validated")
            return _execute_promotion_locked(
                run_dir,
                metadata,
                promotion_manifest,
                execution_symbols,
            )


def resume_kline_promotion(
    promotion_run_id: str,
    *,
    warehouse_dir: Path | str | None = None,
    recover_stale_lock: bool = False,
) -> PromotionResult:
    warehouse_path = (
        Path(warehouse_dir) if warehouse_dir is not None else Path(WAREHOUSE_DIR)
    )
    with CanonicalWriteLock(
        warehouse_path,
        operation="resume-kline-promotion",
        run_id=promotion_run_id,
    ):
        run_dir, metadata, manifest = _load_promotion_run(
            promotion_run_id, warehouse_path
        )
        if metadata["status"] == "completed":
            _validate_committed_rows(manifest)
            return _promotion_result(
                promotion_run_id,
                metadata["staging_run_id"],
                manifest,
                status="completed",
            )
        if metadata["status"] == "rolled_back":
            raise KlinePromotionError("rolled_back promotion run 不允许 resume")
        with staging.MigrationLock(
            staging.get_migration_root(warehouse_path),
            metadata["staging_run_id"],
            recover_stale=recover_stale_lock,
        ):
            run_dir, metadata, manifest = _load_promotion_run(
                promotion_run_id, warehouse_path
            )
            if metadata["status"] == "rolled_back":
                raise KlinePromotionError("rolled_back promotion run 不允许 resume")
            _validate_committed_rows(manifest)
            source_run_dir, _source_metadata, _source_manifest, selected = (
                _load_staging_targets(metadata["staging_run_id"], warehouse_path)
            )
            selected_by_symbol = {row["symbol"]: row for row in selected}
            if set(selected_by_symbol) != {row["symbol"] for row in manifest.rows}:
                raise KlinePromotionError("晋级 manifest 与 staging 目标股票不一致")
            for row in manifest.rows:
                source_row = selected_by_symbol[row["symbol"]]
                expected_stage = staging._safe_stage_path(
                    source_run_dir, source_row["stage_path"], row["symbol"]
                )
                if (
                    Path(row["source_stage_path"]).resolve() != expected_stage
                    or row["source_stage_sha256"] != source_row["stage_sha256"]
                ):
                    raise KlinePromotionError(
                        f"晋级 staged 证据不一致: {row['symbol']}"
                    )
            _preflight_resume_space(manifest, warehouse_path)
            return _execute_promotion_locked(
                run_dir,
                metadata,
                manifest,
            )


def _rollback_row(
    manifest: PromotionManifest, row: dict[str, str], run_dir: Path
) -> None:
    symbol = row["symbol"]
    canonical = Path(row["canonical_path"])
    rollback_temp = _safe_promotion_file(
        run_dir,
        Path("rollback-temp") / f"symbol={symbol}" / "data.parquet",
    )
    current_hash = _file_hash_or_none(canonical)
    original_hash = row["original_canonical_sha256"] or None
    target_hash = row["canonical_sha256"] or row["source_stage_sha256"]
    _assert_backup_state(row)
    if current_hash not in {None, original_hash, target_hash}:
        raise KlinePromotionError(
            f"canonical 当前摘要未知，可能已被后续写入，拒绝回滚: {symbol}"
        )

    if rollback_temp.exists():
        if _file_hash_or_none(rollback_temp) != target_hash:
            raise KlinePromotionError(f"rollback-temp 摘要不一致: {symbol}")
    if current_hash == target_hash and not rollback_temp.exists():
        _copy_file_atomic(canonical, rollback_temp)
        if _file_hash_or_none(rollback_temp) != target_hash:
            raise KlinePromotionError(f"rollback-temp 写入校验失败: {symbol}")

    if row["canonical_existed"] == "1":
        if current_hash != original_hash:
            _restore_backup(row)
    elif current_hash == target_hash:
        _restore_backup(row)

    if row["canonical_existed"] == "1":
        _read_and_validate_canonical(canonical, symbol)
    elif canonical.exists():
        raise KlinePromotionError(f"回滚后新建 canonical 仍存在: {symbol}")
    if rollback_temp.exists():
        _unlink_file_no_follow(rollback_temp)
    manifest.update(symbol, status="rolled_back")


def rollback_kline_promotion(
    promotion_run_id: str,
    *,
    warehouse_dir: Path | str | None = None,
) -> PromotionResult:
    warehouse_path = (
        Path(warehouse_dir) if warehouse_dir is not None else Path(WAREHOUSE_DIR)
    )
    with CanonicalWriteLock(
        warehouse_path,
        operation="rollback-kline-promotion",
        run_id=promotion_run_id,
    ):
        run_dir, metadata, manifest = _load_promotion_run(
            promotion_run_id, warehouse_path
        )
        if metadata["status"] == "rolled_back":
            return _promotion_result(
                promotion_run_id,
                metadata["staging_run_id"],
                manifest,
                status="rolled_back",
            )
        for row in manifest.rows:
            if row["status"] in {"rolled_back", "pending", "failed"}:
                continue
            if row["status"] not in {"committed", "rollback-required"}:
                raise KlinePromotionError(
                    f"股票状态不允许回滚: {row['symbol']}: {row['status']}"
                )
            try:
                _rollback_row(manifest, row, run_dir)
            except Exception as exc:
                manifest.update(
                    row["symbol"],
                    status="rollback-required",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                _write_promotion_metadata(run_dir, metadata, status="failed")
                raise
        manifest.compact()
        metadata = _write_promotion_metadata(run_dir, metadata, status="rolled_back")
    return _promotion_result(
        promotion_run_id,
        metadata["staging_run_id"],
        manifest,
        status="rolled_back",
    )
