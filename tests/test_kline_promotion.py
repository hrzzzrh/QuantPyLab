from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest

from tools import kline_promotion as promotion_mod
from tools import kline_source_migration as staging_mod
from utils.canonical_write_lock import CanonicalWriteLock, CanonicalWriteLockError


def make_stage_frame(factor: float = 2.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2026-08-03", "2026-08-04"],
            "open": [9.5, 10.5],
            "high": [10.5, 11.5],
            "low": [9.0, 10.0],
            "close": [10.0, 11.0],
            "volume": [100.0, 200.0],
            "amount": [1000.0, 2200.0],
            "adj_factor": [factor, factor],
        }
    )


def create_staging_run(
    warehouse_dir: Path,
    symbols: tuple[str, ...] = ("600009",),
) -> tuple[Path, dict[str, pd.DataFrame]]:
    root = staging_mod.get_migration_root(warehouse_dir)
    targets = [
        staging_mod.MigrationTarget(symbol, f"name-{symbol}") for symbol in symbols
    ]
    _, run_dir, manifest = staging_mod._build_run(
        root,
        "staging-run",
        targets,
        "sina-klc",
        "20100101",
        "20260807",
        False,
    )
    frames = {}
    for target in targets:
        frame = make_stage_frame()
        frames[target.symbol] = frame
        stage_path = run_dir / "staged" / f"symbol={target.symbol}" / "data.parquet"
        staging_mod._write_staged_frame(frame, stage_path)
        metrics = staging_mod._quality_metrics(frame)
        manifest.update(
            target.symbol,
            status="staged",
            source_used="sina-klc",
            stage_path=str(stage_path),
            **metrics,
            new_start_date="2026-08-03",
            new_end_date="2026-08-04",
            stage_sha256=staging_mod._sha256(stage_path),
        )
    return run_dir, frames


def canonical_path(warehouse_dir: Path, symbol: str) -> Path:
    return warehouse_dir / "daily_kline" / f"symbol={symbol}" / "data.parquet"


def test_promotion_replaces_canonical_and_keeps_backup(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    _, frames = create_staging_run(warehouse_dir)
    old = make_stage_frame(factor=1.0)
    old_path = canonical_path(warehouse_dir, "600009")
    old_path.parent.mkdir(parents=True)
    old.to_parquet(old_path, index=False)
    old_bytes = old_path.read_bytes()

    result = promotion_mod.promote_kline_staging(
        "staging-run", warehouse_dir=warehouse_dir
    )

    assert len(result.promoted_symbols) == 1
    assert result.failed_symbols == ()
    assert (
        old_path.read_bytes()
        == (
            warehouse_dir
            / ".migrations"
            / "kline-source"
            / "staging-run"
            / "staged"
            / "symbol=600009"
            / "data.parquet"
        ).read_bytes()
    )
    promotion_dir = (
        warehouse_dir / ".migrations" / "kline-promotion" / result.promotion_run_id
    )
    backup_path = promotion_dir / "backup" / "symbol=600009" / "data.parquet"
    assert backup_path.read_bytes() == old_bytes
    pd.testing.assert_frame_equal(pd.read_parquet(old_path), frames["600009"])


def test_promotion_dry_run_does_not_write_canonical(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    create_staging_run(warehouse_dir)

    result = promotion_mod.promote_kline_staging(
        "staging-run", dry_run=True, warehouse_dir=warehouse_dir
    )

    assert result.dry_run is True
    assert result.promotion_run_id == ""
    assert not canonical_path(warehouse_dir, "600009").exists()
    assert not (warehouse_dir / "daily_kline").exists()


def test_promotion_supports_single_symbol_gray_release(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    create_staging_run(warehouse_dir, symbols=("600009", "600010"))

    result = promotion_mod.promote_kline_staging(
        "staging-run", symbol="600009", warehouse_dir=warehouse_dir
    )

    assert result.promoted_symbols == ("600009",)
    assert canonical_path(warehouse_dir, "600009").exists()
    assert not canonical_path(warehouse_dir, "600010").exists()

    resumed = promotion_mod.resume_kline_promotion(
        result.promotion_run_id, warehouse_dir=warehouse_dir
    )
    assert set(resumed.promoted_symbols) == {"600009", "600010"}
    assert canonical_path(warehouse_dir, "600010").exists()


def test_promotion_resume_completed_run_is_idempotent(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    create_staging_run(warehouse_dir)
    first = promotion_mod.promote_kline_staging(
        "staging-run", warehouse_dir=warehouse_dir
    )
    second = promotion_mod.resume_kline_promotion(
        first.promotion_run_id, warehouse_dir=warehouse_dir
    )

    assert second.promoted_symbols == first.promoted_symbols
    assert second.failed_symbols == ()


def test_resume_partial_run_revalidates_committed_canonical(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    create_staging_run(warehouse_dir, symbols=("600009", "600010"))
    promotion = promotion_mod.promote_kline_staging(
        "staging-run", symbol="600009", warehouse_dir=warehouse_dir
    )
    make_stage_frame(factor=3.0).to_parquet(
        canonical_path(warehouse_dir, "600009"), index=False
    )

    with pytest.raises(promotion_mod.KlinePromotionError, match="摘要不一致"):
        promotion_mod.resume_kline_promotion(
            promotion.promotion_run_id, warehouse_dir=warehouse_dir
        )


def test_rollback_restores_existing_canonical(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    create_staging_run(warehouse_dir)
    old = make_stage_frame(factor=1.0)
    old_path = canonical_path(warehouse_dir, "600009")
    old_path.parent.mkdir(parents=True)
    old.to_parquet(old_path, index=False)

    promotion = promotion_mod.promote_kline_staging(
        "staging-run", warehouse_dir=warehouse_dir
    )
    result = promotion_mod.rollback_kline_promotion(
        promotion.promotion_run_id, warehouse_dir=warehouse_dir
    )

    assert result.promoted_symbols == ()
    pd.testing.assert_frame_equal(pd.read_parquet(old_path), old)


def test_rollback_removes_new_canonical_when_no_old_file(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    create_staging_run(warehouse_dir)
    promotion = promotion_mod.promote_kline_staging(
        "staging-run", warehouse_dir=warehouse_dir
    )

    promotion_mod.rollback_kline_promotion(
        promotion.promotion_run_id, warehouse_dir=warehouse_dir
    )

    assert not canonical_path(warehouse_dir, "600009").exists()


def test_resume_rejects_changed_committed_canonical(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    create_staging_run(warehouse_dir)
    promotion = promotion_mod.promote_kline_staging(
        "staging-run", warehouse_dir=warehouse_dir
    )
    changed = make_stage_frame(factor=3.0)
    changed.to_parquet(canonical_path(warehouse_dir, "600009"), index=False)

    with pytest.raises(promotion_mod.KlinePromotionError, match="摘要不一致"):
        promotion_mod.resume_kline_promotion(
            promotion.promotion_run_id, warehouse_dir=warehouse_dir
        )


def test_rollback_rejects_changed_committed_canonical(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    create_staging_run(warehouse_dir)
    promotion = promotion_mod.promote_kline_staging(
        "staging-run", warehouse_dir=warehouse_dir
    )
    changed = make_stage_frame(factor=3.0)
    changed.to_parquet(canonical_path(warehouse_dir, "600009"), index=False)

    with pytest.raises(promotion_mod.KlinePromotionError, match="后续写入"):
        promotion_mod.rollback_kline_promotion(
            promotion.promotion_run_id, warehouse_dir=warehouse_dir
        )


def test_rollback_recovers_rollback_required_with_temp_file(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    _, frames = create_staging_run(warehouse_dir)
    old = make_stage_frame(factor=1.0)
    old_path = canonical_path(warehouse_dir, "600009")
    old_path.parent.mkdir(parents=True)
    old.to_parquet(old_path, index=False)

    promotion = promotion_mod.promote_kline_staging(
        "staging-run", warehouse_dir=warehouse_dir
    )
    run_dir = (
        warehouse_dir / ".migrations" / "kline-promotion" / promotion.promotion_run_id
    )
    manifest = promotion_mod.PromotionManifest.load(
        run_dir / promotion_mod.PROMOTION_MANIFEST_FILENAME
    )
    manifest.update("600009", status="rollback-required")
    metadata = promotion_mod._read_json(
        run_dir / promotion_mod.PROMOTION_METADATA_FILENAME
    )
    promotion_mod._write_promotion_metadata(run_dir, metadata, status="failed")
    rollback_temp = run_dir / "rollback-temp" / "symbol=600009" / "data.parquet"
    rollback_temp.parent.mkdir(parents=True)
    shutil.copyfile(old_path, rollback_temp)
    old_path.unlink()

    result = promotion_mod.rollback_kline_promotion(
        promotion.promotion_run_id, warehouse_dir=warehouse_dir
    )

    assert result.status == "rolled_back"
    pd.testing.assert_frame_equal(pd.read_parquet(old_path), old)
    assert not rollback_temp.exists()
    assert frames["600009"].shape == (2, 8)


def test_promotion_rejects_tampered_quality_evidence(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    run_dir, _ = create_staging_run(warehouse_dir)
    manifest_path = run_dir / staging_mod.MANIFEST_FILENAME
    manifest = staging_mod.MigrationManifest.load(manifest_path)
    manifest.update("600009", new_rows="999")

    with pytest.raises(promotion_mod.KlinePromotionError, match="质量证据不一致"):
        promotion_mod.promote_kline_staging("staging-run", warehouse_dir=warehouse_dir)
    assert not canonical_path(warehouse_dir, "600009").exists()


def test_resume_rejects_committing_without_backup_evidence(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    create_staging_run(warehouse_dir)
    old_path = canonical_path(warehouse_dir, "600009")
    old_path.parent.mkdir(parents=True)
    make_stage_frame(factor=1.0).to_parquet(old_path, index=False)
    promotion = promotion_mod.promote_kline_staging(
        "staging-run", warehouse_dir=warehouse_dir
    )
    run_dir = (
        warehouse_dir / ".migrations" / "kline-promotion" / promotion.promotion_run_id
    )
    manifest = promotion_mod.PromotionManifest.load(
        run_dir / promotion_mod.PROMOTION_MANIFEST_FILENAME
    )
    manifest.update("600009", status="committing", backup_sha256="")

    with pytest.raises(promotion_mod.KlinePromotionError, match="backup 摘要"):
        promotion_mod.resume_kline_promotion(
            promotion.promotion_run_id, warehouse_dir=warehouse_dir
        )


def test_resume_rejects_backup_not_matching_original_canonical(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    create_staging_run(warehouse_dir)
    old_path = canonical_path(warehouse_dir, "600009")
    old_path.parent.mkdir(parents=True)
    make_stage_frame(factor=1.0).to_parquet(old_path, index=False)
    promotion = promotion_mod.promote_kline_staging(
        "staging-run", warehouse_dir=warehouse_dir
    )
    run_dir = (
        warehouse_dir / ".migrations" / "kline-promotion" / promotion.promotion_run_id
    )
    manifest = promotion_mod.PromotionManifest.load(
        run_dir / promotion_mod.PROMOTION_MANIFEST_FILENAME
    )
    manifest.update("600009", backup_sha256=promotion_mod._sha256(old_path))

    with pytest.raises(promotion_mod.KlinePromotionError, match="backup 与旧"):
        promotion_mod.resume_kline_promotion(
            promotion.promotion_run_id, warehouse_dir=warehouse_dir
        )


def test_resume_removes_orphan_state_temp_file(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    create_staging_run(warehouse_dir)
    promotion = promotion_mod.promote_kline_staging(
        "staging-run", warehouse_dir=warehouse_dir
    )
    state_dir = (
        warehouse_dir
        / ".migrations"
        / "kline-promotion"
        / promotion.promotion_run_id
        / "state"
    )
    orphan = state_dir / ".symbol=600009.json.0123456789abcdef0123456789abcdef.tmp"
    orphan.write_bytes(b"partial")

    promotion_mod.resume_kline_promotion(
        promotion.promotion_run_id, warehouse_dir=warehouse_dir
    )

    assert not orphan.exists()


def test_promotion_rejects_symlinked_canonical_directory(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    create_staging_run(warehouse_dir)
    outside = tmp_path / "outside"
    outside.mkdir()
    warehouse_dir.mkdir(exist_ok=True)
    (warehouse_dir / "daily_kline").symlink_to(outside, target_is_directory=True)

    with pytest.raises(promotion_mod.KlinePromotionError, match="符号链接"):
        promotion_mod.promote_kline_staging(
            "staging-run", dry_run=True, warehouse_dir=warehouse_dir
        )


def test_promotion_rejects_overlapping_staging_run(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    create_staging_run(warehouse_dir)
    promotion_mod.promote_kline_staging(
        "staging-run", symbol="600009", warehouse_dir=warehouse_dir
    )

    with pytest.raises(promotion_mod.KlinePromotionError, match="已存在 promotion"):
        promotion_mod.promote_kline_staging("staging-run", warehouse_dir=warehouse_dir)


def test_promotion_rejects_tampered_staging_before_writing(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    run_dir, _ = create_staging_run(warehouse_dir)
    stage_path = run_dir / "staged" / "symbol=600009" / "data.parquet"
    stage_path.write_bytes(stage_path.read_bytes() + b"tampered")

    with pytest.raises(promotion_mod.KlinePromotionError, match="摘要变化"):
        promotion_mod.promote_kline_staging("staging-run", warehouse_dir=warehouse_dir)
    assert not canonical_path(warehouse_dir, "600009").exists()


def test_promotion_respects_shared_canonical_write_lock(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    create_staging_run(warehouse_dir)

    with CanonicalWriteLock(warehouse_dir, operation="test-holder"):
        with pytest.raises(CanonicalWriteLockError, match="已被占用"):
            promotion_mod.promote_kline_staging(
                "staging-run", warehouse_dir=warehouse_dir
            )

    assert not canonical_path(warehouse_dir, "600009").exists()


def test_promotion_respects_active_staging_lock(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    create_staging_run(warehouse_dir)
    migration_root = staging_mod.get_migration_root(warehouse_dir)

    with staging_mod.MigrationLock(migration_root, "active-staging"):
        with pytest.raises(staging_mod.MigrationLockError, match="已有迁移正在运行"):
            promotion_mod.promote_kline_staging(
                "staging-run", warehouse_dir=warehouse_dir
            )


def test_promotion_can_recover_stale_staging_lock_explicitly(tmp_path):
    warehouse_dir = tmp_path / "warehouse"
    create_staging_run(warehouse_dir)
    migration_root = staging_mod.get_migration_root(warehouse_dir)
    migration_root.mkdir(parents=True, exist_ok=True)
    (migration_root / staging_mod.LOCK_FILENAME).write_text(
        "run_id=stale\npid=2147483647\n", encoding="ascii"
    )

    result = promotion_mod.promote_kline_staging(
        "staging-run",
        warehouse_dir=warehouse_dir,
        recover_stale_lock=True,
    )

    assert result.promoted_symbols == ("600009",)
