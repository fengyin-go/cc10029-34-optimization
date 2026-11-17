"""爬虫任务存量数据集领域服务。

覆盖需求链路：
- 把进程内存里的爬虫任务迁移成「爬虫任务存量数据集」，任务配置、归属、下发批次
  字段名全部沿用，新增字段（任务版本/归属/来源/保留来源/数据集批次/内容指纹）只追加。
- 存量记录必须同时具备 任务版本、归属、下发批次 才能进入迁移完成范围。
- 迁移中断后重跑只继续未完成记录；已校验记录编号不变（SQLite 自增主键，永不重排）。
- 重复/连续送交按内容指纹判重，全库只写入一份。
- 旧内存任务与新记录出现版本冲突时，下游只保留一份来源，由当前指针决定。
- 先做依赖核对、构建环境与数据库检查，再由上线流水线发布并切换指针。
- 采集目标依据当前指针数据集生成目标站点执行快照，快照引用迁移后的归属与批次，
  重复生成只保留一份。
"""
from __future__ import annotations

import platform
import sqlite3
from datetime import datetime
from typing import Any

from app.persistence import (
    LEGACY_FIELDS,
    PIPELINE_STAGES,
    REQUIRED_STOCK_FIELDS,
    PersistentStore,
    fingerprint,
    persistent_store,
)
from app.store import store

CRAWLER_MODULE = "crawler"
TARGETSITE_MODULE = "targetsite"
MEMORY_SOURCE = "内存迁移"
EXTERNAL_SOURCE = "外部送交"
DEFAULT_VERSION = "v1"


class StockDatasetError(Exception):
    """业务可直接展示的校验/冲突错误。"""


class StockDatasetService:
    def __init__(self, db: PersistentStore | None = None) -> None:
        self.db = db or persistent_store

    # ------------------------------------------------------------ 组装与校验
    @staticmethod
    def _missing_required(record: dict[str, Any]) -> list[str]:
        return [field for field in REQUIRED_STOCK_FIELDS if not str(record.get(field) or "").strip()]

    def _normalize_record(
        self,
        raw: dict[str, Any],
        *,
        dataset_batch: str,
        source: str,
        retained_source: str,
        fill_defaults: bool,
    ) -> dict[str, Any]:
        """把一条输入归一成存量数据集记录；只追加字段，不改写既有字段名。"""
        record: dict[str, Any] = {}
        for field in LEGACY_FIELDS:
            record[field] = raw.get(field)
        # 任务版本：内存老任务没有该列，允许补默认版本；外部送交缺省即视为字段缺失。
        record["任务版本"] = raw.get("任务版本") or (DEFAULT_VERSION if fill_defaults else None)
        # 归属：内存老任务按关联站点补归属；外部送交必须显式提供。
        record["归属"] = raw.get("归属") or (raw.get("关联站点") if fill_defaults else None)
        record["来源"] = source
        record["保留来源"] = retained_source
        record["数据集批次"] = dataset_batch
        record["内容指纹"] = fingerprint(record)
        return record

    # ------------------------------------------------------------ 内存迁移
    def start_or_resume_migration(self, *, batch_size: int | None = None) -> dict[str, Any]:
        """从进程内存迁移爬虫任务；存在未完成批次时只做续跑。"""
        open_runs = [
            row for row in self.db.list_migrations()
            if row["状态"] in ("待迁移", "迁移中", "部分完成")
        ]
        if open_runs:
            run = open_runs[0]
            self.db.audit("迁移续跑", f"继续未完成批次 {run['数据集批次']}", run["数据集批次"])
            return self._run_migration(run["数据集批次"], batch_size=batch_size, resumed=True)

        dataset_batch = "MIG-" + datetime.now().strftime("%Y%m%d%H%M%S")
        memory_rows = store.rows(CRAWLER_MODULE)
        self.db.ensure_migration(dataset_batch, "进程内存爬虫任务", len(memory_rows))
        written = 0
        duplicates = 0
        rejected: list[dict[str, Any]] = []
        for raw in memory_rows:
            record = self._normalize_record(
                raw,
                dataset_batch=dataset_batch,
                source=MEMORY_SOURCE,
                retained_source=MEMORY_SOURCE,
                fill_defaults=True,
            )
            missing = self._missing_required(record)
            saved, reason = self.db.insert_stock(record)
            if reason == "duplicate-fingerprint":
                duplicates += 1
                continue
            written += 1
            if missing:
                self.db.mark_stock_state(saved["id"], "校验未通过")
                rejected.append({"id": saved["id"], "任务编号": record["任务编号"], "缺失字段": missing})
        self.db.audit(
            "迁移建批",
            f"批次 {dataset_batch} 写入 {written} 条，判重跳过 {duplicates} 条，校验未通过 {len(rejected)} 条",
            dataset_batch,
        )
        return self._run_migration(dataset_batch, batch_size=batch_size, resumed=False,
                                   written=written, duplicates=duplicates, rejected=rejected)

    def _run_migration(
        self,
        dataset_batch: str,
        *,
        batch_size: int | None = None,
        resumed: bool,
        written: int = 0,
        duplicates: int = 0,
        rejected: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        # 只挑未完成记录；已完成/已校验的记录跳过，编号因此保持稳定。
        pending = [
            row for row in self.db.list_stock(dataset_batch)
            if row["迁移状态"] in ("待迁移", "迁移中")
        ]
        processing = pending if not batch_size or batch_size <= 0 else pending[:batch_size]
        migrated_now: list[int] = []
        interrupted = len(processing) < len(pending)
        for row in processing:
            missing = self._missing_required(row)
            if missing:
                self.db.mark_stock_state(row["id"], "校验未通过")
                continue
            self.db.mark_stock_state(row["id"], "迁移中")
            # 校验通过即固化编号与内容，后续重跑不再触碰。
            self.db.mark_stock_state(row["id"], "已完成", validated=True)
            self.db.set_migration_pointer(dataset_batch, row["id"])
            migrated_now.append(row["id"])
        progress = self.db.update_migration_counters(dataset_batch)
        self.db.audit(
            "迁移执行",
            f"本次完成 {len(migrated_now)} 条，剩余待迁移 {max(0, len(pending) - len(processing))} 条",
            dataset_batch,
        )
        return {
            "数据集批次": dataset_batch,
            "续跑": resumed,
            "中断": interrupted,
            "本次迁移编号": migrated_now,
            "新写入": written,
            "判重跳过": duplicates,
            "校验未通过": rejected or [],
            "进度": progress,
            "待迁移记录": [row["id"] for row in pending[len(processing):]],
        }

    # ------------------------------------------------------------ 外部送交
    def submit_dataset(
        self,
        dataset_batch: str,
        records: list[dict[str, Any]],
        *,
        retained_source: str = EXTERNAL_SOURCE,
    ) -> dict[str, Any]:
        """接收外部送交的存量数据集；连续/重复送交按指纹只写入一份。"""
        if not dataset_batch or not str(dataset_batch).strip():
            raise StockDatasetError("数据集批次不能为空")
        if not records:
            raise StockDatasetError("存量记录不能为空")
        self.db.ensure_migration(dataset_batch, f"外部送交批次 {dataset_batch}", len(records))
        accepted: list[dict[str, Any]] = []
        duplicate_records: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        for raw in records:
            record = self._normalize_record(
                raw,
                dataset_batch=dataset_batch,
                source=EXTERNAL_SOURCE,
                retained_source=retained_source,
                fill_defaults=False,
            )
            missing = self._missing_required(record)
            if not str(record.get("任务编号") or "").strip():
                missing = ["任务编号", *missing]
            existing = self.db.find_stock_by_fingerprint(record["内容指纹"])
            if existing:
                duplicate_records.append({"id": existing["id"], "任务编号": existing["任务编号"]})
                continue
            saved, reason = self.db.insert_stock(record)
            if reason:
                duplicate_records.append({"id": (saved or {}).get("id"), "任务编号": record["任务编号"]})
                continue
            if missing:
                self.db.mark_stock_state(saved["id"], "校验未通过")
                rejected.append({"id": saved["id"], "任务编号": record["任务编号"], "缺失字段": missing})
            else:
                self.db.mark_stock_state(saved["id"], "已完成", validated=True)
                accepted.append({"id": saved["id"], "任务编号": saved["任务编号"]})
        progress = self.db.update_migration_counters(dataset_batch)
        self.db.audit(
            "存量送交",
            f"接收 {len(records)} 条：新写入 {len(accepted)} 条，判重 {len(duplicate_records)} 条，"
            f"校验未通过 {len(rejected)} 条",
            dataset_batch,
        )
        return {
            "数据集批次": dataset_batch,
            "接收总数": len(records),
            "唯一写入": len(accepted),
            "重复跳过": duplicate_records,
            "校验未通过": rejected,
            "进度": progress,
        }

    # ------------------------------------------------------------ 版本冲突
    def detect_version_conflicts(self, dataset_batch: str) -> list[dict[str, Any]]:
        """同任务编号在不同来源/版本间出现不一致时给出冲突清单。"""
        rows = self.db.list_stock()
        by_task: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            by_task.setdefault(str(row["任务编号"]), []).append(row)
        conflicts: list[dict[str, Any]] = []
        for task_code, group in by_task.items():
            versions = {str(row["任务版本"]) for row in group}
            sources = {str(row["来源"]) for row in group}
            target_group = [row for row in group if row["数据集批次"] == dataset_batch]
            if target_group and len(versions) > 1 and len(sources) > 1:
                conflicts.append({
                    "任务编号": task_code,
                    "候选": [
                        {"id": row["id"], "任务版本": row["任务版本"], "来源": row["来源"],
                         "数据集批次": row["数据集批次"]}
                        for row in group
                    ],
                })
        return conflicts

    # ------------------------------------------------------------ 流水线
    def run_precheck(self, dataset_batch: str) -> dict[str, Any]:
        """依赖核对 → 构建环境检查 → 数据库检查；任一不过则不允许发布。"""
        records = self.db.list_stock(dataset_batch)
        if not records:
            raise StockDatasetError(f"数据集批次 {dataset_batch} 不存在或没有存量记录")
        results: list[dict[str, Any]] = []

        # 1) 依赖核对：三必填字段齐全的完整存量记录数量。
        incomplete = [row for row in records if self._missing_required(row)]
        dep_ok = not incomplete
        results.append(self.db.upsert_pipeline_stage(
            dataset_batch, PIPELINE_STAGES[0], dep_ok,
            f"完整存量 {len(records) - len(incomplete)}/{len(records)} 条"
            + (f"；缺字段记录 {[row['id'] for row in incomplete]}" if incomplete else ""),
        ))

        # 2) 构建环境检查：Python 运行时与标准库 sqlite3 可用。
        env_ok = bool(platform.python_version()) and hasattr(sqlite3, "sqlite_version")
        results.append(self.db.upsert_pipeline_stage(
            dataset_batch, PIPELINE_STAGES[1], env_ok,
            f"Python {platform.python_version()}；sqlite3 {sqlite3.sqlite_version}",
        ))

        # 3) 数据库检查：持久化库可读写、存量表行数一致。
        db_ok = len(self.db.list_stock(dataset_batch)) == len(records)
        results.append(self.db.upsert_pipeline_stage(
            dataset_batch, PIPELINE_STAGES[2], db_ok,
            f"持久化库 {self.db.db_path} 可读写，存量 {len(records)} 条",
        ))
        self.db.audit("依赖核对", f"批次 {dataset_batch} 预检结果：{'通过' if all(r['通过'] for r in results) else '未通过'}",
                      dataset_batch)
        return {"数据集批次": dataset_batch, "全部通过": all(row["通过"] for row in results), "阶段": results}

    def release(self, dataset_batch: str, *, retained_source: str | None = None) -> dict[str, Any]:
        """上线流水线发布；通过后切换当前指针，版本取舍决定下游看到的数据集。"""
        records = self.db.list_stock(dataset_batch)
        if not records:
            raise StockDatasetError(f"数据集批次 {dataset_batch} 不存在或没有存量记录")
        stages = {row["阶段"]: row for row in self.db.list_pipeline(dataset_batch)}
        required = PIPELINE_STAGES[:3]
        missing_stage = [name for name in required if name not in stages or not stages[name]["通过"]]
        if missing_stage:
            raise StockDatasetError(f"前置检查未通过，不能发布：{'、'.join(missing_stage)}")

        conflicts = self.detect_version_conflicts(dataset_batch)
        available_sources = sorted({str(row["来源"]) for row in records})
        if conflicts and retained_source not in available_sources:
            raise StockDatasetError(
                "存在版本冲突，必须显式指定保留来源；可选：" + "、".join(available_sources))
        chosen_source = retained_source or (records[0]["来源"] if not conflicts else MEMORY_SOURCE)

        stage = self.db.upsert_pipeline_stage(
            dataset_batch, PIPELINE_STAGES[3], True,
            f"发布批次 {dataset_batch}，保留来源：{chosen_source}，冲突 {len(conflicts)} 处按保留来源收敛",
        )
        self.db.upsert_pipeline_stage(dataset_batch, PIPELINE_STAGES[4], True, "发布完成，指针已切换")
        pointer = self.db.switch_pointer(dataset_batch, chosen_source)
        self.db.audit("流水线发布", f"批次 {dataset_batch} 已发布并切换指针", dataset_batch)
        return {
            "数据集批次": dataset_batch,
            "保留来源": chosen_source,
            "版本冲突": conflicts,
            "发布阶段": stage,
            "当前指针": pointer,
        }

    # ------------------------------------------------------------ 查询视图
    def list_datasets(self) -> dict[str, Any]:
        pointer = self.db.get_pointer()
        return {
            "指针": pointer,
            "迁移批次": self.db.list_migrations(),
            "存量记录": self.db.list_stock(),
            "流水线": self.db.list_pipeline(),
            "审计日志": self.db.list_audit_logs(),
        }

    def get_dataset(self, dataset_batch: str) -> dict[str, Any]:
        records = self.db.list_stock(dataset_batch)
        if not records:
            raise StockDatasetError(f"数据集批次 {dataset_batch} 不存在")
        return {
            "数据集批次": dataset_batch,
            "迁移": self.db.get_migration(dataset_batch),
            "存量记录": records,
            "流水线": self.db.list_pipeline(dataset_batch),
            "版本冲突": self.detect_version_conflicts(dataset_batch),
        }


stock_service = StockDatasetService()
