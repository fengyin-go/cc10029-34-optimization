"""目标站点执行快照服务。

采集目标读取爬虫任务存量数据集生成执行快照：
- 快照必须引用迁移后的任务归属、任务版本和已下发批次；
- 同一份存量数据集连续生成只保留一份（内容指纹唯一），重复请求被拒绝；
- 没有已发布的当前指针数据集时不允许生成。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from app.persistence import persistent_store
from app.services.stockdataset import StockDatasetError
from app.store import store

TARGETSITE_MODULE = "targetsite"


def snapshot_fingerprint(payload: dict[str, Any]) -> str:
    basis = {
        "站点编号": payload.get("站点编号"),
        "任务编号": payload.get("任务编号"),
        "任务版本": payload.get("任务版本"),
        "归属": payload.get("归属"),
        "下发批次": payload.get("下发批次"),
        "数据集批次": payload.get("数据集批次"),
    }
    encoded = json.dumps(basis, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class TargetSnapshotService:
    def __init__(self) -> None:
        self.db = persistent_store

    def _site_lookup(self) -> dict[str, dict[str, Any]]:
        return {str(row.get("站点编号")): row for row in store.rows(TARGETSITE_MODULE)}

    def generate(self) -> dict[str, Any]:
        pointer = self.db.get_pointer()
        if pointer is None:
            raise StockDatasetError("尚未发布爬虫任务存量数据集，无法生成目标站点执行快照")
        dataset_batch = pointer["数据集批次"]
        # 只取迁移完成且字段完整的存量记录进入快照。
        records = [
            row for row in self.db.list_stock(dataset_batch)
            if row["迁移状态"] == "已完成"
            and row["任务版本"] and row["归属"] and row["下发批次"]
        ]
        if not records:
            raise StockDatasetError(f"数据集批次 {dataset_batch} 没有可生成快照的完整存量记录")

        sites = self._site_lookup()
        created: list[dict[str, Any]] = []
        duplicates: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        for index, record in enumerate(records, start=1):
            site_code = str(record.get("关联站点") or "").strip()
            site = sites.get(site_code, {})
            payload = {
                "快照编号": f"SNAP-{dataset_batch}-{index:04d}",
                "站点编号": site_code or f"UNMAPPED-{record['id']}",
                "站点名称": site.get("站点名称", record.get("关联站点")),
                "入口URL": site.get("入口URL", ""),
                "任务编号": record["任务编号"],
                "任务版本": record["任务版本"],
                "归属": record["归属"],
                "下发批次": record["下发批次"],
                "存量记录ID": record["id"],
                "数据集批次": dataset_batch,
                "状态": "已生成",
            }
            payload["内容指纹"] = snapshot_fingerprint(payload)
            existing = self.db.find_snapshot_by_fingerprint(payload["内容指纹"])
            if existing:
                # 连续送交只生成一份：重复结果拒绝写入，保留原快照编号。
                duplicates.append({"快照编号": existing["快照编号"], "任务编号": existing["任务编号"]})
                continue
            saved, reason = self.db.insert_snapshot(payload)
            if reason:
                rejected.append({"任务编号": payload["任务编号"], "原因": reason})
                continue
            created.append(saved)
        self.db.audit(
            "快照生成",
            f"基于批次 {dataset_batch} 生成 {len(created)} 份，重复拒绝 {len(duplicates)} 份",
            dataset_batch,
        )
        return {
            "数据集批次": dataset_batch,
            "当前指针": pointer,
            "生成": created,
            "重复拒绝": duplicates,
            "失败": rejected,
        }

    def list_snapshots(self, site_code: str | None = None) -> list[dict[str, Any]]:
        return self.db.list_snapshots(site_code)
