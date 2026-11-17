"""爬虫任务存量数据集需求链路自测（标准库直跑，无需 pytest）。

运行：.venv/bin/python -I tests/test_stockdataset_manual.py
每个场景独立临时库，跑完即删。
"""
from __future__ import annotations

import os
import sys
import tempfile

# 让 -I 模式下也能 import 到 app（脚本目录的上级是 backend/）。
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.persistence import PersistentStore  # noqa: E402
from app.services.stockdataset import StockDatasetError, StockDatasetService  # noqa: E402
from app.services.target_snapshot import TargetSnapshotService  # noqa: E402
from app.store import Store  # noqa: E402


def fresh_context(tmpdir: str):
    """每个场景一套全新内存仓库 + 全新 SQLite 库。"""
    import app.services.stockdataset as sd_mod
    import app.services.target_snapshot as ts_mod
    import app.persistence as p_mod

    mem = Store()
    db = PersistentStore(os.path.join(tmpdir, "stock.db"))
    service = StockDatasetService(db)
    snapshot_service = TargetSnapshotService()
    snapshot_service.db = db
    # 服务内部引用的是全局 store，这里换成场景专属内存仓库。
    sd_mod.store = mem
    ts_mod.store = mem
    return mem, db, service, snapshot_service


def check(condition: bool, label: str) -> None:
    if not condition:
        raise AssertionError(label)
    print(f"  PASS {label}")


def scenario_migration_resume(tmpdir: str) -> None:
    print("场景1 内存迁移、中断续跑、编号稳定、三必填校验")
    mem, db, service, _ = fresh_context(tmpdir)
    rows = mem.rows("crawler")
    check(len(rows) == 3, "内存中存在 3 条爬虫任务")

    # 第一批只迁移 1 条，模拟中断。
    first = service.start_or_resume_migration(batch_size=1)
    check(first["中断"] is True, "batch_size=1 时迁移标记为中断")
    check(len(first["本次迁移编号"]) == 1, "本次只完成 1 条")
    done_ids_first = list(first["本次迁移编号"])

    # 续跑：不允许重排已校验记录编号。
    second = service.start_or_resume_migration()
    check(second["续跑"] is True, "再次调用识别为续跑而非新建批次")
    check(second["数据集批次"] == first["数据集批次"], "续跑沿用同一数据集批次")
    check(all(i not in second["本次迁移编号"] for i in done_ids_first), "已完成记录不会被重新迁移")
    progress = second["进度"]
    check(progress["已完成数"] == 3 and progress["状态"] == "已完成", "续跑后 3 条全部完成")

    records = db.list_stock(first["数据集批次"])
    check(all(r["任务版本"] for r in records), "每条存量记录都有任务版本")
    check(all(r["归属"] for r in records), "每条存量记录都有归属")
    check(all(r["下发批次"] for r in records), "每条存量记录都有已下发批次")
    check(all(r["已校验"] == 1 and r["迁移状态"] == "已完成" for r in records), "完成记录均已校验")
    # 既有字段名原样保留。
    legacy = ["任务编号", "任务名称", "关联站点", "关联规则", "并发线程", "调度周期", "超时秒数", "下发批次"]
    check(all(field in records[0] for field in legacy), "既有字段名全部沿用，新字段只追加")
    # 编号稳定：自增 id 顺序与内存一致。
    check([r["id"] for r in records] == [1, 2, 3], "存量记录编号连续且不重排")
    check(rows[0]["任务编号"] == records[0]["任务编号"], "迁移前后任务编号保持一致")


def scenario_missing_fields(tmpdir: str) -> None:
    print("场景2 缺任一字段的存量记录不能进入迁移完成范围")
    mem, db, service, _ = fresh_context(tmpdir)
    result = service.submit_dataset(
        "BATCH-MISS",
        [
            {"任务编号": "CRAW-X1", "任务版本": "v2", "归属": "一组", "下发批次": "B-1"},
            {"任务编号": "CRAW-X2", "任务版本": "v2", "归属": "", "下发批次": "B-2"},
            {"任务编号": "CRAW-X3", "任务版本": "", "归属": "二组", "下发批次": "B-3"},
            {"任务编号": "CRAW-X4", "任务版本": "v2", "归属": "三组", "下发批次": ""},
        ],
    )
    check(result["唯一写入"] == 1, "只有 1 条三字段齐全的记录完成")
    check(len(result["校验未通过"]) == 3, "3 条缺字段记录被挡在完成范围外")
    states = {r["任务编号"]: r["迁移状态"] for r in db.list_stock("BATCH-MISS")}
    check(states["CRAW-X1"] == "已完成", "完整记录进入已完成")
    check(all(states[k] == "校验未通过" for k in ("CRAW-X2", "CRAW-X3", "CRAW-X4")), "缺字段记录标记校验未通过")


def scenario_dedup(tmpdir: str) -> None:
    print("场景3 重复/连续送交只写入一份")
    mem, db, service, _ = fresh_context(tmpdir)
    payload = [{"任务编号": "CRAW-D1", "任务版本": "v1", "归属": "平台组", "下发批次": "BD-1",
                "任务名称": "去重任务"}]
    first = service.submit_dataset("BATCH-DUP", payload)
    second = service.submit_dataset("BATCH-DUP", payload)
    check(first["唯一写入"] == 1, "首次送交写入 1 条")
    check(second["唯一写入"] == 0 and len(second["重复跳过"]) == 1, "连续送交判重，不再写入")
    check(len(db.list_stock("BATCH-DUP")) == 1, "数据集内始终只有一份")

    # 换批次但内容相同，全库指纹仍判重。
    third = service.submit_dataset("BATCH-OTHER", payload)
    check(len(third["重复跳过"]) == 1, "跨批次相同内容同样拒绝重复写入")


def scenario_conflict_release_pointer(tmpdir: str) -> None:
    print("场景4 版本冲突、预检、发布与指针切换")
    mem, db, service, _ = fresh_context(tmpdir)
    # 内存迁移得到 内存迁移/v1 的 CRAW-0001。
    migration = service.start_or_resume_migration()
    batch_mem = migration["数据集批次"]
    # 外部送交同任务编号的新版本，制造版本冲突。
    ext_batch = "BATCH-EXT"
    service.submit_dataset(ext_batch, [
        {"任务编号": mem.rows("crawler")[0]["任务编号"], "任务版本": "v9",
         "归属": "外部团队", "下发批次": "BX-9", "任务名称": "外部同名任务"},
    ])
    conflicts = service.detect_version_conflicts(ext_batch)
    check(len(conflicts) == 1, "同任务编号跨来源不同版本被识别为冲突")

    # 外部批次预检通过后发布；有冲突却不指定保留来源应被拒绝。
    precheck = service.run_precheck(ext_batch)
    check(precheck["全部通过"] is True, "依赖核对/构建环境/数据库三阶段通过")
    raised = False
    try:
        service.release(ext_batch)
    except StockDatasetError:
        raised = True
    check(raised, "存在版本冲突且未指定保留来源时拒绝发布")

    release = service.release(ext_batch, retained_source="外部送交")
    pointer = db.get_pointer()
    check(pointer["数据集批次"] == ext_batch and pointer["保留来源"] == "外部送交",
          "发布后当前指针切到外部批次且只保留一份来源")
    stages = {r["阶段"]: r["通过"] for r in db.list_pipeline(ext_batch)}
    check(all(stages.values()) and "流水线发布" in stages and "发布完成" in stages,
          "上线流水线发布阶段完整留痕")


def scenario_precheck_blocks_release(tmpdir: str) -> None:
    print("场景5 预检不过不能发布")
    mem, db, service, _ = fresh_context(tmpdir)
    service.submit_dataset("BATCH-BAD", [
        {"任务编号": "CRAW-B1", "任务版本": "v1", "归属": "", "下发批次": "B"},
    ])
    precheck = service.run_precheck("BATCH-BAD")
    check(precheck["全部通过"] is False, "存在缺字段记录时依赖核对不通过")
    raised = False
    try:
        service.release("BATCH-BAD")
    except StockDatasetError:
        raised = True
    check(raised, "预检未通过的批次不能走上线流水线")
    check(db.get_pointer() is None, "未发布前不存在当前指针")


def scenario_snapshot(tmpdir: str) -> None:
    print("场景6 执行快照引用归属与批次，重复生成拒绝")
    mem, db, service, snapshot_service = fresh_context(tmpdir)
    raised = False
    try:
        snapshot_service.generate()
    except StockDatasetError:
        raised = True
    check(raised, "没有已发布数据集时拒绝生成快照")

    migration = service.start_or_resume_migration()
    service.run_precheck(migration["数据集批次"])
    service.release(migration["数据集批次"])
    first = snapshot_service.generate()
    check(len(first["生成"]) == 3, "基于当前指针数据集生成 3 份快照")
    snap = first["生成"][0]
    stock = next(r for r in db.list_stock(migration["数据集批次"]) if r["id"] == snap["存量记录ID"])
    check(snap["归属"] == stock["归属"], "快照引用迁移后的任务归属")
    check(snap["下发批次"] == stock["下发批次"], "快照引用已下发批次")
    check(snap["任务版本"] == stock["任务版本"], "快照引用任务版本")
    check(snap["数据集批次"] == db.get_pointer()["数据集批次"], "快照挂在当前指针数据集上")

    second = snapshot_service.generate()
    check(len(second["生成"]) == 0 and len(second["重复拒绝"]) == 3, "连续生成只保留一份，重复被拒绝")
    check(len(snapshot_service.list_snapshots()) == 3, "快照清单总数不膨胀")

    # 切换指针到新数据集后，新快照与旧快照并存但各自唯一。
    service.submit_dataset("BATCH-NEW", [
        {"任务编号": "CRAW-N1", "任务版本": "v3", "归属": "新归属", "下发批次": "BN-1"},
    ])
    service.run_precheck("BATCH-NEW")
    service.release("BATCH-NEW")
    switched = snapshot_service.generate()
    check(len(switched["生成"]) == 1, "指针切换后按新数据集生成快照")
    check(db.get_pointer()["数据集批次"] == "BATCH-NEW", "版本取舍决定下游当前指针")


def scenario_restart_persistence(tmpdir: str) -> None:
    print("场景7 服务重启后存量数据集与迁移进度仍可追踪")
    mem, db, service, _ = fresh_context(tmpdir)
    result = service.start_or_resume_migration(batch_size=2)
    batch = result["数据集批次"]
    finished_before = {r["id"]: r["任务编号"] for r in db.list_stock(batch) if r["迁移状态"] == "已完成"}
    db_path = db.db_path

    # 模拟重启：丢弃内存对象，新建 PersistentStore 指向同一库文件。
    del db
    reloaded = PersistentStore(db_path)
    reloaded_service = StockDatasetService(reloaded)
    records = reloaded.list_stock(batch)
    check(len(records) == 3, "重启后存量数据集 3 条记录仍在")
    run = reloaded.get_migration(batch)
    check(run["已完成数"] == 2 and run["状态"] in ("迁移中", "部分完成", "待迁移"), "重启保留中断进度")
    resume = reloaded_service.start_or_resume_migration()
    check(resume["续跑"] is True, "重启后续跑只处理剩余未完成记录")
    after = {r["id"]: r["任务编号"] for r in reloaded.list_stock(batch) if r["迁移状态"] == "已完成"}
    check(finished_before.items() <= after.items(), "重启前已迁移任务编号未被改变")
    check(len(after) == 3, "重启续跑后全部完成且无重复")


def main() -> None:
    scenarios = [
        scenario_migration_resume,
        scenario_missing_fields,
        scenario_dedup,
        scenario_conflict_release_pointer,
        scenario_precheck_blocks_release,
        scenario_snapshot,
        scenario_restart_persistence,
    ]
    for index, scenario in enumerate(scenarios):
        with tempfile.TemporaryDirectory(prefix=f"stock-case-{index}-") as tmpdir:
            scenario(tmpdir)
    print(f"\n全部 {len(scenarios)} 个场景通过")


if __name__ == "__main__":
    main()
