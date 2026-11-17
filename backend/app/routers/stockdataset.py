"""爬虫任务存量数据集接口（爬虫引擎调用链）。

路由统一挂在 /api/crawler 之下，体现「沿着爬虫引擎调用链梳理存量数据集来源
与目标站点执行快照」：
- 内存存量迁移（可断点续跑、编号稳定）
- 外部存量数据集送交（内容指纹判重，只写入一份）
- 依赖核对 / 构建环境与数据库检查 / 上线流水线发布与指针切换
- 迁移前后编号、归属、下发批次的核对视图
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.schemas import ActionResult
from app.services.stockdataset import StockDatasetError, stock_service

router = APIRouter(prefix="/api/crawler/stock-datasets", tags=["爬虫引擎-存量数据集"])


class SubmitPayload(BaseModel):
    dataset_batch: str = Field(..., description="数据集批次编号")
    records: list[dict[str, Any]] = Field(default_factory=list, description="存量记录列表")
    retained_source: str | None = Field(default=None, description="版本冲突时保留的来源")


class ReleasePayload(BaseModel):
    retained_source: str | None = Field(default=None, description="版本冲突时保留的来源")


def _fail(message: str) -> ActionResult:
    return ActionResult(ok=False, message=message)


@router.get("")
def list_datasets() -> dict[str, Any]:
    """存量数据集总览：当前指针、迁移批次、存量记录、流水线与审计日志。"""
    return stock_service.list_datasets()


@router.post("/migration")
def migrate_memory(batch_size: int | None = None) -> ActionResult:
    """把进程内存中的爬虫任务迁移为存量数据集；中断后重跑只续跑未完成记录。"""
    try:
        result = stock_service.start_or_resume_migration(batch_size=batch_size)
    except StockDatasetError as exc:
        return _fail(str(exc))
    progress = result["进度"]
    message = (
        f"批次 {result['数据集批次']}：已完成 {progress['已完成数']}/{progress['存量总数']}，"
        f"校验未通过 {progress['校验未通过数']}，"
        + ("迁移已中断，可再次调用续跑" if result["中断"] else "本批已处理完")
    )
    return ActionResult(ok=True, message=message, entry=result)


@router.post("/submissions")
def submit_dataset(payload: SubmitPayload) -> ActionResult:
    """接收外部送交的存量数据集；重复/连续送交按内容指纹只写入一份。"""
    try:
        result = stock_service.submit_dataset(
            payload.dataset_batch, payload.records, retained_source=payload.retained_source or "外部送交")
    except StockDatasetError as exc:
        return _fail(str(exc))
    message = (
        f"批次 {result['数据集批次']}：接收 {result['接收总数']} 条，"
        f"唯一写入 {result['唯一写入']} 条，重复跳过 {len(result['重复跳过'])} 条，"
        f"校验未通过 {len(result['校验未通过'])} 条"
    )
    return ActionResult(ok=True, message=message, entry=result)


@router.get("/{dataset_batch}")
def get_dataset(dataset_batch: str) -> dict[str, Any]:
    try:
        return stock_service.get_dataset(dataset_batch)
    except StockDatasetError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{dataset_batch}/precheck")
def run_precheck(dataset_batch: str) -> ActionResult:
    """迁移先核对依赖：依赖核对、构建环境检查、数据库检查。"""
    try:
        result = stock_service.run_precheck(dataset_batch)
    except StockDatasetError as exc:
        return _fail(str(exc))
    return ActionResult(
        ok=bool(result["全部通过"]),
        message="依赖核对全部通过，可进入上线流水线" if result["全部通过"] else "依赖核对未通过，请查看阶段明细",
        entry=result,
    )


@router.post("/{dataset_batch}/release")
def release_dataset(dataset_batch: str, payload: ReleasePayload | None = None) -> ActionResult:
    """上线流水线发布；版本取舍决定下游当前指针。"""
    retained = payload.retained_source if payload else None
    try:
        result = stock_service.release(dataset_batch, retained_source=retained)
    except StockDatasetError as exc:
        return _fail(str(exc))
    return ActionResult(
        ok=True,
        message=f"批次 {dataset_batch} 已发布，当前指针保留来源：{result['保留来源']}",
        entry=result,
    )


@router.get("/{dataset_batch}/conflicts")
def list_conflicts(dataset_batch: str) -> dict[str, Any]:
    """旧内存任务与新记录的版本冲突清单。"""
    return {"数据集批次": dataset_batch, "版本冲突": stock_service.detect_version_conflicts(dataset_batch)}
