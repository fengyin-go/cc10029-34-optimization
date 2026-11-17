"""持久化数据仓库：把爬虫任务存量从进程内存落到本地 SQLite。

历史上爬虫任务和下发批次只保存在 ``app.store`` 的进程内存里，服务重启即丢。
本模块只用标准库 ``sqlite3``，默认落盘到 backend/data/crawler_stock.db，
重启后存量数据集、迁移进度和目标站点执行快照仍可连续追踪。

设计约定：
- 既有中文字段名（任务编号、下发批次 等）原样保留，新增字段一律以追加列的
  形式出现，不改写、不重命名旧列。
- 所有写操作在事务内完成，保证迁移中断后只留下「已完成」或「待迁移」两种状态。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from datetime import datetime
from typing import Any, Iterable

# 允许通过环境变量更换库文件位置；默认放在 backend/data 下。
DEFAULT_DB_PATH = os.environ.get(
    "CRAWLER_STOCK_DB",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "crawler_stock.db"),
)

# 爬虫任务存量数据集的既有字段（沿用爬虫引擎原始字段名，顺序不调整）。
LEGACY_FIELDS = [
    "任务编号",
    "任务名称",
    "关联站点",
    "关联规则",
    "并发线程",
    "调度周期",
    "超时秒数",
    "下发批次",
]
# 存量数据集新增字段，只追加。
APPEND_FIELDS = [
    "任务版本",
    "归属",
    "来源",
    "保留来源",
    "数据集批次",
    "内容指纹",
]
# 参与内容指纹的业务字段：任务配置 + 任务版本 + 归属。
# 来源、保留来源、数据集批次属于提交信封，不参与判重，
# 这样同一份存量数据连续/跨批次送交时指纹保持一致，全库只写入一份。
CONTENT_FIELDS = LEGACY_FIELDS + ["任务版本", "归属"]
# 进入「迁移完成」范围必须同时具备的三个字段。
REQUIRED_STOCK_FIELDS = ["任务版本", "归属", "下发批次"]

MIGRATION_STATES = ["待迁移", "迁移中", "已完成", "部分完成", "校验未通过"]
SNAPSHOT_STATES = ["待生成", "已生成", "已拒绝"]
PIPELINE_STAGES = ["依赖核对", "构建环境检查", "数据库检查", "流水线发布", "发布完成"]


def now_text() -> str:
    return datetime.now().isoformat(timespec="seconds")


def fingerprint(record: dict[str, Any]) -> str:
    """对存量记录的业务内容取稳定指纹，重复送交时据此判重。

    只取任务配置、任务版本与归属等业务字段；来源、保留来源、数据集批次
    等信封字段不参与，保证同一内容跨批次送交仍能识别为重复。
    """
    basis = {key: record.get(key) for key in CONTENT_FIELDS}
    encoded = json.dumps(basis, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class PersistentStore:
    """SQLite 访问层：线程内单连接 + 串行写锁。"""

    def __init__(self, db_path: str | None = None) -> None:
        self.db_path = db_path or DEFAULT_DB_PATH
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    # ------------------------------------------------------------------ schema
    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS stock_dataset (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    任务编号 TEXT NOT NULL,
                    任务名称 TEXT,
                    关联站点 TEXT,
                    关联规则 TEXT,
                    并发线程 TEXT,
                    调度周期 TEXT,
                    超时秒数 TEXT,
                    下发批次 TEXT,
                    任务版本 TEXT,
                    归属 TEXT,
                    来源 TEXT NOT NULL DEFAULT '内存迁移',
                    保留来源 TEXT NOT NULL DEFAULT '内存迁移',
                    数据集批次 TEXT NOT NULL,
                    内容指纹 TEXT NOT NULL,
                    迁移状态 TEXT NOT NULL DEFAULT '待迁移',
                    已校验 INTEGER NOT NULL DEFAULT 0,
                    创建时间 TEXT NOT NULL,
                    更新时间 TEXT NOT NULL,
                    UNIQUE(任务编号, 任务版本, 数据集批次)
                );

                CREATE TABLE IF NOT EXISTS migration_state (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    数据集批次 TEXT NOT NULL UNIQUE,
                    来源说明 TEXT NOT NULL DEFAULT '进程内存爬虫任务',
                    状态 TEXT NOT NULL DEFAULT '待迁移',
                    存量总数 INTEGER NOT NULL DEFAULT 0,
                    已完成数 INTEGER NOT NULL DEFAULT 0,
                    校验未通过数 INTEGER NOT NULL DEFAULT 0,
                    当前指针 INTEGER,
                    开始时间 TEXT,
                    最近续跑时间 TEXT,
                    完成时间 TEXT
                );

                CREATE TABLE IF NOT EXISTS target_snapshot (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    快照编号 TEXT NOT NULL UNIQUE,
                    站点编号 TEXT NOT NULL,
                    站点名称 TEXT,
                    入口URL TEXT,
                    任务编号 TEXT NOT NULL,
                    任务版本 TEXT NOT NULL,
                    归属 TEXT NOT NULL,
                    下发批次 TEXT NOT NULL,
                    存量记录ID INTEGER NOT NULL,
                    数据集批次 TEXT NOT NULL,
                    状态 TEXT NOT NULL DEFAULT '待生成',
                    内容指纹 TEXT NOT NULL,
                    生成时间 TEXT,
                    UNIQUE(内容指纹)
                );

                CREATE TABLE IF NOT EXISTS dataset_pointer (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    数据集批次 TEXT NOT NULL,
                    保留来源 TEXT NOT NULL,
                    切换时间 TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pipeline_check (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    数据集批次 TEXT NOT NULL,
                    阶段 TEXT NOT NULL,
                    通过 INTEGER NOT NULL,
                    说明 TEXT,
                    检查时间 TEXT NOT NULL,
                    UNIQUE(数据集批次, 阶段)
                );

                CREATE TABLE IF NOT EXISTS stock_audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    数据集批次 TEXT,
                    动作 TEXT NOT NULL,
                    详情 TEXT,
                    记录时间 TEXT NOT NULL
                );
                """
            )

    # ------------------------------------------------------------------- helpers
    def audit(self, action: str, detail: str = "", dataset_batch: str | None = None) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO stock_audit_log(数据集批次, 动作, 详情, 记录时间) VALUES (?, ?, ?, ?)",
                (dataset_batch, action, detail, now_text()),
            )

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def query_one(self, sql: str, params: Iterable[Any] = ()) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(sql, tuple(params)).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------- stock dataset
    def find_stock_by_fingerprint(self, content_hash: str) -> dict[str, Any] | None:
        return self.query_one("SELECT * FROM stock_dataset WHERE 内容指纹 = ?", (content_hash,))

    def find_stock(self, record_id: int) -> dict[str, Any] | None:
        return self.query_one("SELECT * FROM stock_dataset WHERE id = ?", (record_id,))

    def list_stock(self, dataset_batch: str | None = None) -> list[dict[str, Any]]:
        if dataset_batch:
            return self.query(
                "SELECT * FROM stock_dataset WHERE 数据集批次 = ? ORDER BY id", (dataset_batch,))
        return self.query("SELECT * FROM stock_dataset ORDER BY 数据集批次, id")

    def insert_stock(self, record: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
        """插入一条存量记录；命中唯一约束/指纹重复时返回既有记录与提示。"""
        content_hash = record.get("内容指纹") or fingerprint(record)
        record = dict(record)
        record["内容指纹"] = content_hash
        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM stock_dataset WHERE 内容指纹 = ?", (content_hash,)).fetchone()
            if existing:
                return dict(existing), "duplicate-fingerprint"
            timestamp = now_text()
            columns = [*LEGACY_FIELDS, *APPEND_FIELDS, "迁移状态", "已校验", "创建时间", "更新时间"]
            placeholders = ", ".join("?" for _ in columns)
            values = []
            for column in columns:
                if column == "迁移状态":
                    values.append(record.get(column) or "待迁移")
                elif column == "已校验":
                    values.append(record.get(column, 0) or 0)
                elif column in ("创建时间", "更新时间"):
                    values.append(timestamp)
                else:
                    values.append(record.get(column))
            try:
                cur = self._conn.execute(
                    f"INSERT INTO stock_dataset({', '.join(columns)}) VALUES ({placeholders})",
                    values,
                )
            except sqlite3.IntegrityError:
                row = self._conn.execute(
                    "SELECT * FROM stock_dataset WHERE 任务编号 = ? AND 任务版本 = ? AND 数据集批次 = ?",
                    (record.get("任务编号"), record.get("任务版本"), record.get("数据集批次")),
                ).fetchone()
                return (dict(row) if row else None), "duplicate-key"
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM stock_dataset WHERE id = ?", (cur.lastrowid,)).fetchone()
        return dict(row), None

    def mark_stock_state(self, record_id: int, state: str, validated: bool | None = None) -> None:
        validated_value = None if validated is None else (1 if validated else 0)
        with self._lock, self._conn:
            if validated_value is None:
                self._conn.execute(
                    "UPDATE stock_dataset SET 迁移状态 = ?, 更新时间 = ? WHERE id = ?",
                    (state, now_text(), record_id),
                )
            else:
                self._conn.execute(
                    "UPDATE stock_dataset SET 迁移状态 = ?, 已校验 = ?, 更新时间 = ? WHERE id = ?",
                    (state, validated_value, now_text(), record_id),
                )

    # -------------------------------------------------------------- migration run
    def get_migration(self, dataset_batch: str) -> dict[str, Any] | None:
        return self.query_one("SELECT * FROM migration_state WHERE 数据集批次 = ?", (dataset_batch,))

    def list_migrations(self) -> list[dict[str, Any]]:
        return self.query("SELECT * FROM migration_state ORDER BY id")

    def ensure_migration(self, dataset_batch: str, source_note: str, total: int) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM migration_state WHERE 数据集批次 = ?", (dataset_batch,)).fetchone()
            if row:
                return dict(row)
            self._conn.execute(
                """INSERT INTO migration_state
                   (数据集批次, 来源说明, 状态, 存量总数, 已完成数, 校验未通过数, 开始时间)
                   VALUES (?, ?, '待迁移', ?, 0, 0, ?)""",
                (dataset_batch, source_note, total, now_text()),
            )
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM migration_state WHERE 数据集批次 = ?", (dataset_batch,)).fetchone()
        return dict(row)

    def update_migration_counters(self, dataset_batch: str) -> dict[str, Any]:
        with self._lock:
            stats = self._conn.execute(
                """SELECT
                       SUM(CASE WHEN 迁移状态 = '已完成' THEN 1 ELSE 0 END) AS done,
                       SUM(CASE WHEN 迁移状态 = '校验未通过' THEN 1 ELSE 0 END) AS rejected,
                       COUNT(*) AS total
                   FROM stock_dataset WHERE 数据集批次 = ?""",
                (dataset_batch,),
            ).fetchone()
            done = int(stats["done"] or 0)
            rejected = int(stats["rejected"] or 0)
            total = int(stats["total"] or 0)
            finished = done + rejected >= total and total > 0
            state = "已完成" if finished and rejected == 0 else ("部分完成" if finished else "迁移中")
            self._conn.execute(
                """UPDATE migration_state
                   SET 状态 = ?, 已完成数 = ?, 校验未通过数 = ?, 存量总数 = ?,
                       最近续跑时间 = ?, 完成时间 = COALESCE(完成时间, ?)
                   WHERE 数据集批次 = ?""",
                (state, done, rejected, total, now_text(), now_text() if finished else None, dataset_batch),
            )
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM migration_state WHERE 数据集批次 = ?", (dataset_batch,)).fetchone()
        return dict(row)

    def set_migration_pointer(self, dataset_batch: str, record_id: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE migration_state SET 当前指针 = ? WHERE 数据集批次 = ?",
                (record_id, dataset_batch),
            )

    # ------------------------------------------------------------------ snapshots
    def find_snapshot_by_fingerprint(self, content_hash: str) -> dict[str, Any] | None:
        return self.query_one("SELECT * FROM target_snapshot WHERE 内容指纹 = ?", (content_hash,))

    def insert_snapshot(self, snapshot: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM target_snapshot WHERE 内容指纹 = ?",
                (snapshot["内容指纹"],),
            ).fetchone()
            if existing:
                return dict(existing), "duplicate-snapshot"
            columns = [
                "快照编号", "站点编号", "站点名称", "入口URL", "任务编号", "任务版本",
                "归属", "下发批次", "存量记录ID", "数据集批次", "状态", "内容指纹", "生成时间",
            ]
            values = [snapshot.get(column) for column in columns if column != "生成时间"]
            values.append(now_text())
            placeholders = ", ".join("?" for _ in columns)
            try:
                cur = self._conn.execute(
                    f"INSERT INTO target_snapshot({', '.join(columns)}) VALUES ({placeholders})", values)
            except sqlite3.IntegrityError:
                row = self._conn.execute(
                    "SELECT * FROM target_snapshot WHERE 快照编号 = ?", (snapshot["快照编号"],)).fetchone()
                return (dict(row) if row else None), "duplicate-snapshot"
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM target_snapshot WHERE id = ?", (cur.lastrowid,)).fetchone()
        return dict(row), None

    def list_snapshots(self, site_code: str | None = None) -> list[dict[str, Any]]:
        if site_code:
            return self.query(
                "SELECT * FROM target_snapshot WHERE 站点编号 = ? ORDER BY id DESC", (site_code,))
        return self.query("SELECT * FROM target_snapshot ORDER BY id DESC")

    def mark_snapshot_rejected(self, snapshot_id: int) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE target_snapshot SET 状态 = '已拒绝' WHERE id = ?", (snapshot_id,))

    # ------------------------------------------------------------------- pointer
    def get_pointer(self) -> dict[str, Any] | None:
        return self.query_one("SELECT * FROM dataset_pointer WHERE id = 1")

    def switch_pointer(self, dataset_batch: str, retained_source: str) -> dict[str, Any]:
        with self._lock, self._conn:
            self._conn.execute(
                """INSERT INTO dataset_pointer(id, 数据集批次, 保留来源, 切换时间)
                   VALUES (1, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                       数据集批次 = excluded.数据集批次,
                       保留来源 = excluded.保留来源,
                       切换时间 = excluded.切换时间""",
                (dataset_batch, retained_source, now_text()),
            )
            row = self._conn.execute("SELECT * FROM dataset_pointer WHERE id = 1").fetchone()
        return dict(row)

    # ------------------------------------------------------------------ pipeline
    def upsert_pipeline_stage(self, dataset_batch: str, stage: str, passed: bool, note: str) -> dict[str, Any]:
        with self._lock, self._conn:
            self._conn.execute(
                """INSERT INTO pipeline_check(数据集批次, 阶段, 通过, 说明, 检查时间)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(数据集批次, 阶段) DO UPDATE SET
                       通过 = excluded.通过, 说明 = excluded.说明, 检查时间 = excluded.检查时间""",
                (dataset_batch, stage, 1 if passed else 0, note, now_text()),
            )
            row = self._conn.execute(
                "SELECT * FROM pipeline_check WHERE 数据集批次 = ? AND 阶段 = ?",
                (dataset_batch, stage),
            ).fetchone()
        return dict(row)

    def list_pipeline(self, dataset_batch: str | None = None) -> list[dict[str, Any]]:
        if dataset_batch:
            return self.query(
                "SELECT * FROM pipeline_check WHERE 数据集批次 = ? ORDER BY id", (dataset_batch,))
        return self.query("SELECT * FROM pipeline_check ORDER BY id")

    def list_audit_logs(self, limit: int = 100) -> list[dict[str, Any]]:
        return self.query(
            "SELECT * FROM stock_audit_log ORDER BY id DESC LIMIT ?", (limit,))


persistent_store = PersistentStore()
