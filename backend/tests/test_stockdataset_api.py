"""存量数据集 HTTP 接口冒烟测试：只用标准库，自动起停 uvicorn。

运行：.venv/bin/python -I tests/test_stockdataset_api.py
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def call(base: str, method: str, path: str, payload: object | None = None) -> tuple[int, dict]:
    data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def wait_ready(base: str, process: subprocess.Popen) -> None:
    for _ in range(50):
        if process.poll() is not None:
            raise RuntimeError("uvicorn 提前退出")
        try:
            status, body = call(base, "GET", "/api/health")
            if status == 200 and body.get("ok"):
                return
        except OSError:
            pass
        time.sleep(0.2)
    raise RuntimeError("服务健康检查超时")


def main() -> None:
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    with tempfile.TemporaryDirectory(prefix="stock-api-") as tmpdir:
        env = dict(os.environ, CRAWLER_STOCK_DB=os.path.join(tmpdir, "stock.db"))
        process = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(port)],
            cwd=BACKEND_DIR, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        try:
            wait_ready(base, process)

            # 路由顺序：stock-datasets 不会被 /api/crawler/{entry_id} 吞掉。
            status, overview = call(base, "GET", "/api/crawler/stock-datasets")
            assert status == 200 and overview["存量记录"] == [], overview

            # 中断迁移 + 续跑。
            status, first = call(base, "POST", "/api/crawler/stock-datasets/migration?batch_size=1")
            assert status == 200 and first["ok"] and first["entry"]["中断"], first
            status, second = call(base, "POST", "/api/crawler/stock-datasets/migration")
            assert second["entry"]["续跑"] and second["entry"]["进度"]["已完成数"] == 3, second
            batch = second["entry"]["数据集批次"]

            # 重复提交外部数据集只写一份。
            record = {"任务编号": "CRAW-API1", "任务版本": "v2", "归属": "接口组", "下发批次": "BA-1"}
            status, r1 = call(base, "POST", "/api/crawler/stock-datasets/submissions",
                              {"dataset_batch": "BATCH-API", "records": [record]})
            assert r1["ok"] and r1["entry"]["唯一写入"] == 1, r1
            status, r2 = call(base, "POST", "/api/crawler/stock-datasets/submissions",
                              {"dataset_batch": "BATCH-API", "records": [record]})
            assert r2["entry"]["唯一写入"] == 0 and len(r2["entry"]["重复跳过"]) == 1, r2

            # 预检 → 发布 → 快照，重复快照被拒绝。
            status, pre = call(base, "POST", f"/api/crawler/stock-datasets/{batch}/precheck")
            assert pre["ok"] and pre["entry"]["全部通过"], pre
            status, rel = call(base, "POST", f"/api/crawler/stock-datasets/{batch}/release", {})
            assert rel["ok"], rel
            status, snap1 = call(base, "POST", "/api/targetsite/snapshots/generate")
            assert snap1["ok"] and len(snap1["entry"]["生成"]) == 3, snap1
            status, snap2 = call(base, "POST", "/api/targetsite/snapshots/generate")
            assert len(snap2["entry"]["生成"]) == 0 and len(snap2["entry"]["重复拒绝"]) == 3, snap2
            status, listing = call(base, "GET", "/api/targetsite/snapshots")
            assert listing["total"] == 3, listing
            snapshot = listing["items"][0]
            assert snapshot["归属"] and snapshot["任务版本"] and snapshot["下发批次"], snapshot

            # 缺字段批次预检不过、禁止发布。
            call(base, "POST", "/api/crawler/stock-datasets/submissions",
                 {"dataset_batch": "BATCH-INVALID",
                  "records": [{"任务编号": "CRAW-BAD", "任务版本": "v1", "下发批次": "X"}]})
            status, bad_pre = call(base, "POST", "/api/crawler/stock-datasets/BATCH-INVALID/precheck")
            assert bad_pre["ok"] is False, bad_pre
            status, bad_rel = call(base, "POST", "/api/crawler/stock-datasets/BATCH-INVALID/release", {})
            assert bad_rel["ok"] is False, bad_rel

            print("API 冒烟测试全部通过")
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()


if __name__ == "__main__":
    main()
