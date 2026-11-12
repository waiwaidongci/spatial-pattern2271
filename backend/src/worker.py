"""独立计算 worker：可恢复、可并发、幂等。

设计要点：
- 从 Redis 阻塞读取任务，claim_run 以条件 UPDATE 原子领取（fencing token）；
  并发领取与重复投递只会有一个持有者，其余静默跳过，不会重复发布。
- 计算期间守护线程按心跳续租；进程中断后租约到期，其他 worker 经
  reclaim_stuck 重新投递并接手，沿用建单冻结的 input_snapshot。
- 发布时校验持有者身份与 attempt：失去资格的旧计算迟到不能覆盖接手者结果。
- 完整结果文件与库内结果核对一致后，状态才翻转为 completed。
"""
import logging
import os
import threading
import time
import traceback
from pathlib import Path

from .config.database import Base, engine, SessionLocal
from .config.schema_bootstrap import ensure_run_schema
from .config.settings import (
    ARTIFACT_DIR,
    JOB_QUEUE_KEY,
    RUN_HEARTBEAT_SECONDS,
    RUN_SWEEP_SECONDS,
)
from .services.analysis_service import analyze_spatial
from .services.artifact_service import artifact_dir
from .services.run_lifecycle import (
    claim_run,
    publish_completed,
    publish_failed,
    reclaim_stuck,
    renew_lease,
)
from .services.run_service import queue

logger = logging.getLogger("spatial.worker")


def worker_id() -> str:
    return f"worker-{os.getpid()}"


class LeaseHeartbeat(threading.Thread):
    """计算期间周期性续租；停止续租即代表持有者可能已失格。"""

    def __init__(self, run_id, owner, attempt, interval=RUN_HEARTBEAT_SECONDS):
        super().__init__(daemon=True)
        self.run_id = run_id
        self.owner = owner
        self.attempt = attempt
        self.interval = float(interval)
        self._stop_event = threading.Event()
        self.lost = False

    def stop(self):
        self._stop_event.set()

    def run(self):
        while not self._stop_event.wait(self.interval):
            with SessionLocal() as db:
                if not renew_lease(db, self.run_id, self.owner, self.attempt):
                    self.lost = True
                    return


def process_job(run_id, owner=None) -> str:
    """处理单个任务 id，返回结果标记：completed/failed/skipped。"""
    owner = owner or worker_id()
    with SessionLocal() as db:
        run = claim_run(db, run_id, owner)
        if run is None:
            # 重复投递或已被其他 worker 领取：不重复计算、不重复发布。
            return "skipped"
        attempt = run.attempts
        snapshot = run.input_snapshot
        permutations = run.permutations
        seed = run.seed
    heartbeat = LeaseHeartbeat(run_id, owner, attempt)
    heartbeat.start()
    try:
        # 始终使用建单时冻结的网格与邻接快照，不回读可变来源数据。
        result = analyze_spatial(snapshot, permutations, seed)
        with SessionLocal() as db:
            if publish_completed(db, run_id, owner, attempt, result):
                return "completed"
            logger.warning("run %s attempt %s 失去执行资格，结果被拒绝", run_id, attempt)
            return "skipped"
    except Exception as exc:
        traceback.print_exc()
        with SessionLocal() as db:
            if not publish_failed(db, run_id, owner, attempt, str(exc)):
                logger.warning("run %s attempt %s 失败回报迟到，已忽略", run_id, attempt)
                return "skipped"
            return "failed"
    finally:
        heartbeat.stop()
        heartbeat.join(timeout=2)


def sweep(owner=None):
    """周期回收：队列丢失的 queued 与租约到期的 running 重新投递。"""
    owner = owner or worker_id()
    try:
        with SessionLocal() as db:
            stats = reclaim_stuck(db, queue)
        if stats["requeued"] or stats["reclaimed"]:
            logger.info("%s 回收任务 %s", owner, stats)
    except Exception:
        logger.exception("回收扫描失败，下轮重试")


def main():
    logging.basicConfig(level=logging.INFO)
    Base.metadata.create_all(engine)
    ensure_run_schema()
    Path(ARTIFACT_DIR).mkdir(parents=True, exist_ok=True)
    artifact_dir()
    owner = worker_id()
    last_sweep = 0.0
    while True:
        Path(ARTIFACT_DIR, "heartbeat").write_text(str(time.time()))
        now_ts = time.monotonic()
        if now_ts - last_sweep >= RUN_SWEEP_SECONDS:
            sweep(owner)
            last_sweep = now_ts
        job = queue.blpop(JOB_QUEUE_KEY, timeout=2)
        if not job:
            continue
        try:
            process_job(int(job[1]), owner)
        except Exception:
            logger.exception("任务 %s 处理异常，等待回收机制兜底", job[1])


if __name__ == "__main__":
    main()
