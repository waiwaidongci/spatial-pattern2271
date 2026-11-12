"""研究任务的领取、租约、接管与幂等发布。

并发模型：
- claim_run 用条件 UPDATE 做 CAS，attempts 单调递增并作为 fencing token；
  同一时刻只有一个持有者能把任务推进到新的 attempt。
- publish_completed / publish_failed 只接受“当前持有者 + 当前 attempt”，
  失去执行资格的旧计算迟到时写入被拒绝，不能覆盖接手者的结果。
- reclaim_stuck 由 worker 周期执行：队列短暂不可用丢失的 queued 任务会被
  重新投递；租约到期的 running 任务（计算进程中断）会被重新投递并可被接手。
所有计算始终沿用建单时冻结的 input_snapshot，不回读可能变化的网格与邻接。
"""
from sqlalchemy import update, func
from ..models import SpatialRun
from ..constants.status import RunStatus
from ..utils.clock import now, iso_after
from ..config.settings import RUN_LEASE_SECONDS, JOB_QUEUE_KEY


def _lease_cutoff(seconds=RUN_LEASE_SECONDS):
    return iso_after(now(), seconds=0 - float(seconds))


def claim_run(db, run_id, worker_id) -> SpatialRun | None:
    """原子领取一个可执行任务，返回带新租约的行；无权领取时返回 None。

    可领取条件：
    - queued：排队中（含被 sweeper 重新投递的丢失任务）；
    - running 且 lease_expires_at 已到期：上一持有者中断，允许接手。
    """
    row = db.get(SpatialRun, run_id)
    if row is None:
        return None
    if row.status == RunStatus.QUEUED:
        eligible = True
    elif row.status == RunStatus.RUNNING and row.lease_expires_at and row.lease_expires_at <= now():
        eligible = True
    else:
        eligible = False
    if not eligible:
        return None
    previous_owner = row.lease_owner
    next_attempt = (row.attempts or 0) + 1
    result = db.execute(
        update(SpatialRun)
        .where(SpatialRun.id == run_id)
        .where(
            (SpatialRun.status == RunStatus.QUEUED)
            | (
                (SpatialRun.status == RunStatus.RUNNING)
                & (SpatialRun.lease_expires_at.is_not(None))
                & (SpatialRun.lease_expires_at <= now())
            )
        )
        .values(
            status=RunStatus.RUNNING,
            attempts=func.coalesce(SpatialRun.attempts, 0) + 1,
            lease_owner=worker_id,
            lease_expires_at=iso_after(now(), RUN_LEASE_SECONDS),
        )
    )
    if result.rowcount != 1:
        db.rollback()
        return None
    db.commit()
    db.refresh(row)
    if previous_owner and previous_owner != worker_id:
        from .audit_service import log
        log(db, worker_id, "spatial.reclaimed", f"{run_id}:{next_attempt}")
        db.commit()
    return row


def renew_lease(db, run_id, worker_id, attempt) -> bool:
    """计算期间续租；仅当前持有者可续，旧持有者续租失败即应放弃。"""
    result = db.execute(
        update(SpatialRun)
        .where(SpatialRun.id == run_id)
        .where(SpatialRun.attempts == attempt)
        .where(SpatialRun.lease_owner == worker_id)
        .where(SpatialRun.status == RunStatus.RUNNING)
        .values(lease_expires_at=iso_after(now(), RUN_LEASE_SECONDS))
    )
    db.commit()
    return result.rowcount == 1


def _still_owner(db, run_id, worker_id, attempt) -> SpatialRun | None:
    row = db.get(SpatialRun, run_id)
    if (
        row is not None
        and row.status == RunStatus.RUNNING
        and row.attempts == attempt
        and row.lease_owner == worker_id
    ):
        return row
    return None


def publish_completed(db, run_id, worker_id, attempt, result) -> bool:
    """幂等发布成功结果。先核对完整结果文件，再翻转完成状态。

    只有当前持有者（worker_id + fencing attempt 同时匹配）可以发布；
    已完成/失败或已被接手的任务，迟到的旧计算一律拒绝。
    """
    from .artifact_service import write_verified_artifact, verify_artifact
    row = _still_owner(db, run_id, worker_id, attempt)
    if row is None:
        return False
    # 完整结果文件先落盘并通过一致性核对，状态才允许变为 completed。
    write_verified_artifact(run_id, result)
    updated = db.execute(
        update(SpatialRun)
        .where(SpatialRun.id == run_id)
        .where(SpatialRun.attempts == attempt)
        .where(SpatialRun.lease_owner == worker_id)
        .where(SpatialRun.status == RunStatus.RUNNING)
        .values(
            status=RunStatus.COMPLETED,
            result=result,
            error="",
            lease_owner="",
            lease_expires_at="",
        )
    )
    if updated.rowcount != 1:
        db.rollback()
        return False
    db.commit()
    # 提交后再次确认：文件与库内结果一致，完成状态才真正对外可见。
    db.refresh(row)
    if not verify_artifact(run_id, row.result):
        raise RuntimeError("完整结果文件与研究结果不一致，拒绝完成")
    from .audit_service import log
    log(db, worker_id, "spatial.completed", f"{run_id}:{attempt}")
    db.commit()
    return True


def publish_failed(db, run_id, worker_id, attempt, error) -> bool:
    """幂等发布失败（如常量网格等无效输入）；失去资格的旧计算不得覆盖接手者。"""
    row = _still_owner(db, run_id, worker_id, attempt)
    if row is None:
        return False
    updated = db.execute(
        update(SpatialRun)
        .where(SpatialRun.id == run_id)
        .where(SpatialRun.attempts == attempt)
        .where(SpatialRun.lease_owner == worker_id)
        .where(SpatialRun.status == RunStatus.RUNNING)
        .values(
            status=RunStatus.FAILED,
            error=(error or "计算失败")[:2000],
            lease_owner="",
            lease_expires_at="",
        )
    )
    if updated.rowcount != 1:
        db.rollback()
        return False
    db.commit()
    from .audit_service import log
    log(db, worker_id, "spatial.failed", f"{run_id}:{attempt}")
    db.commit()
    return True


def reclaim_stuck(db, redis_queue) -> dict:
    """重新投递丢失或中断的任务，返回各类回收数量。幂等且可多 worker 并发。

    - queued 且创建/入队后超过一个租约周期仍未被领取：多半是 Redis 队列
      短暂不可用或消息丢失，重新 rpush；
    - running 且租约到期：持有者进程中断，重新投递，由 claim 完成接手。
    已完成/失败的终态任务绝不重新入队。
    """
    cutoff = _lease_cutoff()
    requeued = 0
    reclaimed = 0
    pending = (
        db.query(SpatialRun)
        .filter(SpatialRun.status.in_((RunStatus.QUEUED, RunStatus.RUNNING)))
        .order_by(SpatialRun.id)
        .all()
    )
    queued_ids = set()
    try:
        queued_ids = {int(x) for x in redis_queue.lrange(JOB_QUEUE_KEY, 0, -1)}
        queue_ok = True
    except Exception:
        queue_ok = False
    for run in pending:
        if run.status == RunStatus.RUNNING:
            if run.lease_expires_at and run.lease_expires_at <= now():
                # 已在队列中等待接手则不重复投递，避免消息积压。
                if queue_ok and run.id not in queued_ids:
                    redis_queue.rpush(JOB_QUEUE_KEY, run.id)
                    reclaimed += 1
            continue
        # queued：超过一个租约周期没有被领取即视为投递丢失。
        stale = not run.lease_expires_at and (run.created_at or "") <= cutoff
        if stale and (not queue_ok or run.id not in queued_ids):
            if queue_ok:
                redis_queue.rpush(JOB_QUEUE_KEY, run.id)
                requeued += 1
    if requeued or reclaimed:
        db.commit()
    return {"requeued": requeued, "reclaimed": reclaimed}
