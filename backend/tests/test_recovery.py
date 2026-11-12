"""中断接手、租约与并发领取的验证。"""
import time

from src.constants.status import RunStatus
from src.services.run_lifecycle import claim_run, reclaim_stuck, renew_lease
from src.config.settings import JOB_QUEUE_KEY

from conftest import make_survey, make_run


def test_queued_claim_is_atomic_and_increments_fencing_token(db, fake_redis):
    survey = make_survey(db)
    run = make_run(db, survey)

    claimed = claim_run(db, run.id, "worker-A")
    assert claimed is not None
    assert claimed.status == RunStatus.RUNNING
    assert claimed.attempts == 1
    assert claimed.lease_owner == "worker-A"
    assert claimed.lease_expires_at

    # 第二个 worker 并发领取同一任务：租约未到期，必须失败。
    assert claim_run(db, run.id, "worker-B") is None
    db.refresh(run)
    assert run.attempts == 1 and run.lease_owner == "worker-A"


def test_interrupted_worker_lease_expires_and_is_taken_over(db, fake_redis):
    """计算进程中断：租约到期后，重新投递并由接手者领取，attempts 递增。"""
    survey = make_survey(db)
    run = make_run(db, survey)
    first = claim_run(db, run.id, "worker-A")
    assert first.attempts == 1

    # worker-A 崩溃，不再续租；等待租约到期。
    time.sleep(0.35)
    stats = reclaim_stuck(db, fake_redis)
    assert stats["reclaimed"] == 1
    assert fake_redis.lrange(JOB_QUEUE_KEY, 0, -1) == [str(run.id)]

    second = claim_run(db, run.id, "worker-B")
    assert second is not None
    assert second.attempts == 2
    assert second.lease_owner == "worker-B"
    # 状态仍为 running，输入快照保持建单冻结内容不变。
    assert second.status == RunStatus.RUNNING
    assert second.input_snapshot == run.input_snapshot


def test_lost_queued_job_is_requeued_when_queue_was_unavailable(db, fake_redis):
    """队列短暂不可用导致投递丢失：queued 任务被周期回收自动补投。"""
    survey = make_survey(db, code="LG-T2")
    run = make_run(db, survey)
    # 模拟消息从未进入队列（建单时 Redis 不可用）。
    assert fake_redis.lrange(JOB_QUEUE_KEY, 0, -1) == []
    time.sleep(0.35)

    stats = reclaim_stuck(db, fake_redis)
    assert stats["requeued"] == 1
    assert fake_redis.lrange(JOB_QUEUE_KEY, 0, -1) == [str(run.id)]

    # 再次扫描不应重复投递。
    assert reclaim_stuck(db, fake_redis) == {"requeued": 0, "reclaimed": 0}
    assert fake_redis.lrange(JOB_QUEUE_KEY, 0, -1) == [str(run.id)]


def test_duplicate_delivery_is_claimed_only_once(db, fake_redis):
    """重复投递（同一 id 在队列中出现多次）不会产生两次有效领取。"""
    survey = make_survey(db, code="LG-T3")
    run = make_run(db, survey)
    fake_redis.rpush(JOB_QUEUE_KEY, run.id, run.id, run.id)

    first = claim_run(db, run.id, "worker-A")
    assert first is not None
    assert claim_run(db, run.id, "worker-A") is None
    assert claim_run(db, run.id, "worker-B") is None
    db.refresh(run)
    assert run.attempts == 1


def test_lease_renewal_keeps_owner_and_rejects_stale_renewer(db, fake_redis):
    survey = make_survey(db, code="LG-T4")
    run = make_run(db, survey)
    claim_run(db, run.id, "worker-A")
    assert renew_lease(db, run.id, "worker-A", 1) is True
    # 旧 attempt / 错误持有者不能续租。
    assert renew_lease(db, run.id, "worker-A", 99) is False
    assert renew_lease(db, run.id, "worker-B", 1) is False


def test_terminal_runs_are_never_requeued(db, fake_redis):
    survey = make_survey(db, code="LG-T5")
    run = make_run(db, survey)
    claim_run(db, run.id, "worker-A")
    run.status = RunStatus.COMPLETED
    run.lease_owner = ""
    run.lease_expires_at = ""
    db.commit()
    time.sleep(0.35)
    assert reclaim_stuck(db, fake_redis) == {"requeued": 0, "reclaimed": 0}
    assert claim_run(db, run.id, "worker-B") is None
