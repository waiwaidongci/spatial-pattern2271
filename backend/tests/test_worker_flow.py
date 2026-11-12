"""worker 端到端行为：正常完成、重复投递不重复发布、建单时队列不可用。"""
from src.constants.status import RunStatus
from src.config.settings import JOB_QUEUE_KEY
from src.services.analysis_service import analyze_spatial
from src.services.artifact_service import verify_artifact

import src.worker as worker_module
from conftest import make_survey, make_run


def test_process_job_completes_and_writes_artifact(db, fake_redis):
    survey = make_survey(db)
    run = make_run(db, survey)
    fake_redis.rpush(JOB_QUEUE_KEY, run.id)

    outcome = worker_module.process_job(run.id, "worker-A")
    assert outcome == "completed"
    db.refresh(run)
    assert run.status == RunStatus.COMPLETED
    assert run.attempts == 1
    expected = analyze_spatial(run.input_snapshot, run.permutations, run.seed)
    assert run.result == expected
    assert verify_artifact(run.id, expected)


def test_duplicate_job_messages_do_not_publish_twice(db, fake_redis):
    survey = make_survey(db, code="LG-T12")
    run = make_run(db, survey)
    # 同一条任务被投递多次（重复投递/多 worker 竞争）。
    fake_redis.rpush(JOB_QUEUE_KEY, run.id, run.id)

    assert worker_module.process_job(run.id, "worker-A") == "completed"
    assert worker_module.process_job(run.id, "worker-B") == "skipped"
    db.refresh(run)
    assert run.status == RunStatus.COMPLETED
    assert run.attempts == 1
    assert run.lease_owner in ("", "worker-A")


def test_worker_sweep_then_reclaim_processes_interrupted_run(db, fake_redis):
    """完整中断接手链路：领取后崩溃 -> 扫描重投 -> 新 worker 接手完成。"""
    import time
    survey = make_survey(db, code="LG-T13")
    run = make_run(db, survey)
    from src.services.run_lifecycle import claim_run
    claim_run(db, run.id, "worker-A")
    time.sleep(0.35)

    worker_module.sweep("worker-B")
    queued = fake_redis.lrange(JOB_QUEUE_KEY, 0, -1)
    assert str(run.id) in queued

    outcome = worker_module.process_job(run.id, "worker-B")
    assert outcome == "completed"
    db.refresh(run)
    assert run.status == RunStatus.COMPLETED
    assert run.attempts == 2
    assert verify_artifact(run.id, run.result)


def test_create_run_when_queue_down_stays_recoverable(db, fake_redis, monkeypatch):
    """建单时队列短暂不可用：研究正常受理为 queued，恢复后自动补投并完成。"""
    from src.services import run_service
    from src.schemas.run import RunInput

    class BrokenRedis:
        def rpush(self, *a, **k):
            raise ConnectionError("redis down")
        def lrange(self, *a, **k):
            raise ConnectionError("redis down")

    survey = make_survey(db, code="LG-T14")
    payload = RunInput(name="队列故障期建单", survey_id=survey.id, permutations=499, seed=2026)

    class User:
        name = "测试研究员"
        email = "analyst@longge.demo"

    monkeypatch.setattr(run_service, "queue", BrokenRedis())
    row = run_service.create_run(db, payload, User())
    assert row["status"] == RunStatus.QUEUED
    rid = row["id"]

    # 队列恢复：worker 扫描补投，随后正常处理。
    monkeypatch.setattr(run_service, "queue", fake_redis)
    monkeypatch.setattr(worker_module, "queue", fake_redis)
    import time
    time.sleep(0.35)
    worker_module.sweep("worker-A")
    assert str(rid) in fake_redis.lrange(JOB_QUEUE_KEY, 0, -1)
    assert worker_module.process_job(rid, "worker-A") == "completed"
    from src.models import SpatialRun
    finished = db.get(SpatialRun, rid)
    assert finished.status == RunStatus.COMPLETED
    assert verify_artifact(rid, finished.result)
