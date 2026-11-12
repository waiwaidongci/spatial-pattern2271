"""幂等发布、旧计算迟到、完整结果文件核对与无效输入的验证。"""
import json
import time

from src.constants.status import RunStatus
from src.services.analysis_service import analyze_spatial
from src.services.artifact_service import artifact_path, verify_artifact
from src.services.run_lifecycle import (
    claim_run,
    publish_completed,
    publish_failed,
    reclaim_stuck,
)

from conftest import make_survey, make_run


def _compute(run):
    return analyze_spatial(run.input_snapshot, run.permutations, run.seed)


def test_completion_visible_only_after_artifact_matches_result(db, fake_redis):
    survey = make_survey(db)
    run = make_run(db, survey)
    claim_run(db, run.id, "worker-A")
    result = _compute(run)

    assert publish_completed(db, run.id, "worker-A", 1, result) is True
    db.refresh(run)
    assert run.status == RunStatus.COMPLETED
    assert run.result == result
    # 完整结果文件已落盘且与库内结果一致，完成状态才可见。
    path = artifact_path(run.id)
    assert path.is_file()
    assert json.loads(path.read_text(encoding="utf-8")) == result
    assert verify_artifact(run.id, run.result) is True


def test_stale_compute_cannot_overwrite_successor_result(db, fake_redis):
    """旧持有者迟到的成功结果不能覆盖接手者已发布的结果。"""
    survey = make_survey(db, code="LG-T6")
    run = make_run(db, survey)
    claim_run(db, run.id, "worker-A")  # attempt 1，随后“崩溃”

    time.sleep(0.35)
    reclaim_stuck(db, fake_redis)
    successor = claim_run(db, run.id, "worker-B")  # attempt 2
    assert successor.attempts == 2
    successor_result = _compute(run)
    assert publish_completed(db, run.id, "worker-B", 2, successor_result) is True

    # worker-A 复活，带着 attempt 1 的旧结果迟到。
    stale_result = dict(successor_result, evidence_count=999)
    assert publish_completed(db, run.id, "worker-A", 1, stale_result) is False
    db.refresh(run)
    assert run.status == RunStatus.COMPLETED
    assert run.attempts == 2
    assert run.lease_owner == "worker-B" or run.lease_owner == ""
    assert run.result == successor_result
    assert run.result["evidence_count"] != 999
    assert verify_artifact(run.id, run.result) is True


def test_stale_failure_cannot_overwrite_successor(db, fake_redis):
    """旧持有者迟到的失败回报不能把接手者的成功打成 failed。"""
    survey = make_survey(db, code="LG-T7")
    run = make_run(db, survey)
    claim_run(db, run.id, "worker-A")
    time.sleep(0.35)
    reclaim_stuck(db, fake_redis)
    claim_run(db, run.id, "worker-B")
    assert publish_completed(db, run.id, "worker-B", 2, _compute(run)) is True

    assert publish_failed(db, run.id, "worker-A", 1, "迟到的失败") is False
    db.refresh(run)
    assert run.status == RunStatus.COMPLETED
    assert run.error == ""


def test_tampered_artifact_blocks_completion(db, fake_redis, monkeypatch):
    """结果文件无法与研究结果核对一致时，拒绝进入 completed。"""
    survey = make_survey(db, code="LG-T8")
    run = make_run(db, survey)
    claim_run(db, run.id, "worker-A")
    result = _compute(run)

    import src.services.artifact_service as artifact_service

    def broken_write(run_id, payload):
        raise RuntimeError("完整结果文件与研究结果不一致")

    monkeypatch.setattr(artifact_service, "write_verified_artifact", broken_write)

    import pytest
    with pytest.raises(RuntimeError):
        publish_completed(db, run.id, "worker-A", 1, result)
    db.refresh(run)
    # 状态未翻转为完成，阅读端不会看到“已完成”。
    assert run.status == RunStatus.RUNNING
    assert run.result == {}


def test_constant_grid_fails_as_before_and_publishes_failure(db, fake_redis):
    """常量网格等无效输入照常失败，失败同样走幂等发布并记录原因。"""
    survey = make_survey(db, code="LG-T9", constant=True)
    run = make_run(db, survey)

    import src.worker as worker_module
    outcome = worker_module.process_job(run.id, "worker-A")
    assert outcome == "failed"
    db.refresh(run)
    assert run.status == RunStatus.FAILED
    assert "恒定" in run.error or "方差" in run.error
    # 失败任务不会被回收机制重新投递。
    time.sleep(0.35)
    assert reclaim_stuck(db, fake_redis) == {"requeued": 0, "reclaimed": 0}


def test_failure_is_also_fenced_against_stale_owner(db, fake_redis):
    survey = make_survey(db, code="LG-T10", constant=True)
    run = make_run(db, survey)
    claim_run(db, run.id, "worker-A")
    # 错误 attempt 的失败回报直接被拒绝。
    assert publish_failed(db, run.id, "worker-A", 7, "x") is False
    db.refresh(run)
    assert run.status == RunStatus.RUNNING


def test_successful_run_uses_frozen_snapshot_after_takeover(db, fake_redis):
    """接手者沿用建单冻结的网格与邻接，来源数据后续变化不影响结果。"""
    survey = make_survey(db, code="LG-T11")
    run = make_run(db, survey, seed=4242)
    frozen = json.loads(json.dumps(run.input_snapshot))
    claim_run(db, run.id, "worker-A")
    time.sleep(0.35)
    reclaim_stuck(db, fake_redis)
    claim_run(db, run.id, "worker-B")

    expected = analyze_spatial(frozen, run.permutations, run.seed)
    assert publish_completed(db, run.id, "worker-B", 2, expected) is True
    db.refresh(run)
    assert run.result["input_hash"] == expected["input_hash"]
    assert run.input_snapshot == frozen
