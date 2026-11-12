"""并发竞争下的 CAS 语义：多个会话同时领取，只有一个成功。"""
from src.config.database import SessionLocal
from src.constants.status import RunStatus
from src.services.run_lifecycle import claim_run

from conftest import make_survey, make_run


def test_concurrent_claims_only_one_wins(db, fake_redis):
    survey = make_survey(db, code="LG-C1")
    run = make_run(db, survey)

    winners = []
    for owner in ("worker-A", "worker-B", "worker-C"):
        with SessionLocal() as session:
            claimed = claim_run(session, run.id, owner)
            if claimed is not None:
                winners.append((owner, claimed.attempts))

    assert winners == [("worker-A", 1)]
    db.refresh(run)
    assert run.status == RunStatus.RUNNING
    assert run.attempts == 1
    assert run.lease_owner == "worker-A"


def test_concurrent_takeover_after_expiry_only_one_wins(db, fake_redis):
    import time
    survey = make_survey(db, code="LG-C2")
    run = make_run(db, survey)
    with SessionLocal() as session:
        first = claim_run(session, run.id, "worker-A")
        assert first.attempts == 1
    time.sleep(0.35)

    winners = []
    for owner in ("worker-B", "worker-C"):
        with SessionLocal() as session:
            claimed = claim_run(session, run.id, owner)
            if claimed is not None:
                winners.append((owner, claimed.attempts))

    assert len(winners) == 1
    assert winners[0][1] == 2  # fencing token 只递增一次
    db.refresh(run)
    assert run.attempts == 2
    assert run.lease_owner == winners[0][0]


def test_takeover_writes_reclaim_audit(db, fake_redis):
    import time
    from src.models import Audit
    survey = make_survey(db, code="LG-C3")
    run = make_run(db, survey)
    claim_run(db, run.id, "worker-A")
    time.sleep(0.35)

    successor = claim_run(db, run.id, "worker-B")
    assert successor is not None and successor.attempts == 2
    audit = (
        db.query(Audit)
        .filter(Audit.action == "spatial.reclaimed")
        .order_by(Audit.id.desc())
        .first()
    )
    assert audit is not None
    assert audit.actor == "worker-B"
    assert str(run.id) in audit.target
