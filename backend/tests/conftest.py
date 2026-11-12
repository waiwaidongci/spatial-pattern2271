"""pytest 全局夹具：SQLite 内存库 + fakeredis，隔离且无需外部服务。

通过环境变量在导入应用配置前指向临时 artifact 目录与短租约，
使“租约到期被接手”的场景可以在测试中快速复现。
"""
import os
import sys
import tempfile
from pathlib import Path

import fakeredis
import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

_TMP = tempfile.mkdtemp(prefix="spatial-test-")
os.environ.setdefault("DATABASE_URL", f"sqlite:///{_TMP}/test.db")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("ARTIFACT_DIR", f"{_TMP}/artifacts")
os.environ.setdefault("RUN_LEASE_SECONDS", "0.3")
os.environ.setdefault("RUN_HEARTBEAT_SECONDS", "0.1")
os.environ.setdefault("RUN_SWEEP_SECONDS", "0.1")
os.environ.setdefault("SEED_DEMO", "false")
os.environ.setdefault("JWT_SECRET", "test-secret")

from src.config.database import Base, SessionLocal, engine  # noqa: E402
from src.config.schema_bootstrap import ensure_run_schema  # noqa: E402
from src.models import (  # noqa: E402
    GridCell,
    NeighborEdge,
    SpatialRun,
    SpatialSurvey,
)
from src.services import run_service  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def _schema():
    Base.metadata.create_all(engine)
    ensure_run_schema()
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture
def db():
    with SessionLocal() as session:
        yield session


@pytest.fixture(autouse=True)
def _clean_tables():
    """每个测试之间清空业务表，避免文件 SQLite 跨测试串数据。"""
    with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            conn.execute(table.delete())
    yield
    with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            conn.execute(table.delete())


@pytest.fixture
def fake_redis(monkeypatch):
    client = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(run_service, "queue", client)
    import src.worker as worker_module
    monkeypatch.setattr(worker_module, "queue", client)
    return client


def make_survey(db, code="LG-T1", constant=False):
    """构造 4 格 4 边（2×2 rook 图）的最小研究；constant=True 为常量输入。"""
    survey = SpatialSurvey(
        code=code,
        name=f"测试观测期 {code}",
        period="2026-11-10",
        description="测试用最小图",
        source="test.csv",
        cell_size_m=250,
    )
    db.add(survey)
    db.flush()
    values = [60.0, 62.0, 64.0, 66.0] if not constant else [62.0, 62.0, 62.0, 62.0]
    for idx, value in enumerate(values):
        db.add(
            GridCell(
                survey_id=survey.id,
                code=f"G{idx + 1:02}",
                row=idx // 2,
                col=idx % 2,
                value=value,
                note="测试格",
            )
        )
    for a, b in [("G01", "G02"), ("G01", "G03"), ("G02", "G04"), ("G03", "G04")]:
        db.add(NeighborEdge(survey_id=survey.id, a=a, b=b))
    db.flush()
    return survey


def make_run(db, survey, seed=2026, permutations=499, name="测试研究"):
    snapshot = run_service.create_snapshot(db, survey)
    run = SpatialRun(
        name=name,
        survey_id=survey.id,
        permutations=permutations,
        seed=seed,
        input_snapshot=snapshot,
        status="queued",
        owner="测试研究员",
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run
