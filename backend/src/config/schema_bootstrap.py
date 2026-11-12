"""幂等的运行时 schema 补齐。

首次部署由 SQLAlchemy metadata 建表；对已存在的旧库（升级前创建的
spatial_runs 缺少租约/fencing 列），启动时做一次轻量、幂等的列补齐，
避免要求手工迁移即可获得中断接管能力。
"""
from sqlalchemy import inspect, text
from .database import engine

_EXTRA_COLUMNS = {
    "attempts": "INTEGER NOT NULL DEFAULT 0",
    "lease_owner": "VARCHAR(120) NOT NULL DEFAULT ''",
    "lease_expires_at": "VARCHAR(64) NOT NULL DEFAULT ''",
}


def ensure_run_schema():
    inspector = inspect(engine)
    if "spatial_runs" not in inspector.get_table_names():
        return
    existing = {col["name"] for col in inspector.get_columns("spatial_runs")}
    dialect = engine.dialect.name
    type_sql = "sqlite" if dialect == "sqlite" else None
    with engine.begin() as conn:
        for name, pg_type in _EXTRA_COLUMNS.items():
            if name in existing:
                continue
            col_type = "INTEGER" if type_sql == "sqlite" and name == "attempts" else pg_type
            conn.execute(text(f"ALTER TABLE spatial_runs ADD COLUMN {name} {col_type}"))
        # 部分方言 ADD COLUMN ... DEFAULT 不会回填既有行，统一兜底为非空默认值。
        conn.execute(text("UPDATE spatial_runs SET attempts = 0 WHERE attempts IS NULL"))
        conn.execute(text("UPDATE spatial_runs SET lease_owner = '' WHERE lease_owner IS NULL"))
        conn.execute(
            text("UPDATE spatial_runs SET lease_expires_at = '' WHERE lease_expires_at IS NULL")
        )
