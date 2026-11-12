import os
DATABASE_URL=os.environ.get("DATABASE_URL","postgresql+psycopg://spatial:spatial@db/spatial")
REDIS_URL=os.environ.get("REDIS_URL","redis://redis:6379/0")
JWT_SECRET=os.environ.get("JWT_SECRET","local-spatial-pattern-change-this-key")
SEED_DEMO=os.environ.get("SEED_DEMO","true").lower()=="true"
ARTIFACT_DIR=os.environ.get("ARTIFACT_DIR","/artifacts")
# 租约与接管时序，均可经环境变量覆盖（测试使用短租约）。
RUN_LEASE_SECONDS=float(os.environ.get("RUN_LEASE_SECONDS","30"))
RUN_HEARTBEAT_SECONDS=float(os.environ.get("RUN_HEARTBEAT_SECONDS","10"))
RUN_SWEEP_SECONDS=float(os.environ.get("RUN_SWEEP_SECONDS","5"))
JOB_QUEUE_KEY="spatial:jobs"
