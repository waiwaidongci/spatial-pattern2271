-- 任务恢复与并发接手所需的租约/fencing 列。
-- 首次部署由 SQLAlchemy metadata 直接建出这些列；本脚本仅用于升级已存在的旧库。
-- 应用启动时 src/config/schema_bootstrap.py 也会幂等补齐同样的列。
ALTER TABLE spatial_runs ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE spatial_runs ADD COLUMN IF NOT EXISTS lease_owner VARCHAR(120) NOT NULL DEFAULT '';
ALTER TABLE spatial_runs ADD COLUMN IF NOT EXISTS lease_expires_at VARCHAR(64) NOT NULL DEFAULT '';
-- 中断后可被快速检索的待处理任务。
CREATE INDEX IF NOT EXISTS run_status_lease_idx ON spatial_runs (status, lease_expires_at);
