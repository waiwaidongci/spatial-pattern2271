from sqlalchemy import String,Text,JSON,ForeignKey,Index
from sqlalchemy.orm import Mapped,mapped_column
from ..config.database import Base
from ..utils.clock import now
class SpatialRun(Base):
    __tablename__="spatial_runs"
    __table_args__=(Index("run_status_lease_idx","status","lease_expires_at"),)
    id:Mapped[int]=mapped_column(primary_key=True)
    name:Mapped[str]
    survey_id:Mapped[int]=mapped_column(ForeignKey("spatial_surveys.id"))
    permutations:Mapped[int]
    seed:Mapped[int]
    input_snapshot:Mapped[dict]=mapped_column(JSON)
    status:Mapped[str]=mapped_column(default="queued")
    result:Mapped[dict]=mapped_column(JSON,default=dict)
    error:Mapped[str]=mapped_column(Text,default="")
    owner:Mapped[str]
    # 领取/接管凭据：attempts 单调递增，作为写入结果的 fencing token；
    # lease_owner 标识当前持有者，lease_expires_at 为租约到期时刻。
    attempts:Mapped[int]=mapped_column(default=0)
    lease_owner:Mapped[str]=mapped_column(String(120),default="")
    lease_expires_at:Mapped[str]=mapped_column(default="")
    created_at:Mapped[str]=mapped_column(default=now)
