from datetime import datetime, timedelta, timezone
def now():
    return datetime.now(timezone.utc).isoformat()
def parse_ts(value):
    """解析 ISO8601 时间字符串；空值或非法值返回 None。"""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt
def iso_after(value, seconds):
    """在给定 ISO8601 时间上加减秒数，返回新的 ISO8601 字符串。"""
    dt = parse_ts(value) or datetime.now(timezone.utc)
    return (dt + timedelta(seconds=float(seconds))).isoformat()
