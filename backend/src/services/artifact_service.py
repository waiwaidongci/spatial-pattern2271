"""完整结果文件（artifact）的原子写入与核对。

完成状态对外可见的前提是：artifact 已落盘，且其内容与数据库中的研究结果
逐项一致。写临时文件 -> 校验 -> 原子替换，避免半成品文件被读取。
"""
import json
import os
from pathlib import Path
from ..config.settings import ARTIFACT_DIR

def artifact_dir() -> Path:
    path = Path(ARTIFACT_DIR)
    path.mkdir(parents=True, exist_ok=True)
    return path

def artifact_path(run_id) -> Path:
    return artifact_dir() / f"run-{run_id}.json"

def _load(path: Path):
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)

def verify_artifact(run_id, expected) -> bool:
    """文件存在、可解析为 JSON，且与期望结果完整一致。"""
    path = artifact_path(run_id)
    try:
        if not path.is_file():
            return False
        return _load(path) == expected
    except (OSError, ValueError):
        return False

def write_verified_artifact(run_id, result) -> Path:
    """先写临时文件并自检，再原子替换正式文件；返回正式文件路径。"""
    target = artifact_path(run_id)
    payload = json.dumps(result, ensure_ascii=False)
    tmp = target.with_name(f".run-{run_id}.{os.getpid()}.tmp")
    try:
        with tmp.open("w", encoding="utf-8") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        # 落盘后立刻回读核对，半成品/损坏文件不会晋升为正式产物。
        if _load(tmp) != result:
            raise RuntimeError("结果文件自检不一致")
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
    if not verify_artifact(run_id, result):
        raise RuntimeError("完整结果文件与研究结果不一致")
    return target
