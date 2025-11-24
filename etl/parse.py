"""字段解析与校验辅助函数。"""

from typing import Any, Dict


def normalize_status(record: Dict[str, Any]) -> Dict[str, Any]:
    """标准化订单状态字段，示例函数后续可扩展。"""
    status = record.get("status", "").lower()
    record["status"] = status if status in {"paid", "pending", "refunded"} else "unknown"
    return record

