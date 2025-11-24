"""将清洗后的数据加载到 Hive。"""

import json
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from pyhive import hive


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data_sources"
CONFIG_PATH = PROJECT_ROOT / "config" / "hive_config.yaml"


def load_config() -> Dict[str, Any]:
    """读取 Hive 连接配置。"""
    import yaml

    with open(CONFIG_PATH, "r", encoding="utf-8") as fp:
        return yaml.safe_load(fp)


def _connect() -> hive.Connection:
    cfg = load_config()
    return hive.Connection(
        host=cfg["host"],
        port=cfg["port"],
        username=cfg["username"],
        database=cfg["database"],
        transport=cfg.get("transport", "socket"),
    )


def load_table(file_path: Path, table: str, columns: str) -> None:
    """通用写入逻辑（演示用，实际环境可替换为批量写入方式）。"""
    df = pd.read_csv(file_path) if file_path.suffix == ".csv" else pd.read_json(file_path, lines=True)
    conn = _connect()
    cursor = conn.cursor()
    for _, row in df.iterrows():
        values = ", ".join(_quote_value(row[col]) for col in columns.split(", "))
        cursor.execute(f"INSERT INTO TABLE {table} ({columns}) VALUES ({values})")


def _quote_value(value: Any) -> str:
    if pd.isna(value):
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    return f"'{value}'"


def load_users() -> None:
    load_table(RAW_DIR / "cleaned_users.csv", "users_etl", "user_id, name, age, signup_dt")


def load_orders() -> None:
    load_table(RAW_DIR / "cleaned_orders.json", "orders_etl", "order_id, user_id, amount, order_time, status, dt")


if __name__ == "__main__":
    load_users()
    load_orders()
    print("Hive 写入完成")

