"""数据清洗脚本，负责生成 Hive 入仓前的 CSV / JSON。"""

from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data_sources"
OUTPUT_USERS = RAW_DIR / "cleaned_users.csv"
OUTPUT_ORDERS = RAW_DIR / "cleaned_orders.json"


def clean_users(source: Path = RAW_DIR / "users.csv") -> pd.DataFrame:
    """清洗用户数据：去重 + 填充缺失值。"""
    df = pd.read_csv(source)
    df = df.drop_duplicates(subset=["user_id"])
    df["age"] = df["age"].fillna(df["age"].median())
    df["signup_dt"] = pd.to_datetime(df["signup_dt"])
    return df


def clean_orders(source: Path = RAW_DIR / "orders.json") -> pd.DataFrame:
    """清洗订单数据：时间格式化 + 过滤异常金额。"""
    df = pd.read_json(source, lines=True)
    df["order_time"] = pd.to_datetime(df["order_time"])
    df = df[df["amount"] >= 0].copy()
    df["dt"] = df["order_time"].dt.strftime("%Y-%m-%d")
    return df


def persist_outputs(users_df: pd.DataFrame, orders_df: pd.DataFrame) -> None:
    """将清洗结果写回文件，用于后续加载。"""
    users_df.to_csv(OUTPUT_USERS, index=False)
    orders_df.to_json(OUTPUT_ORDERS, orient="records", lines=True, date_format="iso")


if __name__ == "__main__":
    users = clean_users()
    orders = clean_orders()
    persist_outputs(users, orders)
    print(f"清洗完成：{OUTPUT_USERS.name}, {OUTPUT_ORDERS.name}")

