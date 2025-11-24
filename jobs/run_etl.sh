#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "[1/3] 清洗数据..."
python3 "${PROJECT_ROOT}/etl/clean.py"

echo "[2/3] 加载 Hive..."
python3 "${PROJECT_ROOT}/etl/load.py"

echo "[3/3] 指标查询示例（登录 Hive 后执行 hive/metrics_queries.hql）"

