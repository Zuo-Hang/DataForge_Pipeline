#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "[1/2] 构建 Java ETL..."
mvn -q -f "${PROJECT_ROOT}/pom.xml" -DskipTests clean package

echo "[2/2] 运行 ETL Pipeline..."
java -jar "${PROJECT_ROOT}/target/dataforge-pipeline.jar"

echo "[附加] 指标查询示例（登录 Hive 执行 hive/metrics_queries.hql）"

