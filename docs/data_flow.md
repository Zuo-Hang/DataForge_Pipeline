# 数据流说明

1. `data_sources/` 中维护原始 CSV / JSON 样例，模拟用户与订单事件流。
2. `etl/clean.py` 负责执行 Pandas 清洗，输出 `cleaned_users.csv`、`cleaned_orders.json`。
3. `etl/load.py` 将清洗结果加载到 Hive 分区表，为指标层提供统一口径。
4. `hive/metrics_queries.hql` 定义 DAU、订单金额等指标查询。
5. 运行 `jobs/run_etl.sh` 串联上述步骤，最终在日志或控制台展示指标结果。

