## DataForge Pipeline

DataForge Pipeline 旨在通过一个简化版 “Hive + ETL + 指标体系” 案例，串联数据采集、清洗、入仓和指标展示的端到端流程。最新版本完全基于 Java + Maven 实现，便于落地到企业常见的大数据平台。

### 当前 MVP 目标
- 使用 Java + Maven 构建可运行的 ETL，可读取 CSV/JSON 并执行清洗
- 以 Hive 作为仓库，完成建表、分区管理与 JDBC 写入
- 构建 `DAU`、订单量等基础指标的 SQL
- 借助 `jobs/run_etl.sh` 一键构建与运行 ETL，输出指标结果（Hive 中查询）

### 目录结构
```
├── config/                 # 连接配置与常量
├── data_sources/           # 原始与清洗后的样例数据
├── docs/                   # 数据流与指标文档
├── src/                    # Java 清洗、解析、加载脚本
├── hive/                   # Hive 建表与指标 SQL
├── jobs/                   # 任务编排脚本与后续调度入口
└── README.md
```

### 使用方式
1. 准备 Hive 环境并执行 `hive/create_tables.hql` 建表
2. 确保本地已安装 JDK 17 与 Maven 3.9+，运行 `bash jobs/run_etl.sh`（自动构建 + 执行 Java ETL）
3. 登录 Hive 执行 `hive/metrics_queries.hql` 验证 DAU / 订单指标

### 关键模块
- `com.dataforge.pipeline.clean`: `UserCleaner`、`OrderCleaner` 使用 OpenCSV + Jackson 完成清洗、缺失值填充、字段标准化。
- `com.dataforge.pipeline.load.HiveLoader`: 基于 Hive JDBC 将清洗结果写入 `users_etl`、`orders_etl`。
- `com.dataforge.pipeline.App`: 串联清洗与装载逻辑，可直接通过 `java -jar target/dataforge-pipeline.jar` 运行。
- `jobs/run_etl.sh`: 一键打包 + 运行脚本，方便演示或接入调度器。

### 下一步计划
1. 引入调度器（Airflow / DolphinScheduler）编排 Java 作业
2. 扩展指标层，增加更多主题域与分层表
3. 增加数据质量校验、监控与回溯机制
4. 补充文档，说明数据流、字段定义与指标口径

