# 指标定义

## DAU (Daily Active Users)
- **定义**：当日产生订单的去重用户数。
- **来源**：`orders_etl` 表的 `user_id` 字段。
- **计算 SQL**：
  ```
  SELECT dt, COUNT(DISTINCT user_id) AS dau
  FROM orders_etl
  GROUP BY dt;
  ```

## total_amount / order_count
- **定义**：每日总订单金额与订单数量。
- **来源**：`orders_etl` 表。
- **计算 SQL**：
  ```
  SELECT dt,
         SUM(amount) AS total_amount,
         COUNT(*) AS order_count
  FROM orders_etl
  GROUP BY dt;
  ```

