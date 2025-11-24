-- 每日订单金额与数量
SELECT
  dt,
  SUM(amount) AS total_amount,
  COUNT(*) AS order_count
FROM orders_etl
GROUP BY dt
ORDER BY dt DESC;

-- DAU 指标
SELECT
  dt,
  COUNT(DISTINCT user_id) AS dau
FROM orders_etl
GROUP BY dt
ORDER BY dt DESC;

