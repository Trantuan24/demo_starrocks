-- ════════════════════════════════════════════════════════════
-- 03_validation_queries.sql
-- Các query kiểm chứng pipeline đang hoạt động đúng
-- Chạy thủ công sau khi demo đang chạy
-- ════════════════════════════════════════════════════════════

USE demo;

-- ─────────────────────────────────────────────────────────
-- 1. Kiểm tra Routine Load state
-- ─────────────────────────────────────────────────────────
SHOW ROUTINE LOAD FOR rl_sales_events\G


-- ─────────────────────────────────────────────────────────
-- 2. Tổng số row trong bảng (chạy 2 lần, kiểm tra tăng)
-- ─────────────────────────────────────────────────────────
SELECT COUNT(*) AS total_events FROM rt_sales_events;


-- ─────────────────────────────────────────────────────────
-- 3. 10 event mới nhất
-- ─────────────────────────────────────────────────────────
SELECT *
FROM rt_sales_events
ORDER BY event_time DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────
-- 4. Doanh thu theo phút (chart chính trong Superset)
-- ─────────────────────────────────────────────────────────
SELECT
    date_trunc('minute', event_time)    AS minute_time,
    COUNT(*)                            AS order_count,
    SUM(amount)                         AS revenue
FROM rt_sales_events
GROUP BY minute_time
ORDER BY minute_time DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────
-- 5. Doanh thu theo tỉnh/thành
-- ─────────────────────────────────────────────────────────
SELECT
    province,
    COUNT(*)            AS order_count,
    SUM(amount)         AS total_revenue,
    AVG(amount)         AS avg_order_value
FROM rt_sales_events
GROUP BY province
ORDER BY total_revenue DESC;


-- ─────────────────────────────────────────────────────────
-- 6. Top sản phẩm theo doanh thu
-- ─────────────────────────────────────────────────────────
SELECT
    product,
    COUNT(*)            AS order_count,
    SUM(amount)         AS total_revenue
FROM rt_sales_events
GROUP BY product
ORDER BY total_revenue DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────
-- 7. Doanh thu theo phương thức thanh toán
-- ─────────────────────────────────────────────────────────
SELECT
    payment_method,
    COUNT(*)            AS order_count,
    SUM(amount)         AS total_revenue,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
FROM rt_sales_events
GROUP BY payment_method
ORDER BY total_revenue DESC;


-- ─────────────────────────────────────────────────────────
-- 8. Kiểm tra latency ingest (ingest_time - event_time)
-- ─────────────────────────────────────────────────────────
SELECT
    AVG(TIMESTAMPDIFF(SECOND, event_time, ingest_time)) AS avg_latency_seconds,
    MAX(TIMESTAMPDIFF(SECOND, event_time, ingest_time)) AS max_latency_seconds,
    MIN(TIMESTAMPDIFF(SECOND, event_time, ingest_time)) AS min_latency_seconds
FROM rt_sales_events
WHERE ingest_time >= NOW() - INTERVAL 5 MINUTE;


-- ─────────────────────────────────────────────────────────
-- 9. Doanh thu theo giờ hôm nay
-- ─────────────────────────────────────────────────────────
SELECT
    date_trunc('hour', event_time)  AS hour_bucket,
    COUNT(*)                        AS order_count,
    SUM(amount)                     AS revenue
FROM rt_sales_events
WHERE event_time >= CURDATE()
GROUP BY hour_bucket
ORDER BY hour_bucket;


-- ─────────────────────────────────────────────────────────
-- 10. Kiểm tra StarRocks backends
-- ─────────────────────────────────────────────────────────
SHOW BACKENDS\G
