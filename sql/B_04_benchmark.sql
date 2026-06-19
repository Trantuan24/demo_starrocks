-- ════════════════════════════════════════════════════════════
-- B_04_benchmark.sql  (Phương án B)
-- Cùng một câu hỏi BI, chạy trên 3 nguồn để so sánh:
--   (A) External Iceberg  : scan file Parquet trên MinIO mỗi lần   -> baseline
--   (B) Native mart       : đọc bảng columnar local đã pre-aggregate -> nhanh
--   (C) Materialized View : optimizer tự rewrite query gốc sang MV
--
-- Dùng scripts/benchmark_b.py để đo latency tự động nhiều lần.
-- Chạy tay: bật profiling rồi xem EXPLAIN / query log.
-- ════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────
-- Q1: Doanh thu theo tỉnh (toàn bộ thời gian)
-- ───────────────────────────────────────────────

-- (A) External Iceberg
SELECT province, SUM(amount) AS revenue, COUNT(*) AS orders
FROM iceberg_dtm.dtm.fact_sales
GROUP BY province
ORDER BY revenue DESC;

-- (B) Native mart
SELECT province, SUM(revenue) AS revenue, SUM(order_count) AS orders
FROM default_catalog.serving.ads_revenue_daily
GROUP BY province
ORDER BY revenue DESC;

-- ───────────────────────────────────────────────
-- Q2: Doanh thu theo ngày trong 30 ngày gần nhất (partition pruning)
-- ───────────────────────────────────────────────

-- (A) External Iceberg
SELECT report_date, SUM(amount) AS revenue
FROM iceberg_dtm.dtm.fact_sales
WHERE report_date >= date_sub(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY report_date
ORDER BY report_date;

-- (B) Native mart
SELECT report_date, SUM(revenue) AS revenue
FROM default_catalog.serving.ads_revenue_daily
WHERE report_date >= date_sub(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY report_date
ORDER BY report_date;

-- ───────────────────────────────────────────────
-- Q3: Top sản phẩm theo doanh thu
-- ───────────────────────────────────────────────

-- (A) External Iceberg
SELECT product, SUM(amount) AS revenue
FROM iceberg_dtm.dtm.fact_sales
GROUP BY product
ORDER BY revenue DESC
LIMIT 10;

-- (B) Native mart
SELECT product, SUM(revenue) AS revenue
FROM default_catalog.serving.ads_revenue_daily
GROUP BY product
ORDER BY revenue DESC
LIMIT 10;

-- ───────────────────────────────────────────────
-- Chứng minh query-rewrite của Materialized View:
-- Query gốc viết trên Iceberg, optimizer tự rewrite sang mv_revenue_daily.
-- Tìm dòng có 'mv_revenue_daily' trong output để xác nhận MV được dùng.
-- ───────────────────────────────────────────────
EXPLAIN
SELECT province, SUM(amount) AS revenue
FROM iceberg_dtm.dtm.fact_sales
GROUP BY province;
