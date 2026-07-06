-- ════════════════════════════════════════════════════════════
-- A_01_direct_query.sql  (Phương án A - "đọc thẳng" External Catalog Iceberg)
-- ────────────────────────────────────────────────────────────
-- Mục đích A: chứng minh StarRocks query DATA LAKE Iceberg TẠI CHỖ,
--             KHÔNG ETL, KHÔNG vật chất hóa. Mỗi query đọc thẳng Parquet
--             trên MinIO qua external catalog iceberg_dtm.
--
-- Khác B: A KHÔNG dùng native mart / MV. Để thấy đúng "đọc thẳng",
--         ta TẮT MV query-rewrite, ép StarRocks scan Iceberg thật.
--
-- Yêu cầu: external catalog iceberg_dtm đã tạo (B_01) + DTM đã seed (B_02).
-- Chạy:    .\scripts\demo.ps1 query-a       (xem demo.ps1)
--      hoặc docker exec -i demo_starrocks mysql -uroot < sql/A_01_direct_query.sql
-- ════════════════════════════════════════════════════════════

-- Tắt query-rewrite => bảo đảm ĐỌC THẲNG Iceberg, không lén dùng MV
SET enable_materialized_view_rewrite = false;

-- ── 0. Xác nhận đang đọc thẳng Iceberg (plan phải có IcebergScanNode) ──
EXPLAIN
SELECT province, SUM(amount) AS revenue
FROM iceberg_dtm.dtm.fact_sales
WHERE report_date >= '2026-06-01'
GROUP BY province;

-- ── A-Q1: Doanh thu theo tỉnh (đọc thẳng lake) ──
SELECT province, SUM(amount) AS revenue, COUNT(*) AS orders
FROM iceberg_dtm.dtm.fact_sales
GROUP BY province
ORDER BY revenue DESC;

-- ── A-Q2: Doanh thu 30 ngày gần nhất (partition pruning trên Iceberg) ──
SELECT report_date, SUM(amount) AS revenue
FROM iceberg_dtm.dtm.fact_sales
WHERE report_date >= date_sub(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY report_date
ORDER BY report_date;

-- ── A-Q3: Top sản phẩm (đọc thẳng lake) ──
SELECT product, SUM(amount) AS revenue
FROM iceberg_dtm.dtm.fact_sales
GROUP BY product
ORDER BY revenue DESC
LIMIT 10;

-- ── A-Q4: Federation - JOIN Iceberg (external) với native (cùng 1 query) ──
-- Chứng minh A có thể trộn data lake với bảng nội bộ StarRocks.
SELECT i.province, SUM(i.amount) AS lake_revenue
FROM iceberg_dtm.dtm.fact_sales i
GROUP BY i.province
ORDER BY lake_revenue DESC
LIMIT 5;

-- ── A-meta: time-travel / snapshot của Iceberg (đặc trưng external catalog) ──
SELECT * FROM iceberg_dtm.dtm.fact_sales$snapshots
ORDER BY committed_at DESC
LIMIT 5;
