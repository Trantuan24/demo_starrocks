-- ════════════════════════════════════════════════════════════
-- B_03_native_and_mv.sql  (Phương án B - phần "đáng giá")
-- Vật chất hóa Iceberg DTM thành:
--   (1) Native serving mart  : serving.ads_revenue_daily  (INSERT INTO SELECT)
--   (2) Async Materialized View: serving.mv_revenue_daily (auto query-rewrite)
-- Superset/dashboard query vào đây để có latency thấp thay vì scan Iceberg.
-- ════════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS default_catalog.serving;

-- ── 1. NATIVE SERVING MART ──
-- Pre-aggregate doanh thu theo ngày/tỉnh/sản phẩm, lưu cột (columnar) local.
DROP TABLE IF EXISTS default_catalog.serving.ads_revenue_daily;
CREATE TABLE default_catalog.serving.ads_revenue_daily (
    report_date   DATE          NOT NULL,
    province      VARCHAR(64)   NOT NULL,
    product       VARCHAR(128)  NOT NULL,
    revenue       DECIMAL(38,2),
    order_count   BIGINT,
    total_qty     BIGINT
)
DUPLICATE KEY(report_date, province, product)
PARTITION BY date_trunc('month', report_date)
DISTRIBUTED BY HASH(province) BUCKETS 8
PROPERTIES ("replication_num" = "1");

-- Load từ Iceberg DTM (external catalog) -> native table
INSERT INTO default_catalog.serving.ads_revenue_daily
SELECT
    report_date,
    province,
    product,
    SUM(amount)    AS revenue,
    COUNT(*)       AS order_count,
    SUM(quantity)  AS total_qty
FROM iceberg_dtm.dtm.fact_sales
GROUP BY report_date, province, product;

-- ── 2. ASYNC MATERIALIZED VIEW trên Iceberg external catalog ──
-- StarRocks tự refresh và tự rewrite query phù hợp sang MV này.
DROP MATERIALIZED VIEW IF EXISTS default_catalog.serving.mv_revenue_daily;
CREATE MATERIALIZED VIEW default_catalog.serving.mv_revenue_daily
DISTRIBUTED BY HASH(province) BUCKETS 8
REFRESH ASYNC EVERY (INTERVAL 30 MINUTE)   -- MV trên external table bắt buộc có interval
PROPERTIES (
    "replication_num" = "1"
)
AS
SELECT
    report_date,
    province,
    product,
    SUM(amount)  AS revenue,
    COUNT(*)     AS order_count
FROM iceberg_dtm.dtm.fact_sales
GROUP BY report_date, province, product;

-- Refresh ngay (đồng bộ) để có data cho benchmark/dashboard
REFRESH MATERIALIZED VIEW default_catalog.serving.mv_revenue_daily WITH SYNC MODE;

-- ── 3. Kiểm tra ──
SELECT COUNT(*) AS mart_rows FROM default_catalog.serving.ads_revenue_daily;
SELECT * FROM information_schema.materialized_views
WHERE table_name = 'mv_revenue_daily';
