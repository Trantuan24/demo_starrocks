-- ════════════════════════════════════════════════════════════
-- B_02_seed_dtm.sql  (Phương án B)
-- Tạo bảng Iceberg "DTM giả" (fact_sales) và bơm ~1,000,000 dòng mock.
--
-- Điểm hay: KHÔNG cần Spark/NiFi. StarRocks tự ghi thẳng vào Iceberg
-- (data file Parquet nằm trên MinIO, metadata qua REST catalog).
--
-- Cách sinh dữ liệu: dùng bảng "digits" (0-9) cross join 6 lần -> 10^6 dòng,
-- mỗi tổ hợp cho một số n duy nhất 0..999999, rồi map sang các chiều.
-- ════════════════════════════════════════════════════════════

-- ── 1. Bảng sinh số (numbers generator) trong default_catalog ──
CREATE DATABASE IF NOT EXISTS default_catalog._gen;

DROP TABLE IF EXISTS default_catalog._gen.digits;
CREATE TABLE default_catalog._gen.digits (d TINYINT)
DUPLICATE KEY(d)
DISTRIBUTED BY HASH(d) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO default_catalog._gen.digits VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

-- ── 2. Tạo namespace + bảng Iceberg "DTM" ──
CREATE DATABASE IF NOT EXISTS iceberg_dtm.dtm;

DROP TABLE IF EXISTS iceberg_dtm.dtm.fact_sales;
-- Lưu ý Iceberg trong StarRocks: cột partition (report_date) phải ở CUỐI column defs.
CREATE TABLE iceberg_dtm.dtm.fact_sales (
    order_id        BIGINT          COMMENT 'ID đơn hàng',
    event_time      DATETIME        COMMENT 'Thời điểm phát sinh',
    province        VARCHAR(64)     COMMENT 'Tỉnh/thành',
    product         VARCHAR(128)    COMMENT 'Sản phẩm',
    payment_method  VARCHAR(32)     COMMENT 'Phương thức thanh toán',
    amount          DECIMAL(18,2)   COMMENT 'Doanh thu (VND)',
    quantity        INT             COMMENT 'Số lượng',
    report_date     DATE            COMMENT 'Ngày báo cáo (partition, để cuối)'
)
PARTITION BY (report_date);

-- ── 3. Seed ~1,000,000 dòng, trải đều trên 90 ngày gần nhất ──
INSERT INTO iceberg_dtm.dtm.fact_sales
SELECT
    order_id,
    event_time,
    province,
    product,
    payment_method,
    amount,
    quantity,
    to_date(event_time)                                            AS report_date
FROM (
    SELECT
        n                                                          AS order_id,
        hours_sub(NOW(), CAST(n % 2160 AS INT))                    AS event_time,  -- 2160 = 90*24 giờ
        CASE n % 6
            WHEN 0 THEN 'Hanoi'  WHEN 1 THEN 'HCM'      WHEN 2 THEN 'Da Nang'
            WHEN 3 THEN 'Can Tho' WHEN 4 THEN 'Hai Phong' ELSE 'Binh Duong'
        END                                                        AS province,
        CASE CAST(floor(n / 6) AS BIGINT) % 5
            WHEN 0 THEN 'Data Package' WHEN 1 THEN 'Voice Package' WHEN 2 THEN 'Device'
            WHEN 3 THEN 'Service Fee'  ELSE 'Roaming'
        END                                                        AS product,
        CASE n % 4
            WHEN 0 THEN 'CARD' WHEN 1 THEN 'CASH' WHEN 2 THEN 'BANKING' ELSE 'WALLET'
        END                                                        AS payment_method,
        CAST(50000 + (n % 950) * 1000 AS DECIMAL(18,2))            AS amount,
        CAST(1 + (n % 5) AS INT)                                   AS quantity
    FROM (
        SELECT
            (d0.d + d1.d*10 + d2.d*100 + d3.d*1000 + d4.d*10000 + d5.d*100000) AS n
        FROM default_catalog._gen.digits d0
        CROSS JOIN default_catalog._gen.digits d1
        CROSS JOIN default_catalog._gen.digits d2
        CROSS JOIN default_catalog._gen.digits d3
        CROSS JOIN default_catalog._gen.digits d4
        CROSS JOIN default_catalog._gen.digits d5
    ) nums
) src;

-- ── 4. Kiểm tra ──
SELECT COUNT(*) AS dtm_rows FROM iceberg_dtm.dtm.fact_sales;
SELECT MIN(report_date) AS min_date, MAX(report_date) AS max_date FROM iceberg_dtm.dtm.fact_sales;
