-- ════════════════════════════════════════════════════════════
-- 01_create_database_and_table.sql
-- Tạo database demo và bảng rt_sales_events
-- ════════════════════════════════════════════════════════════

-- Tạo database
CREATE DATABASE IF NOT EXISTS demo;

USE demo;

-- Tạo bảng realtime sales events
-- DUPLICATE KEY: append-only event stream, không deduplicate
-- Nếu cần upsert/CDC → đổi sang PRIMARY KEY(order_id)
CREATE TABLE IF NOT EXISTS rt_sales_events (
    event_time      DATETIME        NOT NULL    COMMENT 'Thời gian event từ producer',
    order_id        BIGINT          NOT NULL    COMMENT 'ID đơn hàng, unique trong demo',
    province        VARCHAR(64)                 COMMENT 'Tỉnh/thành phố',
    product         VARCHAR(128)                COMMENT 'Sản phẩm',
    amount          DECIMAL(18, 2)              COMMENT 'Doanh thu (VND)',
    payment_method  VARCHAR(32)                 COMMENT 'Phương thức thanh toán',
    ingest_time     DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời điểm StarRocks ingest'
)
DUPLICATE KEY(event_time, order_id)
PARTITION BY date_trunc('day', event_time)
DISTRIBUTED BY HASH(order_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1"   -- Single node demo, production dùng >= 3
);

-- Verify
SHOW TABLES;
DESCRIBE rt_sales_events;
