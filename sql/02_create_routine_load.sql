-- ════════════════════════════════════════════════════════════
-- 02_create_routine_load.sql
-- Tạo Routine Load: StarRocks tự đọc Kafka topic sales_events
-- ════════════════════════════════════════════════════════════

USE demo;

-- Tạo Routine Load
-- Lưu ý:
--   kafka_broker_list: phải là hostname mà StarRocks container resolve được
--   kafka:9092 → internal Docker network (PLAINTEXT listener)
--   property.kafka_default_offsets = OFFSET_BEGINNING → đọc từ đầu topic (demo only)
CREATE ROUTINE LOAD demo.rl_sales_events ON rt_sales_events
COLUMNS(event_time, order_id, province, product, amount, payment_method)
PROPERTIES
(
    "desired_concurrent_number" = "1",
    "max_batch_interval"        = "5",    -- flush mỗi 5 giây
    "max_error_number"          = "100",  -- tối đa 100 lỗi parse trước khi tự pause
    "format"                    = "json",
    "jsonpaths"                 = "[\"$.event_time\", \"$.order_id\", \"$.province\", \"$.product\", \"$.amount\", \"$.payment_method\"]",
    "strip_outer_array"         = "false"
)
FROM KAFKA
(
    "kafka_broker_list"                 = "kafka:9092",
    "kafka_topic"                       = "sales_events",
    "property.kafka_default_offsets"    = "OFFSET_BEGINNING"
);

-- Verify trạng thái Routine Load
-- Sau khi tạo, state sẽ là RUNNING
SHOW ROUTINE LOAD FOR rl_sales_events\G
