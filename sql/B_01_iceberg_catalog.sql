-- ════════════════════════════════════════════════════════════
-- B_01_iceberg_catalog.sql  (Phương án B)
-- Tạo Iceberg External Catalog trỏ vào Iceberg REST catalog + MinIO.
-- StarRocks đọc/ghi Iceberg qua catalog này mà KHÔNG copy data.
--
-- __MINIO_USER__ / __MINIO_PASSWORD__ được init_demo_b.sh thay bằng giá trị
-- từ biến môi trường trước khi chạy.
-- ════════════════════════════════════════════════════════════

CREATE EXTERNAL CATALOG IF NOT EXISTS iceberg_dtm
PROPERTIES (
    "type"                            = "iceberg",
    "iceberg.catalog.type"            = "rest",
    "iceberg.catalog.uri"             = "http://iceberg-rest:8181",
    "iceberg.catalog.warehouse"       = "s3://warehouse/",
    "aws.s3.endpoint"                 = "http://minio:9000",
    "aws.s3.access_key"               = "__MINIO_USER__",
    "aws.s3.secret_key"               = "__MINIO_PASSWORD__",
    "aws.s3.region"                   = "us-east-1",
    "aws.s3.enable_ssl"               = "false",
    "aws.s3.enable_path_style_access" = "true"
);

-- Kiểm tra catalog đã tạo
SHOW CATALOGS;
