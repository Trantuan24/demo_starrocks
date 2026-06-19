# R&D: Đánh giá StarRocks cho lớp BI Serving và Query Acceleration trên Lakehouse Iceberg

**Phiên bản:** 2.0  
**Ngày tạo:** 2026-06-14  
**Ngày cập nhật:** 2026-06-15  
**Phạm vi:** Lakehouse dùng Iceberg, MinIO, Hive Metastore, Trino, Spark, Kafka, NiFi, Debezium, Superset  
**Mục tiêu:** Đánh giá khả năng bổ sung StarRocks vào kiến trúc hiện tại để tăng tốc dashboard/BI, giảm tải query lặp lại, và hỗ trợ một số use case near real-time

---

## Mục lục

1. [Executive Summary](#1-executive-summary)
2. [Bối cảnh hệ thống hiện tại](#2-bối-cảnh-hệ-thống-hiện-tại)
3. [Tổng quan StarRocks](#3-tổng-quan-starrocks)
4. [Tích hợp StarRocks — Đầu vào và Đầu ra](#4-tích-hợp-starrocks--đầu-vào-và-đầu-ra)
5. [Lưu trữ, HA, và Các Đảm bảo Big Data](#5-lưu-trữ-ha-và-các-đảm-bảo-big-data)
6. [Table Design — Partition, Bucket, Sort Key, Index](#6-table-design--partition-bucket-sort-key-index)
7. [Phương án tích hợp vào kiến trúc hiện tại](#7-phương-án-tích-hợp-vào-kiến-trúc-hiện-tại)
8. [Kiến trúc đề xuất](#8-kiến-trúc-đề-xuất)
9. [Thiết kế POC](#9-thiết-kế-poc)
10. [Benchmark đề xuất](#10-benchmark-đề-xuất)
11. [Rủi ro và Giới hạn](#11-rủi-ro-và-giới-hạn)
12. [Lộ trình triển khai](#12-lộ-trình-triển-khai)
13. [Tiêu chí Go/No-Go](#13-tiêu-chí-gono-go)
14. [Khuyến nghị cuối cùng](#14-khuyến-nghị-cuối-cùng)
15. [Appendix](#15-appendix)

---

## 1. Executive Summary

### 1.1 Hệ thống hiện tại

Stack lakehouse hiện tại đã đầy đủ các thành phần chính:

- Ingestion qua NiFi, Kafka/API, và CDC/Debezium
- Dữ liệu lưu trên Iceberg (MinIO) qua các tầng Staging → MDM → DTM
- Spark xử lý toàn bộ ETL/ELT
- Trino làm query engine đọc Iceberg
- Superset hiển thị dashboard, hiện query qua Trino

### 1.2 Vấn đề cần giải quyết

| Vấn đề | Biểu hiện |
|--------|-----------|
| Dashboard query chậm | Trino phải scan Iceberg/MinIO mỗi lần |
| Trino bị tải cao | Nhiều dashboard lặp lại cùng aggregate query |
| Không có pre-computation | Mỗi query tính lại từ đầu, không có MV |
| Thiếu near-realtime | Dữ liệu phải qua Spark batch mới lên dashboard |

### 1.3 Vai trò phù hợp của StarRocks

StarRocks **không phải** công nghệ thay thế lakehouse hiện tại. Vai trò phù hợp nhất là:

- Lớp **BI serving / query acceleration** phía trên Iceberg DTM
- Engine phục vụ dashboard có **latency thấp và concurrency cao**
- Lớp **native serving mart / materialized view** cho dashboard quan trọng
- Lớp **realtime serving** cho một số Kafka topic cần near-realtime

### 1.4 Khuyến nghị chính

1. **Không** thay Spark MDM/DTM bằng StarRocks
2. **Không** thay Trino toàn hệ thống
3. **Bắt đầu** bằng POC StarRocks đọc Iceberg DTM qua Hive Metastore + MinIO
4. **Tiếp theo** tạo native table / materialized view cho dashboard hot
5. **Sau cùng** mới xét Kafka → StarRocks cho các use case realtime rõ ràng

---

## 2. Bối cảnh hệ thống hiện tại

### 2.1 Luồng ingestion

Hệ thống có 3 nhóm ingestion chính:

```
Source ──────────────► NiFi ──────────────────────────────► Iceberg Staging
                                                                    │
Domain systems ──► Post API ──► Kafka ──► Spark Streaming ──────────┤
                                    └──► Kafka Connect Iceberg Sink ─┤
                                                                      │
CDC sources ──► Debezium ──► Kafka Connect ──────────────────────────┘
```

### 2.2 Luồng xử lý dữ liệu

```
Iceberg Staging (MinIO)
        │
        ▼
  Spark MDM job
        │
        ▼
  Iceberg MDM (MinIO)
        │
        ▼
  Spark DTM job
        │
        ▼
  Iceberg DTM (MinIO)  ◄── Source of truth
        │
        ▼
      Trino
        │
        ▼
   Superset dashboards
```

### 2.3 Stack hiện tại — tóm tắt

| Thành phần | Vai trò | Ghi chú |
|-----------|---------|---------|
| NiFi | Batch ingestion | Pull từ source systems |
| Kafka | Event backbone, streaming | API push, CDC |
| Debezium | CDC connector | Ít dùng hơn |
| Apache Iceberg | Table format | Staging, MDM, DTM |
| MinIO | Object storage S3-compatible | Lưu Iceberg data |
| Hive Metastore | Metadata/catalog | Quản lý Iceberg schema |
| Apache Spark | ETL/ELT engine | MDM, DTM transform |
| Trino | Query engine | Ad-hoc, dashboard |
| Apache Superset | BI/Dashboard | Kết nối qua Trino |

---

## 3. Tổng quan StarRocks

### 3.1 StarRocks là gì?

StarRocks là **analytical database** theo kiến trúc **MPP (Massively Parallel Processing)** — query được chia nhỏ và chạy song song trên nhiều node cùng lúc, thay vì tuần tự trên 1 máy. Được thiết kế tối ưu cho workload **OLAP** với latency thấp và concurrency cao.

> **OLAP vs OLTP:** OLTP (MySQL, PostgreSQL) tối ưu cho insert/update/delete nhiều transaction nhỏ. OLAP tối ưu cho đọc và phân tích dữ liệu lớn. StarRocks là OLAP.

### 3.2 Kiến trúc node

```
                    Client / BI Tool
                   (Superset, Grafana)
                          │
                    ┌─────▼──────┐
                    │     FE     │  ← MySQL protocol
                    │ (Frontend) │  ← Parse, Optimize, Schedule
                    └─────┬──────┘
                          │ execution plan
             ┌────────────┼────────────┐
             ▼            ▼            ▼
       ┌──────────┐ ┌──────────┐ ┌──────────┐
       │ BE / CN  │ │ BE / CN  │ │ BE / CN  │
       │  node 1  │ │  node 2  │ │  node 3  │
       └──────────┘ └──────────┘ └──────────┘
```

**FE (Frontend):**
- Nhận SQL từ client qua MySQL protocol
- Parse SQL → optimize (CBO) → tạo execution plan → phân phối xuống BE/CN
- Quản lý metadata: biết tablet nào nằm ở BE nào
- Cần deploy nhiều FE (ít nhất 3) trong production để tránh SPOF

**BE (Backend) — Shared-Nothing:**
- Vừa lưu data (columnar, local disk) vừa execute query
- Mỗi BE giữ một phần data (shard/tablet)
- Query chạy song song trên tất cả BE liên quan
- Latency thấp nhất vì data local

**CN (Compute Node) — Shared-Data:**
- Chỉ execute query, không lưu data lâu dài
- Data lưu trên object storage (MinIO/S3)
- CN có local cache để tránh đọc MinIO mọi lúc
- Phù hợp khi muốn tách compute và storage

### 3.3 Hai chế độ triển khai

#### Shared-Nothing (FE + BE)

```
FE ──► BE1 | BE2 | BE3
           (data + compute local disk)
```

- Data replicate giữa các BE (mặc định 3 replica)
- Latency thấp nhất
- Scale storage = thêm BE node
- Phù hợp: native serving mart, dashboard SLA cao

#### Shared-Data (FE + CN)

```
FE ──► CN1 | CN2 | CN3
            (compute + local cache)
                  │
          MinIO / Object Storage
           (data lưu lâu dài ở đây)
```

- Data lưu trên MinIO, CN cache hot data
- Scale compute và storage độc lập
- Phù hợp: cloud-native, tái dụng MinIO infrastructure sẵn có

### 3.4 Các thành phần kỹ thuật cốt lõi

**Vectorized Execution Engine:** Query thực thi theo batch column (1024 row/lần) thay vì từng row. Tận dụng CPU cache và SIMD instruction → nhanh hơn đáng kể cho OLAP.

**Columnar Storage:** Data lưu theo cột. Query `SELECT SUM(revenue)` chỉ đọc cột `revenue`, không đọc các cột khác → giảm I/O mạnh.

**Cost-Based Optimizer (CBO):** Tự động tính execution plan tối ưu cho join nhiều bảng — không cần hint thủ công.

**Asynchronous Materialized View:** Pre-compute kết quả query phức tạp, lưu thành bảng vật lý. StarRocks tự động rewrite query để dùng MV khi phù hợp.

**4 kiểu table:**

| Table Type | Dùng khi | Đặc điểm |
|-----------|---------|---------|
| Duplicate Key | Event log, append-only | Cho phép duplicate key |
| Aggregate | KPI pre-aggregated | Tự aggregate khi insert |
| Unique Key | Dimension, dedup | Giữ row mới nhất theo key |
| Primary Key | CDC, upsert, realtime | Hỗ trợ update/delete/upsert hiệu quả |

### 3.5 So sánh với các stack cùng vai trò

| Tiêu chí | StarRocks | ClickHouse | Apache Druid | Apache Pinot |
|---------|----------|-----------|-------------|-------------|
| Kiến trúc | MPP, shared-nothing/shared-data | MPP, shared-nothing | Lambda (batch+stream) | Lambda (batch+stream) |
| Điểm mạnh | Flexible OLAP, MV mạnh, external catalog, upsert tốt | Aggregate cực nhanh, insert throughput | Realtime sub-second từ streaming | Ultra-low latency, user-facing |
| Điểm yếu | Vận hành phức tạp hơn ClickHouse | Join phức tạp yếu, external catalog hạn chế | SQL linh hoạt kém | SQL hạn chế, ops phức tạp |
| Join | Tốt (CBO, colocate join) | Yếu (khuyến khích denormalize) | Hạn chế | Hạn chế |
| Upsert/CDC | Primary Key table, tốt | ReplacingMergeTree có giới hạn | Phức tạp | Phức tạp |
| Materialized View | Async MV, query rewrite tự động | Có nhưng đơn giản | Không native | Không native |
| External Catalog | Iceberg, Hive, Delta, Hudi | Có nhưng ít mature | Không phải trọng tâm | Không phải trọng tâm |
| Kafka ingestion | Routine Load, Kafka connector | Kafka engine tích hợp sẵn | Native | Native |
| SQL compatibility | MySQL-compatible, ANSI SQL | Dialect riêng | SQL hạn chế | SQL hạn chế |
| Phù hợp nhất | BI serving trên lakehouse, flexible OLAP | Pure analytics, log analytics | Realtime từ event stream | User-facing realtime cực thấp |

**Kết luận:** Với stack lakehouse Iceberg + Superset của hệ thống hiện tại, StarRocks là lựa chọn phù hợp nhất vì hỗ trợ Iceberg external catalog, MV mạnh, và join linh hoạt.

---

## 4. Tích hợp StarRocks — Đầu vào và Đầu ra

### 4.1 Đầu vào — Nguồn dữ liệu StarRocks đọc được

StarRocks chia thành 2 nhóm: **đọc trực tiếp không copy data (External Catalog)** và **nạp data vào native table (Loading)**.

#### 4.1.1 External Catalog

Cho phép query dữ liệu từ hệ thống bên ngoài mà không cần migrate vào StarRocks.

Các loại catalog được hỗ trợ (v4.1):

| Catalog | Nguồn | Ghi chú |
|--------|-------|---------|
| Iceberg | Apache Iceberg | Phù hợp nhất với stack hiện tại |
| Hive | Apache Hive | |
| Hudi | Apache Hudi | |
| Delta Lake | Delta Lake | |
| JDBC | MySQL, PostgreSQL, Oracle, SQL Server | Từ v3.1+ |
| Elasticsearch | Elasticsearch | Từ v3.1+ |
| Paimon | Apache Paimon | Từ v3.1+ |
| Unified | Hive + Iceberg + Hudi + Delta | Cùng storage + metastore |

> **Lưu ý:** External Table (cơ chế cũ) không còn được khuyến nghị từ v3.0 và có thể bị deprecated. Dùng External Catalog thay thế.

**Ví dụ tạo Iceberg catalog trỏ vào HMS + MinIO:**

```sql
CREATE EXTERNAL CATALOG iceberg_dtm
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift://hive-metastore:9083",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.access_key" = "<access_key>",
    "aws.s3.secret_key" = "<secret_key>"
);
```

#### 4.1.2 Data Loading — Nạp vào native table

| Phương thức | Nguồn | Kiểu | Khi nào dùng |
|------------|-------|------|-------------|
| Stream Load | HTTP push, file local | Sync | Load file nhỏ/vừa, custom app |
| Broker Load | S3, MinIO, HDFS | Async batch | Load file lớn từ object storage |
| Routine Load | Kafka, Pulsar | Async streaming liên tục | **Real-time ingest từ Kafka** |
| Spark Load | Spark cluster | Async batch lớn | Khi đã có Spark, ETL nặng |
| INSERT INTO SELECT | External Catalog, table khác | Sync/Async | **Load từ Iceberg DTM → native table** |

**Connector ecosystem bổ sung:**

- **Kafka Connector** (Kafka Connect sink): phù hợp format Protobuf, Avro, JSON
- **Flink Connector**: streaming ELT từ CDC sources, hỗ trợ schema change qua Flink CDC 3.0
- **Spark Connector**: đọc/ghi StarRocks từ Spark job
- **Airflow, dbt**: orchestration và transformation

**Mapping vào stack hiện tại:**

```
Kafka topic          ──► Routine Load / Kafka Connector ──► StarRocks native table
Iceberg DTM (MinIO)  ──► INSERT INTO SELECT from Iceberg Catalog ──► StarRocks native table
Spark job            ──► Spark Connector ──► StarRocks native table
```

### 4.2 Đầu ra — StarRocks xuất dữ liệu đi đâu

#### 4.2.1 BI / Visualization (đầu ra chính)

StarRocks dùng MySQL protocol — bất kỳ tool nào connect được MySQL đều connect được StarRocks.

| Tool | Cách kết nối |
|-----|-------------|
| Apache Superset | SQLAlchemy URI: `starrocks://user:pass@fe-host:9030/catalog.db` |
| Grafana | MySQL datasource |
| Tableau | MySQL connector |
| Power BI | MySQL connector |
| FineBI, Hex, Querybook | Native support |
| DBeaver, DataGrip | MySQL / JDBC driver |

#### 4.2.2 Export data ra file / Object Storage

**`INSERT INTO FILES()` (khuyến nghị từ v3.1+):**

```sql
-- Export từ StarRocks native table ra MinIO dưới dạng Parquet
INSERT INTO FILES(
    "path" = "s3://export-bucket/revenue/",
    "format" = "parquet",
    "compression" = "lz4",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.access_key" = "xxx",
    "aws.s3.secret_key" = "yyy"
)
SELECT * FROM ads_revenue_daily
WHERE report_date >= '2026-06-01';
```

Linh hoạt hơn `EXPORT` vì cho phép kèm SELECT transformation tùy ý.

#### 4.2.3 Downstream systems

| Downstream | Cơ chế |
|-----------|--------|
| Spark | Spark Connector đọc từ StarRocks |
| Flink | Flink Connector đọc bulk từ StarRocks |
| App/Service | JDBC driver, MySQL client |
| Airflow | SQLExecuteQueryOperator, MySQLHook |
| dbt | dbt-starrocks adapter |

### 4.3 Metadata

StarRocks có hệ thống metadata đầy đủ, truy cập qua SQL chuẩn.

**Information Schema** là database chứa các read-only views về tất cả objects trong instance. Từ v3.2.0 hỗ trợ thêm metadata của external catalog.

```sql
-- Xem tất cả tables trong database DTM
SELECT table_name, table_rows, data_length
FROM information_schema.tables
WHERE table_schema = 'dtm';

-- Xem schema một table
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'ads_revenue_daily';

-- Xem materialized views
SELECT * FROM information_schema.materialized_views;

-- Monitor Routine Load jobs (Kafka ingestion)
SELECT * FROM information_schema.routine_load_jobs;

-- Xem catalog đã tạo
SHOW CATALOGS;
```

**Use case thực tế của metadata:**
- Database auditing và governance
- Performance tuning (row count, data size per partition)
- Tích hợp với external data catalog (Apache Atlas, DataHub)
- Monitoring job ingestion và refresh MV

---

## 5. Lưu trữ, HA, và Các Đảm bảo Big Data

### 5.1 Các đảm bảo Big Data cốt lõi

Bất kỳ hệ thống distributed nào cũng phải giải quyết 4 vấn đề: Scalability, Fault Tolerance, Consistency, và Durability. StarRocks xử lý như sau:

#### Scalability

StarRocks dùng kiến trúc distributed để chia table theo chiều ngang (horizontal sharding) và lưu trên nhiều replica. Cluster có thể scale để hỗ trợ phân tích dữ liệu ở mức 10PB.

```
Scale-out: Thêm BE/CN node mới
    └── FE tự động rebalance tablet sang node mới
    └── Không downtime
    └── Query capacity tăng tuyến tính

Scale-up: Tăng RAM/CPU/disk trên node hiện tại
    └── Đơn giản hơn nhưng có giới hạn vật lý
```

#### Fault Tolerance

StarRocks được thiết kế không có single point of failure. Khi 1 node fail, data tự động migrate mà không ảnh hưởng availability tổng thể.

**FE layer:** Dùng Raft consensus — Leader FE chết → Follower tự bầu Leader mới.

**BE/data layer:** Mỗi tablet có 3 replica mặc định. Khi 1 BE chết:

```
BE node chết
    └── FE phát hiện replica UNKNOWN
    └── Query tự route sang 2 replica còn lại — không downtime
    └── FE trigger CLONE task để rebuild replica thiếu
    └── BE recover → replica được rebuild từ peer BE
```

> **Fresher note:** Đây là lý do mặc định `replication_num = 3`. Nếu set về 1 để tiết kiệm disk trong dev/test, khi BE chết là mất data — không có replica để failover.

#### Consistency (ACID)

StarRocks đảm bảo ACID cho mỗi transaction ingestion:

- **Atomicity:** Load 1 batch data là all-or-nothing — không có trạng thái load nửa chừng
- **Isolation:** Dùng snapshot isolation — query đang chạy không bị ảnh hưởng bởi write song song
- **Durability:** Data được ghi xuống ít nhất `replication_num` BE trước khi trả success

#### Durability

Data được ghi xuống disk trên nhiều BE nodes trước khi FE trả về success. Từ v2.4+, StarRocks hỗ trợ 2-phase commit (2PC) cho Flink và Kafka để tăng exactly-once guarantee.

### 5.2 Cơ chế lưu trữ bên trong

Hiểu phần này giúp nắm tại sao StarRocks query nhanh và HA hoạt động như thế nào.

#### Tablet — đơn vị lưu trữ cơ bản

```
Table
 └── Partition (chia theo thời gian — vd: tháng)
      └── Bucket (chia theo HASH)
           └── Tablet (đơn vị vật lý)
                └── Replica x3 (trên 3 BE khác nhau)
```

Mỗi tablet là 1 logical slice của table. Số tablet được xác định bởi số bucket và số partition. Mỗi tablet có `replication_num` replica trên các BE khác nhau.

#### Columnar storage

Data trong native table lưu theo cột, nén bằng LZ4/Zstd. Query chỉ cần đọc các cột liên quan → tiết kiệm I/O đáng kể so với row-based storage.

### 5.3 High Availability (HA)

StarRocks chia HA thành 2 phần độc lập:

#### HA Metadata — FE cluster

FE HA dùng kiến trúc primary-secondary replication với giao thức raft-like BDB JE.

```
FE Leader      ── nhận write, điều phối query
FE Follower 1  ── voting, có thể thành Leader
FE Follower 2  ── voting
FE Observer    ── non-voting, scale read capacity
```

Số FE Follower phải là số lẻ (2n+1): 3 Follower chịu được 1 chết, 5 Follower chịu được 2 chết.

> **Yêu cầu:** Clock diff giữa FE nodes < 5 giây → dùng NTP.

#### HA Data — BE/tablet replica

Khi BE node down, FE tự động route query sang replica khỏe. Khi node recover, FE tự trigger CLONE để rebuild replica.

### 5.4 Lưu trữ lâu dài trên MinIO

Stack của bạn có 2 vùng MinIO riêng biệt. Cần phân biệt rõ StarRocks dùng vùng nào.

#### Phân vùng MinIO trong hệ thống

```
MinIO Vùng lưu trữ (Storage Zone)
├── Iceberg Staging     ← NiFi/Kafka/Debezium ghi vào
├── Iceberg MDM         ← Spark MDM ghi vào
├── Iceberg DTM         ← Source of truth, Spark DTM ghi vào
└── StarRocks backup    ← BACKUP SNAPSHOT từ StarRocks (nếu cần)

MinIO Vùng App (App Zone)
└── StarRocks native data  ← Chỉ khi dùng Shared-Data mode
     ├── Segment files của cloud-native table
     ├── Materialized view data
     └── CN local cache (hot data)
```

#### Shared-Nothing vs Shared-Data — ảnh hưởng đến MinIO

**Shared-Nothing (FE + BE):**
- Native table lưu local trên BE disk — **không dùng MinIO vùng app**
- MinIO chỉ dùng cho: backup snapshot, export file, đọc Iceberg external catalog
- Latency thấp nhất, phù hợp SLA cao

**Shared-Data (FE + CN):**
- Native table lưu thẳng trên MinIO vùng app
- CN cache hot data local
- Tái dụng được MinIO infrastructure sẵn có

```sql
-- Tạo storage volume trỏ vào MinIO vùng app (Shared-Data)
CREATE STORAGE VOLUME starrocks_app_vol
TYPE = S3
LOCATIONS = ("s3://starrocks-app-bucket")
PROPERTIES (
    "enabled" = "true",
    "aws.s3.endpoint" = "http://minio-app:9000",
    "aws.s3.access_key" = "xxx",
    "aws.s3.secret_key" = "yyy",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.enable_path_style_access" = "true"
);

SET starrocks_app_vol AS DEFAULT STORAGE VOLUME;
```

#### So sánh 2 chế độ theo tiêu chí thực tế

| Tiêu chí | Shared-Nothing (FE+BE) | Shared-Data (FE+CN) |
|---------|----------------------|-------------------|
| Native table lưu ở đâu | Local disk trên BE | MinIO vùng app |
| Tận dụng MinIO sẵn có | Chỉ backup/export | Lưu data chính |
| Query latency | Thấp hơn (data local) | Cao hơn nếu cache miss |
| Scale storage | Thêm BE node + disk | Mở rộng MinIO bucket |
| Scale compute | Thêm BE node | Thêm CN node (độc lập) |
| Phù hợp cho | Dashboard SLA cao, ít node | Cloud-native, tái dụng MinIO |
| Ops complexity | Quản lý disk BE | Quản lý MinIO + cache CN |

### 5.5 Backup và Restore

StarRocks hỗ trợ backup/restore dữ liệu native table ra external storage.

```sql
-- Tạo repository trỏ vào MinIO
CREATE REPOSITORY starrocks_backup_repo
WITH BROKER
ON LOCATION "s3://backup-bucket/starrocks/"
PROPERTIES (
    "aws.s3.access_key" = "xxx",
    "aws.s3.secret_key" = "yyy",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.enable_path_style_access" = "true"
);

-- Backup snapshot
BACKUP SNAPSHOT db_serving.snapshot_20260615
TO starrocks_backup_repo
ON (TABLE ads_revenue_daily, TABLE mart_revenue_monthly);

-- Restore khi cần
RESTORE SNAPSHOT db_serving.snapshot_20260615
FROM starrocks_backup_repo
ON (TABLE ads_revenue_daily)
PROPERTIES ("backup_timestamp" = "2026-06-15-10-00-00");
```

**Backup strategy theo phương án:**

| Phương án | Backup strategy |
|----------|----------------|
| A — External catalog only | Gần như không cần backup data; chỉ backup config và catalog definition |
| B — Native table/MV | Backup định kỳ hoặc rebuild từ Iceberg DTM (thường đơn giản hơn) |
| C — Realtime table từ Kafka | Cần Kafka retention đủ dài để replay; backup snapshot định kỳ |

### 5.6 HA Checklist cho production

```
FE
├── Tối thiểu 3 Follower (chịu 1 node chết)
├── Clock diff giữa FE nodes < 5 giây (NTP)
└── Backup metadata FE định kỳ

BE (shared-nothing)
├── replication_num = 3 (default, không giảm xuống 1 trong production)
├── Ít nhất 3 BE node trên 3 máy vật lý khác nhau
└── Monitor disk usage — tránh full disk (FE reject write khi BE > 85%)

CN (shared-data)
├── Ít nhất 3 CN node
├── Local cache đủ lớn cho hot data
└── MinIO HA configuration

Data
├── BACKUP SNAPSHOT định kỳ ra MinIO backup bucket
├── Với serving copy: rebuild từ Iceberg DTM thay vì maintain backup phức tạp
└── Với realtime table: Kafka retention đủ để replay nếu cần

Network
├── FE reach Hive Metastore (port 9083)
├── BE/CN reach MinIO (port 9000)
└── Superset reach FE port 9030
```

---

## 6. Table Design — Partition, Bucket, Sort Key, Index

Đây là phần kỹ thuật quan trọng nhất khi thiết kế serving table. Hiểu đúng 4 thành phần này ảnh hưởng trực tiếp đến query performance.

### 6.1 Tổng quan 2-level data organization

```
Table
 └── Partition 1 (vd: 2026-05)
 │    ├── Bucket 0  → Tablet → Replica x3
 │    ├── Bucket 1  → Tablet → Replica x3
 │    └── Bucket N  → Tablet → Replica x3
 └── Partition 2 (vd: 2026-06)
      ├── Bucket 0  → Tablet → Replica x3
      └── ...
```

### 6.2 Partition

Partition giúp **partition pruning** — query chỉ scan đúng partition liên quan.

**RANGE Partition** (dùng phổ biến nhất cho time-series/BI):

```sql
-- Partition tự động theo tháng
PARTITION BY date_trunc('month', report_date)

-- Hoặc định nghĩa tay
PARTITION BY RANGE(report_date) (
    PARTITION p202505 VALUES LESS THAN ("2025-06-01"),
    PARTITION p202506 VALUES LESS THAN ("2026-07-01")
)
```

Query `WHERE report_date >= '2026-06-01'` → StarRocks chỉ scan partition `p202606`, bỏ qua toàn bộ partition cũ.

**LIST Partition** (chia theo giá trị cụ thể):

```sql
PARTITION BY LIST(region_code) (
    PARTITION p_north VALUES IN ("HN", "HP"),
    PARTITION p_south VALUES IN ("HCM", "CT")
)
```

> **Fresher pitfall:** Đừng partition quá nhỏ (vd: theo giờ cho data ít). Quá nhiều partition → metadata overhead → FE chậm. Rule of thumb: mỗi partition nên chứa ít nhất vài GB data.

### 6.3 Bucket (Bucketing)

Data trong mỗi partition tiếp tục chia thành các bucket, mỗi bucket map tới 1 tablet.

**Hash Bucketing** (phổ biến nhất):

```sql
DISTRIBUTED BY HASH(province_code) BUCKETS 32
```

Query filter theo `province_code` → **bucket pruning** → chỉ scan đúng bucket liên quan.

**Chọn hash key:**
- Column hay xuất hiện trong `WHERE` hoặc `JOIN ON`
- Column có cardinality cao để tránh data skew
- Tránh column quá ít giá trị (vd: gender M/F) → skew nặng

**Random Bucketing** (từ v3.1, khi không biết chọn gì):

```sql
DISTRIBUTED BY RANDOM
```

Phân tán đều — tránh skew nhưng không có bucket pruning.

### 6.4 Sort Key

Data trong tablet được sort theo sort key. Cứ 1024 row tạo 1 logical data block, và một prefix index entry được tạo từ row đầu tiên của block đó.

```sql
-- Duplicate Key table: sort key là 2 column đầu
DUPLICATE KEY(report_date, province_code)

-- Primary Key table (v3.0+): sort key tách riêng khỏi primary key
PRIMARY KEY(order_id)
ORDER BY(event_date, province_code)
```

Query `WHERE report_date = '2026-06-01' AND province_code = 'HN'` → prefix index match → scan rất ít block.

> **Lưu ý:** Prefix index chỉ match được prefix liên tiếp. Query `WHERE province_code = 'HN'` mà không có `report_date` sẽ không dùng được prefix index nếu `report_date` đứng trước trong sort key.

### 6.5 Index

StarRocks có 2 nhóm index: **built-in (tự động)** và **user-defined (tự tạo)**.

#### Built-in (tự động, không cần cấu hình)

| Index | Cơ chế | Effect |
|------|--------|--------|
| Prefix Index | 1 entry per 1024 rows theo sort key | Skip data block |
| ZoneMap Index | Lưu min/max per data page per column | Skip page không match range filter |
| Ordinal Index | Mapping row → vị trí vật lý | Hỗ trợ internal lookup |

#### User-defined (tự tạo khi cần)

**Bloom Filter Index** — dùng cho high cardinality, equality filter:

```sql
-- Tạo khi create table
PROPERTIES ("bloom_filter_columns" = "customer_id, order_id")
```

Bloom filter detect nhanh data file nào chắc chắn **không** chứa giá trị cần tìm → skip hoàn toàn. Có thể false positive (nói có nhưng không có) nhưng không bao giờ false negative.

Phù hợp: `WHERE customer_id = 12345`, ID column, high cardinality.

**Bitmap Index** — dùng cho combination filter hoặc high cardinality:

```sql
CREATE INDEX idx_province ON ads_revenue_daily (province_code) USING BITMAP;
```

> **Fresher pitfall:** Nhiều người nghĩ bitmap index chỉ dùng cho low cardinality. Trong StarRocks thực tế phù hợp hơn với high cardinality hoặc combination nhiều column. Bitmap index cần filter out ít nhất 999/1000 data mới thực sự hiệu quả.

#### Tóm tắt — khi nào dùng gì

```
Partition (RANGE theo thời gian)
    └── Dashboard luôn filter theo ngày/tháng/quý
    └── Effect: bỏ qua hoàn toàn partition không liên quan

Bucket (HASH theo dimension)
    └── Query hay JOIN/filter theo province_code, merchant_id
    └── Effect: chỉ scan bucket liên quan trong partition

Sort Key
    └── Query hay filter theo 1-2 column cố định
    └── Effect: prefix index skip data block nhanh

ZoneMap (tự động)
    └── Luôn có, không cần làm gì
    └── Effect: skip data page có min/max không match

Bloom Filter Index
    └── Column high cardinality (customer_id, order_id), equality filter
    └── Effect: skip data file không có giá trị cần tìm

Bitmap Index
    └── Filter combination nhiều column hoặc high cardinality
    └── Effect: intersection bitmap tìm row match nhanh
```

---

## 7. Phương án tích hợp vào kiến trúc hiện tại

Có 3 phương án tích hợp, nên triển khai theo thứ tự A → B → C.

### Phương án A — StarRocks đọc trực tiếp Iceberg DTM

#### Mô tả

StarRocks deploy song song với Trino. Tạo Iceberg external catalog trỏ tới Hive Metastore, đọc dữ liệu Iceberg trên MinIO.

```
Iceberg DTM (MinIO)
        ├──► HMS (Hive Metastore)
        │         │
        │    ┌────▼─────────────────────────┐
        │    │ StarRocks Iceberg Ext Catalog │
        └────►                              │
             └────────────┬─────────────────┘
                          │
                     Superset POC
                          │
                     (song song với Trino)
```

#### Cấu hình

```sql
CREATE EXTERNAL CATALOG iceberg_dtm
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift://hive-metastore:9083",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.access_key" = "<access_key>",
    "aws.s3.secret_key" = "<secret_key>"
);

-- Kiểm tra kết nối
SHOW DATABASES FROM iceberg_dtm;
SHOW TABLES FROM iceberg_dtm.dtm;
SELECT COUNT(*) FROM iceberg_dtm.dtm.fact_revenue;
```

#### Ưu / Nhược điểm

| | Chi tiết |
|-|---------|
| ✅ Ưu | Ít thay đổi nhất, phù hợp POC nhanh, không copy data, rollback về Trino dễ |
| ❌ Nhược | Chưa chắc nhanh hơn Trino; vẫn phụ thuộc MinIO throughput và Iceberg file layout |

#### Khi nào dùng

Muốn kiểm chứng tích hợp nhanh, test dashboard thật mà không tạo data copy.

---

### Phương án B — StarRocks làm native BI serving mart

#### Mô tả

StarRocks đọc từ Iceberg DTM rồi lưu vào native table hoặc materialized view. Superset query vào bảng native để có latency tốt hơn.

```
Iceberg DTM (MinIO)
        │
        ▼
 INSERT INTO SELECT / Async MV
        │
        ▼
StarRocks Native Tables / Materialized Views
        │
        ▼
 Superset dashboard quan trọng
```

#### Các kiểu triển khai

**Native table từ DTM:**

```sql
CREATE TABLE ads_revenue_daily (
    report_date DATE NOT NULL,
    province_code VARCHAR(32),
    product_group VARCHAR(128),
    revenue DECIMAL(18,2),
    order_count BIGINT
)
DUPLICATE KEY(report_date, province_code)
PARTITION BY date_trunc('month', report_date)
DISTRIBUTED BY HASH(province_code) BUCKETS 32;

-- Load từ Iceberg DTM
INSERT INTO ads_revenue_daily
SELECT report_date, province_code, product_group, revenue, order_count
FROM iceberg_dtm.dtm.ads_revenue_daily;
```

**Asynchronous Materialized View trên Iceberg:**

```sql
CREATE MATERIALIZED VIEW mv_revenue_daily
DISTRIBUTED BY HASH(province_code)
REFRESH ASYNC EVERY (INTERVAL 1 HOUR)
AS
SELECT
    report_date,
    province_code,
    product_group,
    SUM(revenue) AS revenue,
    COUNT(*) AS record_count
FROM iceberg_dtm.dtm.fact_revenue
GROUP BY report_date, province_code, product_group;
```

#### Ưu / Nhược điểm

| | Chi tiết |
|-|---------|
| ✅ Ưu | Tăng tốc dashboard cao nhất; giảm tải Trino/MinIO; có thể kiểm soát freshness |
| ❌ Nhược | Có data copy; cần refresh/sync; cần xử lý schema evolution thủ công |

#### Khi nào dùng

Dashboard query chậm qua Trino, nhiều user cùng xem, KPI aggregate theo chiều cố định, cần SLA rõ ràng.

---

### Phương án C — StarRocks realtime serving từ Kafka

#### Mô tả

Một số Kafka topic ingest song song vào Iceberg Staging (source of truth) và StarRocks (realtime serving).

```
Kafka topics
    ├──► Kafka Connect Iceberg Sink ──► Iceberg Staging (source of truth)
    └──► Routine Load / Kafka Connector ──► StarRocks Realtime Tables
                                                    │
                                             Realtime dashboard
```

#### Ví dụ Primary Key table cho realtime serving

```sql
CREATE TABLE rt_order_status (
    order_id BIGINT NOT NULL,
    event_time DATETIME NOT NULL,
    customer_id BIGINT,
    status VARCHAR(64),
    amount DECIMAL(18,2),
    updated_at DATETIME
)
PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 32
ORDER BY(event_time)
PROPERTIES ("enable_persistent_index" = "true");
```

#### Ưu / Nhược điểm

| | Chi tiết |
|-|---------|
| ✅ Ưu | Latency thấp; phù hợp monitoring/operational dashboard; Primary Key table xử lý upsert tốt |
| ❌ Nhược | Tạo 2 đường data song song; cần xử lý consistency, late event, duplicate event |

#### Khi nào dùng

Dashboard cần dữ liệu gần realtime, topic có schema ổn định, logic KPI đơn giản không qua DTM.

---

### So sánh 3 phương án

| Tiêu chí | Phương án A | Phương án B | Phương án C |
|---------|------------|------------|------------|
| Data copy | Không | Có | Có |
| Thay đổi pipeline | Không | Tối thiểu (refresh job) | Thêm Kafka route |
| Performance tăng | Không chắc | Rõ ràng | Rõ ràng (realtime) |
| Độ phức tạp | Thấp | Trung bình | Cao |
| Phù hợp cho | POC, validation | Dashboard hot production | Near-realtime use case |
| Thứ tự triển khai | Đầu tiên | Sau A | Sau B |

---

## 8. Kiến trúc đề xuất

### 8.1 Ngắn hạn — POC song song Trino

```
Iceberg DTM (MinIO)
    ├──► Trino ──────────────────► Superset (dashboards hiện tại)
    └──► StarRocks External Catalog ──► Superset (POC dashboards)
```

Mục tiêu: không phá flow hiện tại, test thực tế trên dashboard thật.

### 8.2 Trung hạn — Native BI serving mart

```
Iceberg DTM (MinIO) — Source of Truth
    ├──► Trino ──────────────────────────────► Superset (ad-hoc / general)
    └──► StarRocks Native BI Serving Mart ───► Superset (dashboard SLA cao)
```

Mục tiêu: Trino giữ workload linh hoạt, StarRocks phục vụ dashboard quan trọng.

### 8.3 Dài hạn — Bao gồm realtime

```
Kafka ──► Iceberg Sink ──► Iceberg Staging ──► Spark ──► Iceberg DTM
    │                                                          │
    │                                              StarRocks Native Serving
    │                                                          │
    └──► StarRocks Realtime Tables ─────────────► Superset
                                                  (batch mart + realtime mart)
```

Mục tiêu: batch/curated analytics vẫn qua Iceberg/Spark/DTM; realtime dashboard đọc trực tiếp từ StarRocks realtime tables.

---

## 9. Thiết kế POC

### POC 1 — StarRocks External Iceberg Catalog

**Mục tiêu:** Kiểm chứng kết nối và đo baseline performance.

**Checklist:**
- [ ] Deploy StarRocks test cluster (1 FE + 1 BE tối thiểu)
- [ ] Cấu hình network reach tới HMS (port 9083)
- [ ] Cấu hình access MinIO (endpoint, access key, secret key)
- [ ] Tạo Iceberg external catalog
- [ ] `SHOW DATABASES FROM iceberg_dtm`
- [ ] `SHOW TABLES FROM iceberg_dtm.dtm`
- [ ] `SELECT COUNT(*)` trên bảng fact lớn
- [ ] Query filter theo partition
- [ ] Query aggregate: GROUP BY ngày/tháng/tỉnh
- [ ] Query join 2-3 bảng DTM
- [ ] So sánh latency với Trino trên cùng query

**Success criteria:**
- StarRocks đọc được bảng DTM không lỗi
- Latency có thể đo được và so sánh với Trino
- Không lỗi permission hoặc network

---

### POC 2 — Superset Integration

**Mục tiêu:** Kết nối Superset với StarRocks, test dashboard thật.

**Connection URI:**

```
starrocks://admin:<password>@starrocks-fe:9030/iceberg_dtm.dtm
```

**Checklist:**
- [ ] Cài `starrocks-connector` hoặc MySQL driver trên Superset nếu cần
- [ ] Tạo database connection StarRocks trong Superset
- [ ] Tạo dataset từ external Iceberg catalog
- [ ] Clone 2-3 dashboard đang dùng Trino để chạy qua StarRocks
- [ ] So sánh render time giữa 2 connection
- [ ] Kiểm tra query log StarRocks

**Success criteria:**
- Superset query StarRocks ổn định, không timeout
- Dashboard chạy được mà không cần sửa logic lớn
- Có số đo latency trước/sau

---

### POC 3 — Native Serving Mart / Materialized View

**Mục tiêu:** Tạo native table/MV cho 2-3 dashboard quan trọng, benchmark performance.

**Checklist:**
- [ ] Chọn 2-3 dashboard chậm nhất hoặc quan trọng nhất
- [ ] Lấy SQL query thực tế từ Superset/Trino logs
- [ ] Liệt kê các bảng DTM liên quan
- [ ] Thiết kế native table: chọn table type, partition, bucket, sort key
- [ ] Load dữ liệu từ Iceberg DTM qua INSERT INTO SELECT
- [ ] Thiết kế refresh schedule (hourly/daily)
- [ ] Benchmark: P50/P95/P99 latency trước và sau
- [ ] Kiểm tra data freshness sau refresh

**Success criteria:**
- Dashboard latency giảm rõ rệt so với Trino
- Data freshness đáp ứng SLA
- Storage tăng thêm ở mức chấp nhận được

---

### POC 4 — Kafka Realtime Serving (nếu có nhu cầu)

**Mục tiêu:** Kiểm chứng ingest Kafka vào StarRocks cho 1 realtime dashboard.

**Checklist:**
- [ ] Chọn 1 topic đơn giản, schema ổn định
- [ ] Xác định event type: append-only hay upsert/delete
- [ ] Chọn table type: Duplicate Key hoặc Primary Key
- [ ] Cấu hình Routine Load / Kafka connector
- [ ] Tạo realtime dashboard trong Superset
- [ ] Đo end-to-end latency: event vào Kafka → hiện trên dashboard
- [ ] Đối soát KPI với flow qua Iceberg/Spark/DTM

**Success criteria:**
- End-to-end latency đạt yêu cầu (target < 30 giây)
- Không mất data ngoài kỳ vọng
- Có strategy xử lý late event và replay

---

## 10. Benchmark đề xuất

### 10.1 Nhóm chỉ số query

| Chỉ số | Mô tả |
|-------|-------|
| P50/P90/P95/P99 latency | Query latency percentile |
| Dashboard load time | Thời gian render toàn bộ dashboard |
| Query throughput | Số query/giây |
| Concurrent users | Test với 5, 10, 30, 50 users |
| CPU/memory/disk/network | Resource usage trên StarRocks nodes |
| MinIO/HMS load | Tải lên storage và metastore |
| Query failure rate | Tỉ lệ query lỗi |

### 10.2 Nhóm chỉ số data freshness

| Chỉ số | Mô tả |
|-------|-------|
| DTM → StarRocks lag | Từ khi Spark DTM job xong đến khi StarRocks thấy data |
| MV refresh time | Thời gian hoàn thành 1 lần refresh |
| Kafka → dashboard lag | Chỉ áp dụng phương án C |

### 10.3 Template ghi kết quả

| Query | Engine | Mode | P50 (ms) | P95 (ms) | P99 (ms) | Data scanned | Ghi chú |
|------|--------|------|-------:|-------:|-------:|-------------|---------|
| Q1: Revenue daily | Trino | Iceberg ext | TBD | TBD | TBD | TBD | Baseline |
| Q1: Revenue daily | StarRocks | Iceberg ext | TBD | TBD | TBD | TBD | POC A |
| Q1: Revenue daily | StarRocks | Native/MV | TBD | TBD | TBD | TBD | POC B |
| Q2: Top province | Trino | Iceberg ext | TBD | TBD | TBD | TBD | Baseline |
| Q2: Top province | StarRocks | Native/MV | TBD | TBD | TBD | TBD | POC B |

### 10.4 Các nhóm query cần benchmark

Nên lấy query thật từ Superset/Trino logs thay vì query giả:

1. Simple scan/filter theo partition
2. Aggregate theo ngày/tháng/khu vực
3. Join fact với dimension
4. Top N (ORDER BY + LIMIT)
5. Distinct count (COUNT DISTINCT)
6. Dashboard query lặp lại nhiều nhất
7. Dashboard query chậm nhất hiện tại

---

## 11. Rủi ro và Giới hạn

### 11.1 Rủi ro kỹ thuật

| Rủi ro | Mức độ | Cách giảm thiểu |
|-------|--------|----------------|
| External Iceberg query không nhanh hơn Trino | Trung bình | Benchmark thực tế trước khi quyết định |
| Small files trong Iceberg làm cả Trino lẫn StarRocks chậm | Cao | Chạy compaction trên Iceberg trước POC |
| Hive Metastore thành bottleneck metadata | Trung bình | Monitor HMS latency, scale HMS nếu cần |
| Schema evolution Iceberg → StarRocks native table | Cao | Thiết kế rõ quy trình alter table |
| MV refresh trên external catalog bị stale | Trung bình | Thiết kế refresh trigger sau Spark DTM job |
| CDC/Debezium vào StarRocks cần mapping operation | Cao | Test kỹ insert/update/delete mapping |

### 11.2 Rủi ro vận hành

| Rủi ro | Mức độ | Cách giảm thiểu |
|-------|--------|----------------|
| Thêm cluster cần vận hành | Cao | Bắt đầu nhỏ, mở rộng dần |
| Team chưa có kinh nghiệm StarRocks | Trung bình | POC trước, training song song |
| Credential management MinIO/HMS | Trung bình | Dùng secret management tool |

### 11.3 Rủi ro dữ liệu

| Rủi ro | Mức độ | Cách giảm thiểu |
|-------|--------|----------------|
| Native StarRocks lệch với Iceberg DTM | Trung bình | Data quality check định kỳ so sánh 2 nguồn |
| Realtime StarRocks lệch với batch DTM | Cao | Chấp nhận sự khác biệt có kiểm soát |
| Late event/out-of-order event trên Kafka | Trung bình | Thiết kế watermark và late event handling |

### 11.4 Cách giảm rủi ro tổng thể

- **Giữ Iceberg DTM là source of truth** — StarRocks chỉ là serving copy
- **Rollback path rõ ràng** — Superset dashboard có thể chuyển lại Trino bất cứ lúc nào
- **POC trên dashboard thật** trước khi production
- **Data quality check** so sánh StarRocks với Trino/DTM thường xuyên
- **Chỉ mở rộng native mart** sau khi có benchmark rõ ràng

---

## 12. Lộ trình triển khai

### Phase 0 — Chuẩn bị (1-2 tuần)

**Việc cần làm:**
- Chọn version StarRocks và môi trường test
- Xác định 3-5 dashboard/query dùng benchmark
- Lấy baseline latency từ Trino
- Chuẩn bị thông tin: HMS endpoint, MinIO endpoint, credential, network topology

**Deliverable:**
- Danh sách query benchmark với baseline Trino
- Sơ đồ kết nối network

---

### Phase 1 — External Iceberg Catalog POC (1-2 tuần)

**Việc cần làm:**
- Deploy StarRocks test cluster
- Tạo Iceberg external catalog
- Đọc DTM tables, chạy benchmark
- Kết nối Superset

**Deliverable:**
- Kết quả StarRocks external vs Trino
- Danh sách lỗi/tương thích
- Khuyến nghị có tiếp tục Phase 2 không

---

### Phase 2 — Native Serving Mart / MV POC (2-4 tuần)

**Việc cần làm:**
- Chọn dashboard hot nhất
- Thiết kế native table/MV (partition, bucket, sort key)
- Load dữ liệu từ Iceberg DTM
- Thiết kế refresh schedule
- Benchmark Superset dashboard

**Deliverable:**
- Kết quả performance trước/sau (P50/P95/P99)
- Data freshness report
- Storage estimate
- Khuyến nghị production

---

### Phase 3 — Production Hardening (3-6 tuần)

**Việc cần làm:**
- HA: FE x3, BE/CN x3+
- Monitoring/alerting (FE health, query latency, MV refresh status)
- Security: user/role, credential MinIO, TLS
- Backup/restore strategy
- CI/CD cho schema và refresh SQL
- Runbook vận hành

**Deliverable:**
- Production architecture
- Runbook
- SLA/SLO dashboard

---

### Phase 4 — Realtime Kafka POC (2-4 tuần, nếu cần)

**Việc cần làm:**
- Chọn 1 topic đơn giản để POC
- Thiết kế table type (Primary Key nếu cần upsert)
- Cấu hình Routine Load/Kafka connector
- Tạo realtime dashboard, đo latency
- Đối soát với Iceberg/DTM

**Deliverable:**
- Realtime latency report
- Consistency strategy Kafka vs DTM
- Khuyến nghị mở rộng hoặc dừng

---

## 13. Tiêu chí Go/No-Go

### Go nếu

- StarRocks đọc được Iceberg DTM ổn định qua HMS và MinIO
- Superset kết nối và query ổn định
- Dashboard native/MV nhanh hơn đáng kể so với Trino (target: ít nhất 2-3x trên P95)
- Refresh dữ liệu đáp ứng SLA (target: < 1 giờ cho dashboard quản trị)
- Chi phí vận hành chấp nhận được
- Có rollback path rõ ràng về Trino

### No-Go hoặc Delay nếu

- Không kết nối ổn định với Iceberg/MinIO/HMS
- Performance external không hơn Trino và native/MV không đủ lợi ích rõ ràng
- Refresh quá phức tạp hoặc dễ lệch dữ liệu
- Team chưa có năng lực vận hành thêm cluster
- Dashboard hiện tại chưa có vấn đề latency/concurrency thực sự

### Decision Matrix

| Tình huống | Khuyến nghị |
|-----------|------------|
| Chỉ muốn query lakehouse linh hoạt | Giữ Trino |
| Dashboard chậm, không muốn duplicate data | StarRocks external catalog (Phương án A) |
| Dashboard quan trọng cần SLA thấp | StarRocks native table/MV (Phương án B) |
| Dashboard cần dữ liệu gần realtime | StarRocks realtime table (Phương án C) |
| Transform MDM/DTM phức tạp | Giữ Spark |
| Source of truth lakehouse | Giữ Iceberg |
| Thay thế toàn bộ Trino | Không khuyến nghị |

---

## 14. Khuyến nghị cuối cùng

StarRocks phù hợp nhất với vai trò **bổ sung**, không phải thay thế:

```
Iceberg DTM (MinIO) — Source of Truth
    │
    ├──► Trino ──────────────────────────► Superset (ad-hoc, general dashboards)
    │    (federated query, exploration)
    │
    └──► StarRocks ──────────────────────► Superset (SLA dashboards)
         (BI serving, query acceleration)
```

**Không nên:** Triển khai StarRocks để thay thế toàn bộ Trino hoặc Spark.

**Nên:** Triển khai theo 3 bước:

1. **StarRocks external Iceberg catalog** — POC nhanh, kiểm chứng kết nối
2. **StarRocks native serving mart / materialized view** — tăng tốc dashboard quan trọng
3. **Kafka realtime serving** — chỉ cho use case thật sự cần near-realtime

**Kết luận:** StarRocks nên được đưa vào R&D với mục tiêu rõ là **"BI serving và query acceleration cho Superset trên nền Iceberg DTM"**, không phải **"thay thế lakehouse"**.

---

## 15. Appendix

### Appendix A — Superset Connection URI

```
starrocks://admin:<password>@starrocks-fe:9030/iceberg_dtm.dtm
```

Trong đó:
- `admin`: user StarRocks
- `starrocks-fe`: hostname FE node
- `9030`: FE MySQL query port
- `iceberg_dtm`: external catalog trong StarRocks
- `dtm`: database/schema trong Iceberg catalog

### Appendix B — Checklist kỹ thuật POC

**Network:**
- [ ] StarRocks FE reach Hive Metastore (port 9083)
- [ ] StarRocks BE/CN reach MinIO (port 9000)
- [ ] Superset reach StarRocks FE (port 9030)
- [ ] DNS/hostname resolve giữa các namespace/network

**Credential:**
- [ ] MinIO access key / secret key
- [ ] Hive Metastore access (nếu có auth)
- [ ] StarRocks user/password
- [ ] Superset database connection secret

**Data:**
- [ ] Chọn bảng DTM có partition theo thời gian
- [ ] Chọn bảng fact lớn (> 100M rows nếu có)
- [ ] Chọn dimension table
- [ ] Lấy 3-5 query thực tế từ Superset/Trino logs

**Test sequence:**
```sql
SHOW CATALOGS;
SHOW DATABASES FROM iceberg_dtm;
SHOW TABLES FROM iceberg_dtm.dtm;
SELECT COUNT(*) FROM iceberg_dtm.dtm.<fact_table>;
SELECT * FROM iceberg_dtm.dtm.<fact_table> WHERE report_date = '2026-06-01' LIMIT 10;
SELECT province_code, SUM(revenue) FROM iceberg_dtm.dtm.<fact_table>
  WHERE report_date >= '2026-01-01' GROUP BY province_code ORDER BY 2 DESC LIMIT 10;
```

### Appendix C — Data Modeling gợi ý cho Serving Mart

**Nguyên tắc chọn bảng đưa vào StarRocks:**
- Bảng phục vụ dashboard quan trọng / SLA cao
- Bảng được query nhiều (top query từ Trino logs)
- Bảng có aggregate/join lặp lại
- Chỉ dữ liệu hot (thường là 3-12 tháng gần nhất)

**Gợi ý table type theo use case:**

| Use case | Table type | Lý do |
|---------|-----------|-------|
| Event log, append-only | Duplicate Key | Không cần dedup |
| KPI pre-aggregated | Aggregate | Tự aggregate khi insert |
| Dimension table | Unique Key | Dedup theo natural key |
| CDC/realtime | Primary Key | Hỗ trợ upsert/delete |
| Fact table lớn cho BI | Duplicate Key + partition theo thời gian | Đủ linh hoạt |

**Refresh strategy theo SLA:**

| Loại dashboard | Refresh interval | Trigger |
|---------------|----------------|---------|
| Quản trị / báo cáo | 1-4 lần/ngày | Sau Spark DTM job hoàn tất |
| Vận hành | 5-15 phút/lần | Scheduled async MV |
| Near-realtime | Liên tục | Kafka → StarRocks realtime table |

### Appendix D — Nguồn tham khảo

- StarRocks Introduction: https://docs.starrocks.io/docs/introduction/StarRocks_intro/
- StarRocks Architecture: https://docs.starrocks.io/docs/introduction/Architecture/
- Iceberg Catalog: https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_catalog/
- Loading Overview: https://docs.starrocks.io/docs/loading/Loading_intro/
- Async Materialized Views: https://docs.starrocks.io/docs/using_starrocks/async_mv/Materialized_view/
- Primary Key Table: https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/
- Superset Integration: https://docs.starrocks.io/docs/integrations/BI_integrations/Superset/
- MinIO Shared-Data: https://docs.starrocks.io/docs/deployment/shared_data/minio/
- Indexes Overview: https://docs.starrocks.io/docs/table_design/indexes/
- Unload using INSERT INTO FILES: https://docs.starrocks.io/docs/unloading/unload_using_insert_into_files/
