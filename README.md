# Demo StarRocks: 2 phương án BI serving

Repo này chứa **2 demo** cho R&D StarRocks (`RD_StarRocks_Lakehouse_BI_Serving_v2.md`), dùng chung StarRocks + Superset, tách bằng Docker Compose **profiles**:

| Demo | Phương án | Luồng | Profile |
|---|---|---|---|
| **C** | Realtime serving từ Kafka | `Producer -> Kafka -> Routine Load -> demo.rt_sales_events -> Superset` | `c` |
| **B** | Native serving mart / MV trên Iceberg | `Iceberg DTM (MinIO+REST) -> StarRocks native mart/MV -> Superset` | `b` |

```powershell
.\scripts\demo.ps1 up c      # chạy demo C (realtime Kafka)
.\scripts\demo.ps1 up b      # chạy demo B (Iceberg serving mart)
.\scripts\demo.ps1 up all    # chạy cả hai
```

- Thiết kế demo C: `Demo_StarRocks_Kafka_Superset.md`
- Thiết kế demo B: `Demo_StarRocks_Iceberg_Mart.md`

Không demo nào dựng đầy đủ lakehouse (Spark, NiFi, Debezium, Trino). Demo B chỉ thêm MinIO + Iceberg REST catalog làm "kho DTM giả"; StarRocks tự seed mock data vào Iceberg, không cần Spark.

---

## DEMO C — Kafka -> StarRocks -> Superset (realtime)

Demo này kiểm chứng nhánh realtime BI serving:

```text
Mock Producer -> Kafka topic sales_events -> StarRocks Routine Load -> demo.rt_sales_events -> Superset
```

## 1. Thành phần

| Service | Vai trò | Host access |
|---|---|---|
| Kafka | Broker nhận event demo | `localhost:9094` |
| Producer | Sinh sales event JSON | container nội bộ |
| StarRocks | Routine Load từ Kafka và phục vụ OLAP query | `localhost:9030`, `localhost:8030` |
| Superset | Dashboard BI | `http://localhost:8088` |

Superset login mặc định:

```text
admin / admin
```

## 2. Chạy demo

Start toàn bộ services:

```powershell
.\scripts\demo.ps1 up
```

Hoặc dùng Docker Compose trực tiếp:

```powershell
docker compose up -d --build
```

Kiểm tra trạng thái:

```powershell
.\scripts\demo.ps1 status
```

Các container chính cần chạy:

```text
demo_kafka
demo_starrocks
demo_producer
demo_superset
```

`demo_starrocks_init` và `demo_kafka_init` là container init, chạy xong rồi exit là bình thường.

## 3. Test end-to-end

Cài dependency test trên host nếu chưa có:

```powershell
pip install -r scripts\requirements-test.txt
```

Chạy test:

```powershell
.\scripts\demo.ps1 test
```

Kết quả đúng:

```text
Results: 10/10 passed
```

Test kiểm tra các điểm sau:

| Test | Ý nghĩa |
|---|---|
| Kafka broker reachable | Host kết nối được Kafka qua `localhost:9094` |
| Kafka topic exists | Topic `sales_events` tồn tại |
| Kafka has messages | Producer đang ghi event vào topic |
| StarRocks FE connection | StarRocks MySQL port `9030` hoạt động |
| StarRocks BE alive | BE đã register và alive |
| StarRocks schema | DB `demo` và table `rt_sales_events` tồn tại |
| Routine Load RUNNING | Job `rl_sales_events` đang đọc Kafka |
| Data flowing | Row count trong StarRocks tăng theo thời gian |
| Aggregate query | Query BI aggregate chạy được |
| Superset accessible | Superset health endpoint trả HTTP 200 |

## 4. Setup Superset connection

Sau khi test pass, đăng ký connection StarRocks trong Superset:

```powershell
.\scripts\demo.ps1 setup
```

Script dùng URI MySQL-compatible để ổn định với Superset:

```text
mysql+pymysql://root:@starrocks:9030/demo
```

Sau đó mở Superset:

```text
http://localhost:8088
```

Vào SQL Lab, chọn database `StarRocks Demo`, test:

```sql
SELECT COUNT(*) FROM rt_sales_events;
```

## 5. Tạo dashboard demo

Tạo tự động dataset, chart và dashboard:

```powershell
.\scripts\demo.ps1 dashboard
```

Dashboard được tạo tại:

```text
http://localhost:8088/superset/dashboard/starrocks-realtime-sales/
```

Script tạo các chart:

- `Total Orders`
- `Realtime Revenue by Minute`
- `Orders by Minute`
- `Revenue by Province`
- `Top Products`
- `Payment Method Split`
- `Latest Sales Events`

Nếu muốn tạo thủ công trong UI, dùng dataset sau:

Tạo dataset từ:

```text
Database: StarRocks Demo
Schema: demo
Table: rt_sales_events
```

Chart khuyến nghị:

| Chart | Type | Time column | Metric | Dimension |
|---|---|---|---|---|
| Revenue by minute | Time-series line/bar | `event_time` | `SUM(amount)` | |
| Orders by minute | Time-series bar | `event_time` | `COUNT(*)` | |
| Revenue by province | Bar | | `SUM(amount)` | `province` |
| Top products | Bar | | `SUM(amount)` | `product` |
| Payment method | Pie/bar | | `COUNT(*)` | `payment_method` |

Dashboard refresh nên để `30 seconds` cho demo local.

## 6. Lệnh vận hành nhanh

```powershell
.\scripts\demo.ps1 status        # Xem container
.\scripts\demo.ps1 logs          # Xem logs tất cả services
.\scripts\demo.ps1 producer      # Xem producer logs
.\scripts\demo.ps1 superset      # Xem Superset logs
.\scripts\demo.ps1 routine-load  # Xem trạng thái Routine Load
.\scripts\demo.ps1 count         # Row count trong StarRocks
.\scripts\demo.ps1 mysql         # Mở MySQL shell vào StarRocks
.\scripts\demo.ps1 dashboard     # Tạo lại Superset dashboard
.\scripts\demo.ps1 down          # Stop services
.\scripts\demo.ps1 clean         # Stop và xóa volumes demo
```

## 7. Query kiểm chứng thủ công

Mở MySQL shell:

```powershell
.\scripts\demo.ps1 mysql
```

Query:

```sql
SELECT COUNT(*) FROM rt_sales_events;

SHOW ROUTINE LOAD FROM demo\G

SELECT
    date_trunc('minute', event_time) AS minute_time,
    COUNT(*) AS order_count,
    SUM(amount) AS revenue
FROM rt_sales_events
GROUP BY minute_time
ORDER BY minute_time DESC
LIMIT 10;
```

## 8. Ghi chú kỹ thuật

- Bảng `demo.rt_sales_events` dùng `DUPLICATE KEY(event_time, order_id)`, phù hợp event append-only.
- `DUPLICATE KEY` không deduplicate. Demo giả định `order_id` unique.
- Nếu test CDC/upsert/update/delete, đổi sang `PRIMARY KEY(order_id)`.
- Routine Load đang dùng `OFFSET_BEGINNING`, phù hợp demo. Nếu stop/recreate job, dữ liệu cũ trong topic có thể được đọc lại.
- Superset realtime ở đây là near-realtime bằng auto-refresh/polling, không phải streaming push.
- `replication_num = 1` chỉ phù hợp local demo một node.

## 9. Kịch bản nghiệm thu

Demo đạt nếu:

1. `.\scripts\demo.ps1 test` trả `10/10 passed`.
2. `SHOW ROUTINE LOAD FROM demo\G` cho thấy job `rl_sales_events` ở state `RUNNING`.
3. `SELECT COUNT(*) FROM rt_sales_events` tăng theo thời gian.
4. Superset mở được ở `http://localhost:8088`.
5. Superset query được database `StarRocks Demo`.
6. Dashboard `StarRocks Realtime Sales` mở được tại `/superset/dashboard/starrocks-realtime-sales/`.

---

## DEMO B — Iceberg DTM -> StarRocks native mart/MV -> Superset

Demo B chứng minh Phương án B: StarRocks đọc Iceberg DTM, vật chất hóa thành native table / Materialized View, phục vụ Superset nhanh hơn scan Iceberg trực tiếp. Chi tiết: `Demo_StarRocks_Iceberg_Mart.md`.

### B.1 Chạy

```powershell
.\scripts\demo.ps1 up b
docker logs -f demo_starrocks_init_b    # theo dõi: tạo catalog -> seed 1M dòng -> native mart + MV
```

Init tự động: tạo Iceberg REST catalog `iceberg_dtm`, seed `iceberg_dtm.dtm.fact_sales` (~1M dòng), tạo `serving.ads_revenue_daily` + `serving.mv_revenue_daily`.

### B.2 Benchmark (trái tim demo B)

```powershell
pip install -r scripts\requirements-test.txt
.\scripts\demo.ps1 benchmark
```

So sánh latency external Iceberg vs native mart trên cùng query, kiểm tra data parity, xác nhận MV query-rewrite.

### B.3 Superset

```powershell
.\scripts\demo.ps1 setup-b        # đăng ký connection "StarRocks Serving (B)"
.\scripts\demo.ps1 dashboard-b    # tạo dashboard
```

Dashboard: `http://localhost:8088/superset/dashboard/starrocks-iceberg-mart/`

### B.4 Lệnh nhanh cho B

```powershell
.\scripts\demo.ps1 count-b        # row count DTM + native mart
.\scripts\demo.ps1 mysql-b        # MySQL shell vào db serving
```

| Service B | Port |
|---|---|
| MinIO API / Console | `localhost:9000` / `localhost:9001` (minio/minio12345) |
| Iceberg REST catalog | `localhost:8181` |

---

## Tài liệu liên quan

- `RD_StarRocks_Lakehouse_BI_Serving_v2.md`: bối cảnh R&D tổng thể và vai trò StarRocks trong lakehouse.
- `Demo_StarRocks_Kafka_Superset.md`: thiết kế demo Phương án C - realtime serving từ Kafka.
- `Demo_StarRocks_Iceberg_Mart.md`: thiết kế demo Phương án B - native serving mart / MV trên Iceberg.
