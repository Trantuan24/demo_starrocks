# Demo: Kafka -> StarRocks -> Superset cho realtime BI serving

Ngày tạo: 2026-06-14  
Liên quan tài liệu R&D: `RD_StarRocks_Lakehouse_BI_Serving.md`  
Phương án R&D: Phương án C - StarRocks realtime serving từ Kafka  
Mục tiêu: Dựng demo tối thiểu để kiểm chứng luồng dữ liệu realtime từ Kafka vào StarRocks và hiển thị bằng Superset.

---

## 1. Mục tiêu demo

Demo này dùng để chứng minh phương án:

```mermaid
flowchart LR
  Kafka["Kafka topic"] --> StarRocks["StarRocks realtime table"]
  StarRocks --> Superset["Superset dashboard"]
```

Mục tiêu cụ thể:

- Kafka nhận event realtime giả lập.
- StarRocks ingest event từ Kafka bằng Routine Load hoặc Kafka load mechanism tương đương.
- StarRocks lưu dữ liệu vào bảng realtime.
- Superset kết nối StarRocks.
- Superset hiển thị dashboard realtime đơn giản.

Demo này không nhằm thay thế lakehouse hiện tại. Nó chỉ kiểm chứng riêng nhánh realtime serving trong R&D StarRocks.

---

## 2. Phạm vi demo

### 2.1 Thành phần cần dựng

Demo tối thiểu cần:

| Thành phần | Vai trò |
|---|---|
| Kafka | Message broker nhận event realtime |
| Kafka producer | Sinh dữ liệu demo vào Kafka topic |
| StarRocks | Ingest Kafka event và phục vụ query OLAP |
| Superset | BI dashboard query StarRocks |

### 2.2 Thành phần không cần dựng trong demo tối thiểu

Không cần các thành phần sau:

- Iceberg.
- MinIO.
- Hive Metastore.
- Trino.
- Spark MDM/DTM.
- NiFi.
- Debezium.
- Kafka Connect Iceberg Sink.

Lý do: demo này chỉ kiểm chứng nhánh realtime Kafka -> StarRocks -> Superset. Trong production, Kafka vẫn có thể đi song song vào Iceberg/lakehouse, nhưng nhánh đó không cần xuất hiện trong demo tối thiểu.

### 2.3 Vị trí demo trong kiến trúc production

Trong production, phương án C có thể nằm song song với lakehouse:

```mermaid
flowchart LR
  Kafka["Kafka topics"] --> StarRocksRT["StarRocks realtime serving"]
  Kafka -. "production path" .-> IcebergSink["Iceberg sink / Spark streaming"]
  IcebergSink -.-> Lakehouse["Iceberg Staging / MDM / DTM"]
  StarRocksRT --> Superset["Superset realtime dashboard"]
```

Nhưng trong demo, chỉ dựng nhánh realtime:

```mermaid
flowchart LR
  Producer["Mock producer"] --> Kafka["Kafka"]
  Kafka --> RoutineLoad["StarRocks Routine Load"]
  RoutineLoad --> Table["StarRocks rt_sales_events"]
  Table --> Superset["Superset"]
```

---

## 3. Use case demo đề xuất

Use case đơn giản: realtime sales events.

Mỗi event đại diện cho một đơn hàng hoặc giao dịch phát sinh từ hệ thống nghiệp vụ.

Ví dụ Kafka message dạng JSON:

```json
{
  "event_time": "2026-06-14 12:00:00",
  "order_id": 10001,
  "province": "Hanoi",
  "product": "Data Package",
  "amount": 120000,
  "payment_method": "CARD"
}
```

Các chỉ số dashboard:

- Tổng doanh thu theo thời gian.
- Số đơn theo thời gian.
- Doanh thu theo tỉnh/thành.
- Top sản phẩm theo doanh thu.
- Doanh thu theo phương thức thanh toán.

---

## 4. Kiến trúc triển khai demo

### 4.1 Luồng dữ liệu

1. Producer sinh JSON event.
2. Producer ghi event vào Kafka topic `sales_events`.
3. StarRocks Routine Load đọc topic `sales_events`.
4. StarRocks parse JSON và ghi vào bảng `demo.rt_sales_events`.
5. Superset kết nối StarRocks qua MySQL-compatible protocol/StarRocks SQLAlchemy dialect.
6. Superset tạo dataset và chart từ bảng `demo.rt_sales_events`.

### 4.2 Network port tham khảo

| Service | Port | Ghi chú |
|---|---:|---|
| Kafka broker | 9092 | Producer và StarRocks đọc Kafka |
| StarRocks FE query port | 9030 | MySQL protocol, Superset kết nối |
| StarRocks FE HTTP port | 8030 | UI/API nếu dùng |
| Superset | 8088 | Web UI |

Port thực tế có thể thay đổi theo deployment.

---

## 5. Cấu trúc thư mục demo đề xuất

Nếu cần tách thành project demo riêng, có thể dùng cấu trúc:

```text
starrocks-kafka-superset-demo/
  docker-compose.yml
  sql/
    01_create_database_and_table.sql
    02_create_routine_load.sql
    03_validation_queries.sql
  producer/
    produce_sales_events.py
  superset/
    README.md
  README.md
```

File Markdown này là hướng dẫn triển khai. Có thể dùng nó làm `README.md` cho demo repo.

---

## 6. Kế hoạch dựng demo

### Bước 1: Dựng Kafka

Yêu cầu:

- Kafka broker chạy được.
- Tạo topic `sales_events`.
- Producer có thể ghi message vào topic.
- Consumer test có thể đọc message.

Topic đề xuất:

```text
sales_events
```

Thông số demo:

```text
partitions: 3
replication.factor: 1
```

Với demo local, replication factor 1 là đủ. Với môi trường production, cần replication factor >= 3 nếu cluster Kafka có đủ broker.

### Bước 2: Dựng StarRocks

Yêu cầu:

- StarRocks FE chạy được.
- StarRocks BE/CN chạy được.
- Kết nối được bằng MySQL client qua port query của FE.
- StarRocks node reach được Kafka broker.

Kiểm tra kết nối StarRocks:

```bash
mysql -h <starrocks-fe-host> -P 9030 -u root
```

Lưu ý:

- Port `9030` là query port phổ biến của StarRocks FE.
- User/password tùy deployment.
- Nếu chạy bằng container, cần đảm bảo container StarRocks thấy được hostname Kafka.

### Bước 3: Tạo database và table trong StarRocks

Database demo:

```sql
CREATE DATABASE IF NOT EXISTS demo;
USE demo;
```

Bảng realtime dạng append-only:

```sql
CREATE TABLE IF NOT EXISTS rt_sales_events (
    event_time DATETIME NOT NULL,
    order_id BIGINT NOT NULL,
    province VARCHAR(64),
    product VARCHAR(128),
    amount DECIMAL(18,2),
    payment_method VARCHAR(32),
    ingest_time DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(event_time, order_id)
PARTITION BY date_trunc('day', event_time)
DISTRIBUTED BY HASH(order_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1"
);
```

Ghi chú:

- `DUPLICATE KEY` phù hợp với demo append-only event.
- `event_time` dùng để partition theo ngày.
- `order_id` dùng để distribution.
- `replication_num = 1` chỉ phù hợp demo local/single node.
- Nếu dữ liệu có update/delete hoặc CDC, nên đổi sang `PRIMARY KEY(order_id)`.

### Bước 4: Tạo Routine Load đọc Kafka

Routine Load tham khảo:

```sql
CREATE ROUTINE LOAD demo.rl_sales_events ON rt_sales_events
COLUMNS(event_time, order_id, province, product, amount, payment_method)
PROPERTIES
(
    "desired_concurrent_number" = "1",
    "format" = "json",
    "jsonpaths" = "[\"$.event_time\", \"$.order_id\", \"$.province\", \"$.product\", \"$.amount\", \"$.payment_method\"]",
    "strip_outer_array" = "false"
)
FROM KAFKA
(
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "sales_events",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
```

Lưu ý cần chỉnh theo môi trường:

- `kafka:9092` phải là địa chỉ Kafka mà StarRocks container/node resolve được.
- Nếu chạy Kafka ngoài Docker, có thể là `host.docker.internal:9092` hoặc IP thật.
- Nếu Kafka dùng SASL/SSL, cần thêm Kafka security properties.
- Nếu JSON field khác table column, cần chỉnh `jsonpaths` và `COLUMNS`.

Kiểm tra trạng thái Routine Load:

```sql
SHOW ROUTINE LOAD FOR rl_sales_events\G
```

Nếu cần tạm dừng:

```sql
PAUSE ROUTINE LOAD FOR rl_sales_events;
```

Nếu cần chạy lại:

```sql
RESUME ROUTINE LOAD FOR rl_sales_events;
```

Nếu cần xóa:

```sql
STOP ROUTINE LOAD FOR rl_sales_events;
```

---

## 7. Producer dữ liệu demo

Producer có thể là Python script, Kafka CLI, hoặc tool bất kỳ. Mục tiêu là bắn JSON event liên tục vào topic `sales_events`.

### 7.1 Logic producer

Mỗi 1 giây sinh một event:

- `event_time`: thời gian hiện tại.
- `order_id`: số tăng dần hoặc random.
- `province`: chọn ngẫu nhiên từ danh sách tỉnh/thành.
- `product`: chọn ngẫu nhiên từ danh sách sản phẩm.
- `amount`: random trong một khoảng.
- `payment_method`: CARD, CASH, BANKING, WALLET.

### 7.2 Ví dụ pseudo-code

```python
while True:
    event = {
        "event_time": now(),
        "order_id": next_id(),
        "province": random_choice(["Hanoi", "HCM", "Da Nang", "Can Tho"]),
        "product": random_choice(["Data Package", "Voice Package", "Device", "Service Fee"]),
        "amount": random_amount(),
        "payment_method": random_choice(["CARD", "CASH", "BANKING", "WALLET"])
    }
    kafka_producer.send("sales_events", event)
    sleep(1)
```

### 7.3 Test dữ liệu vào StarRocks

Sau khi producer chạy, kiểm tra trong StarRocks:

```sql
SELECT COUNT(*) FROM demo.rt_sales_events;
```

Kiểm tra 10 dòng mới nhất:

```sql
SELECT *
FROM demo.rt_sales_events
ORDER BY event_time DESC
LIMIT 10;
```

Kiểm tra aggregate realtime:

```sql
SELECT
    date_trunc('minute', event_time) AS minute_time,
    COUNT(*) AS order_count,
    SUM(amount) AS revenue
FROM demo.rt_sales_events
GROUP BY minute_time
ORDER BY minute_time DESC
LIMIT 20;
```

---

## 8. Kết nối Superset với StarRocks

### 8.1 Cài StarRocks SQLAlchemy dialect

Superset cần driver/dialect phù hợp để kết nối StarRocks. Theo tài liệu StarRocks, có thể dùng SQLAlchemy URI dạng:

```text
starrocks://<User>:<Password>@<Host>:<Port>/<Catalog>.<Database>
```

Ví dụ:

```text
starrocks://root:password@starrocks-fe:9030/default_catalog.demo
```

Nếu Superset chưa có dialect StarRocks, cần cài package Python tương ứng theo hướng dẫn của StarRocks/Superset integration.

### 8.2 Tạo database connection trong Superset

Trong Superset:

1. Vào Settings/Data -> Database Connections.
2. Chọn thêm database mới.
3. Nhập SQLAlchemy URI của StarRocks.
4. Test connection.
5. Save.

### 8.3 Tạo dataset

Tạo dataset từ bảng:

```text
default_catalog.demo.rt_sales_events
```

Các cột cần nhận đúng type:

- `event_time`: temporal column.
- `order_id`: numeric/string id.
- `province`: dimension.
- `product`: dimension.
- `amount`: metric source.
- `payment_method`: dimension.

---

## 9. Dashboard demo trong Superset

### 9.1 Chart 1: Realtime revenue by minute

Loại chart:

- Time-series line chart hoặc bar chart.

Dataset:

```text
rt_sales_events
```

Time column:

```text
event_time
```

Metric:

```sql
SUM(amount)
```

Time grain:

```text
minute
```

Mục tiêu:

- Khi producer đang chạy, chart tăng dần theo thời gian.

### 9.2 Chart 2: Order count by minute

Metric:

```sql
COUNT(*)
```

Time grain:

```text
minute
```

Mục tiêu:

- Thấy số lượng event/đơn hàng theo từng phút.

### 9.3 Chart 3: Revenue by province

Dimension:

```text
province
```

Metric:

```sql
SUM(amount)
```

Chart:

- Bar chart.
- Pie chart nếu muốn đơn giản.

### 9.4 Chart 4: Top products

Dimension:

```text
product
```

Metric:

```sql
SUM(amount)
```

Sort:

```text
SUM(amount) DESC
```

Limit:

```text
10
```

### 9.5 Dashboard refresh

Cấu hình dashboard refresh:

```text
5 seconds / 10 seconds / 30 seconds
```

Tùy khả năng môi trường demo. Không nên set refresh quá thấp nếu cluster yếu.

---

## 10. Tiêu chí nghiệm thu demo

Demo được coi là thành công nếu đáp ứng:

| Tiêu chí | Kỳ vọng |
|---|---|
| Kafka topic nhận event | Producer ghi được message liên tục |
| StarRocks Routine Load chạy | State RUNNING, không lỗi parse/network |
| Dữ liệu vào StarRocks | `COUNT(*)` tăng theo thời gian |
| Query realtime chạy được | Aggregate theo phút/tỉnh/sản phẩm có kết quả |
| Superset kết nối StarRocks | Test connection thành công |
| Superset chart hiển thị | Chart đọc được dữ liệu từ StarRocks |
| Dashboard refresh | Khi producer chạy, số liệu dashboard cập nhật |

---

## 11. Các lỗi thường gặp

### 11.1 StarRocks không kết nối được Kafka

Nguyên nhân thường gặp:

- Sai `kafka_broker_list`.
- Hostname Kafka không resolve được trong container StarRocks.
- Kafka advertised listener cấu hình sai.
- Network Docker không chung.
- Kafka bật security nhưng Routine Load chưa cấu hình SASL/SSL.

Cách kiểm tra:

- Từ container/node StarRocks kiểm tra kết nối tới Kafka.
- Kiểm tra advertised listeners của Kafka.
- Kiểm tra log Routine Load.

### 11.2 Routine Load không parse được JSON

Nguyên nhân thường gặp:

- JSON field không khớp `jsonpaths`.
- Type không khớp table column.
- `event_time` format không parse được sang DATETIME.
- Message có wrapper/envelope khác với JSON giả định.

Cách xử lý:

- Kiểm tra sample Kafka message.
- Chỉnh `jsonpaths`.
- Chỉnh `COLUMNS`.
- Chuẩn hóa timestamp format.

### 11.3 Superset không kết nối được StarRocks

Nguyên nhân thường gặp:

- Superset thiếu StarRocks SQLAlchemy dialect.
- URI sai format.
- Superset container không reach được StarRocks FE.
- Sai user/password.
- Port FE query không expose.

Cách xử lý:

- Cài đúng driver/dialect.
- Test bằng MySQL client từ cùng network với Superset.
- Kiểm tra URI `starrocks://user:password@host:9030/default_catalog.demo`.

### 11.4 Dashboard không cập nhật realtime

Nguyên nhân thường gặp:

- Dashboard cache trong Superset.
- Refresh interval chưa bật.
- Query time range không bao gồm event mới.
- Producer không còn chạy.
- Routine Load bị pause/error.

Cách xử lý:

- Disable/giảm cache trong dashboard demo.
- Set time range là "Last 15 minutes" hoặc tương đương.
- Kiểm tra `SHOW ROUTINE LOAD`.
- Kiểm tra `COUNT(*)` trong StarRocks.

---

## 12. Mapping với R&D StarRocks

Demo này phục vụ phần R&D sau:

| Mục trong R&D | Demo này kiểm chứng |
|---|---|
| Phương án C: StarRocks realtime serving từ Kafka | Có |
| Kafka -> StarRocks ingestion | Có |
| Superset -> StarRocks dashboard | Có |
| Iceberg/MinIO/Hive Metastore | Không, vì không thuộc demo tối thiểu |
| Trino comparison | Không, thuộc benchmark phương án A/B |
| Spark MDM/DTM | Không, vì realtime serving đi song song |

Kết quả demo nên trả lời được:

- StarRocks có ingest được Kafka realtime không?
- Superset có hiển thị được dữ liệu từ StarRocks không?
- Dashboard có cập nhật khi event mới vào Kafka không?
- Cơ chế này có đủ đơn giản để dùng cho một số realtime dashboard không?

---

## 13. Gợi ý báo cáo kết quả demo

Sau khi chạy demo, nên ghi lại:

| Nội dung | Kết quả |
|---|---|
| Số lượng event gửi vào Kafka | TBD |
| Số lượng row trong StarRocks | TBD |
| Độ trễ Kafka -> StarRocks | TBD |
| Query latency chart chính | TBD |
| Dashboard refresh interval | TBD |
| Lỗi gặp phải | TBD |
| Khuyến nghị tiếp theo | TBD |

Nên có screenshot:

- Kafka topic có message.
- `SHOW ROUTINE LOAD` state RUNNING.
- Query `COUNT(*)` trong StarRocks tăng.
- Superset dashboard hiển thị chart.

---

## 14. Khuyến nghị mở rộng sau demo

Nếu demo thành công, các bước tiếp theo:

1. Thử dữ liệu lớn hơn: tăng event rate từ 1 event/s lên 100-1000 event/s.
2. Thử nhiều partition Kafka hơn.
3. Benchmark latency Kafka -> StarRocks -> Superset.
4. Thử `PRIMARY KEY` table cho event có update/upsert.
5. Thử CDC/Debezium message nếu có use case thật.
6. Thử chạy song song: Kafka -> Iceberg và Kafka -> StarRocks.
7. Định nghĩa strategy reconcile giữa StarRocks realtime và Iceberg DTM.
8. Xây dựng monitoring cho Routine Load và query latency.

---

## 15. Kết luận

Để demo phương án 3 trong R&D StarRocks, chỉ cần dựng Kafka, StarRocks, Superset và một producer giả lập dữ liệu.

Không cần dựng toàn bộ lakehouse Iceberg/MinIO/Hive/Trino/Spark cho demo tối thiểu. Các thành phần lakehouse chỉ cần xuất hiện trong phần giải thích production context.

Demo thành công khi:

- Kafka nhận event.
- StarRocks ingest được event.
- Superset query StarRocks và hiển thị dashboard cập nhật theo thời gian.

Đây là bằng chứng kỹ thuật ban đầu để đánh giá StarRocks như một realtime serving layer cho một số dashboard cần latency thấp.

---

## 16. Nguồn tham khảo

- StarRocks Routine Load: https://docs.starrocks.io/docs/loading/RoutineLoad/
- StarRocks loading overview: https://docs.starrocks.io/docs/loading/Loading_intro/
- StarRocks Primary Key table: https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/
- StarRocks Superset integration: https://docs.starrocks.io/docs/integrations/BI_integrations/Superset/

