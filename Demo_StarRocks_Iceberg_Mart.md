# Demo: Iceberg DTM -> StarRocks -> Superset cho BI serving / query acceleration

Ngày tạo: 2026-06-17
Liên quan tài liệu R&D: `RD_StarRocks_Lakehouse_BI_Serving_v2.md`
Phương án R&D: **Phương án B - StarRocks làm native BI serving mart / Materialized View**
Mục tiêu: Dựng demo tối thiểu chứng minh StarRocks đọc Iceberg DTM, vật chất hóa thành native table / MV, và phục vụ Superset nhanh hơn so với scan Iceberg trực tiếp.

---

## 1. Demo này chứng minh gì

```text
Iceberg DTM (MinIO + REST catalog)
        │  (1) StarRocks đọc qua External Catalog — KHÔNG copy
        ▼
StarRocks
        │  (2) INSERT INTO SELECT  → native mart  (serving.ads_revenue_daily)
        │  (2) CREATE MATERIALIZED VIEW → MV       (serving.mv_revenue_daily)
        ▼
Superset dashboard  ←  query vào native mart (latency thấp)
```

Câu chuyện cốt lõi của Phương án B:

1. **External catalog (baseline):** StarRocks query thẳng Iceberg trên MinIO — đúng nhưng phải scan file Parquet mỗi lần.
2. **Native mart / MV (tăng tốc):** vật chất hóa dữ liệu pre-aggregated thành bảng columnar local → query nhanh hơn nhiều lần.
3. **Benchmark:** đo latency cùng một câu hỏi BI trên 2 nguồn để định lượng mức tăng tốc.

> Đây **không** phải thay thế lakehouse. Iceberg DTM vẫn là source of truth; StarRocks chỉ là serving copy có thể rebuild bất cứ lúc nào.

---

## 2. Vì sao KHÔNG cần dựng cả lakehouse

Phương án B trong production đọc Iceberg DTM do Spark sinh ra. Nhưng để **kiểm chứng riêng nhánh B**, ta chỉ cần một "kho Iceberg DTM giả" cho StarRocks đọc. Cụ thể demo này:

| Thành phần | Có trong demo B? | Vai trò |
|---|---|---|
| MinIO | ✅ | Object storage S3 lưu file Iceberg |
| Iceberg REST catalog | ✅ | Catalog quản lý metadata Iceberg (thay Hive Metastore cho gọn) |
| StarRocks | ✅ | External catalog + native mart + MV + query |
| Superset | ✅ | BI dashboard |
| Spark / NiFi / Debezium / Kafka / Trino | ❌ | Không cần — StarRocks **tự ghi** mock data vào Iceberg |
| Hive Metastore | ❌ | Thay bằng REST catalog (nhẹ hơn cho demo local) |

**Mẹo then chốt:** StarRocks (>= v3.1) ghi được thẳng vào Iceberg. Nên ta tạo bảng `fact_sales` và bơm ~1 triệu dòng mock **bằng chính StarRocks**, không cần Spark/pyiceberg.

### REST catalog vs Hive Metastore

Demo dùng **Iceberg REST catalog** cho gọn (1 container, không cần DB backend). Khác biệt với production (dùng Hive Metastore) chỉ nằm ở ~5 dòng config khi khai báo catalog:

```sql
-- REST (demo này)
"iceberg.catalog.type" = "rest",
"iceberg.catalog.uri"  = "http://iceberg-rest:8181"

-- Hive Metastore (production)
"iceberg.catalog.type" = "hive",
"hive.metastore.uris"  = "thrift://hive-metastore:9083"
```

Toàn bộ phần "đáng giá" của demo (native mart, MV, query rewrite, benchmark) **giống hệt** dù dùng catalog nào.

---

## 3. Thành phần và port

| Service | Vai trò | Host access |
|---|---|---|
| MinIO | S3 storage cho Iceberg | API `localhost:9000`, Console `localhost:9001` |
| Iceberg REST | Catalog Iceberg | `localhost:8181` |
| StarRocks | External catalog + serving mart | `localhost:9030` (MySQL), `localhost:8030` (FE UI) |
| Superset | BI dashboard | `http://localhost:8088` (admin/admin) |

MinIO console login: `minio` / `minio12345`.

---

## 4. Chạy demo

Demo B và demo C nằm chung repo, tách bằng Docker Compose **profiles**. Demo B dùng profile `b`:

```powershell
.\scripts\demo.ps1 up b          # bật MinIO + Iceberg REST + StarRocks + Superset
```

Quá trình init (`demo_starrocks_init_b`) sẽ tự động:

1. Tạo Iceberg external catalog `iceberg_dtm` (REST -> MinIO).
2. Tạo bảng `iceberg_dtm.dtm.fact_sales` và seed ~1,000,000 dòng.
3. Tạo native mart `serving.ads_revenue_daily` (INSERT INTO SELECT).
4. Tạo + refresh materialized view `serving.mv_revenue_daily`.

Theo dõi init:

```powershell
docker logs -f demo_starrocks_init_b
```

`demo_minio_init` và `demo_starrocks_init_b` là container init — chạy xong rồi exit là bình thường.

---

## 5. Benchmark — trái tim của demo B

```powershell
pip install -r scripts\requirements-test.txt   # nếu chưa có pymysql
.\scripts\demo.ps1 benchmark
```

Script chạy cùng một câu hỏi BI trên **external Iceberg** vs **native mart**, mỗi query nhiều lần lấy median, in bảng so sánh + speedup, kiểm tra data parity, và xác nhận MV query-rewrite.

Kết quả mong đợi: native mart nhanh hơn external Iceberg nhiều lần (thường vài x đến hàng chục x tùy data và cache), tổng doanh thu/đơn khớp nhau giữa 2 nguồn.

Kiểm tra row count nhanh:

```powershell
.\scripts\demo.ps1 count-b
```

---

## 6. Superset

```powershell
.\scripts\demo.ps1 setup-b        # đăng ký connection "StarRocks Serving (B)" -> db serving
.\scripts\demo.ps1 dashboard-b    # tạo dashboard trên native mart
```

Dashboard: `http://localhost:8088/superset/dashboard/starrocks-iceberg-mart/`

Các chart: Total Revenue, Total Orders, Revenue by Day, Revenue by Province, Top Products, Revenue Mart Detail.

Mở SQL Lab, database **StarRocks Serving (B)**:

```sql
SELECT * FROM ads_revenue_daily LIMIT 10;
```

---

## 7. Query kiểm chứng thủ công

```powershell
.\scripts\demo.ps1 mysql-b
```

```sql
-- External Iceberg (baseline)
SELECT province, SUM(amount) FROM iceberg_dtm.dtm.fact_sales GROUP BY province;

-- Native mart (tăng tốc)
SELECT province, SUM(revenue) FROM serving.ads_revenue_daily GROUP BY province;

-- Chứng minh MV query-rewrite: tìm 'mv_revenue_daily' trong plan
EXPLAIN SELECT province, SUM(amount) FROM iceberg_dtm.dtm.fact_sales GROUP BY province;

-- Refresh lại MV khi DTM thay đổi
REFRESH MATERIALIZED VIEW serving.mv_revenue_daily WITH SYNC MODE;
```

---

## 8. Ghi chú kỹ thuật

- **Source of truth** vẫn là Iceberg DTM. `serving.*` chỉ là serving copy, rebuild được bằng cách chạy lại `B_03_native_and_mv.sql`.
- **Refresh:** MV để `REFRESH ASYNC`. Trong production nên trigger refresh sau khi Spark DTM job xong (hourly/daily tùy SLA dashboard).
- **`replication_num = 1`** chỉ phù hợp demo single-node. Production dùng >= 3.
- **Mock data:** sinh bằng numbers generator (cross join bảng digits 0-9), trải đều trên 90 ngày để partition pruning có ý nghĩa. Sửa số dòng / khoảng thời gian trong `sql/B_02_seed_dtm.sql`.
- **Schema evolution:** demo này không xử lý; production cần quy trình ALTER khi schema Iceberg đổi (xem mục Rủi ro trong file R&D).

---

## 9. Tiêu chí nghiệm thu demo

| Tiêu chí | Kỳ vọng |
|---|---|
| StarRocks đọc Iceberg qua REST catalog | `SHOW DATABASES FROM iceberg_dtm` chạy được |
| Seed DTM thành công | `COUNT(*)` trên `fact_sales` ~ 1,000,000 |
| Native mart build được | `serving.ads_revenue_daily` có dữ liệu |
| MV build + refresh | `serving.mv_revenue_daily` xuất hiện trong `information_schema.materialized_views` |
| Benchmark | native mart nhanh hơn external rõ rệt; data parity khớp |
| Superset | dashboard `starrocks-iceberg-mart` mở được và hiển thị số liệu |

---

## 10. Mapping với R&D StarRocks

| Mục trong R&D v2 | Demo này kiểm chứng |
|---|---|
| Phương án B: native serving mart / MV | ✅ |
| External Iceberg catalog (Phương án A, làm nền cho B) | ✅ (đọc Iceberg qua catalog) |
| INSERT INTO SELECT từ Iceberg DTM | ✅ |
| Async Materialized View + query rewrite | ✅ |
| Benchmark external vs native/MV (mục 10) | ✅ |
| Hive Metastore | ⚠️ thay bằng REST catalog cho gọn |
| Spark MDM/DTM | ❌ thay bằng StarRocks tự seed Iceberg |
| Kafka realtime (Phương án C) | ❌ xem `Demo_StarRocks_Kafka_Superset.md` |

---

## 11. Kết luận

Để demo Phương án B, **không cần dựng cả lakehouse** — chỉ cần MinIO + Iceberg REST catalog làm "kho DTM giả", còn StarRocks tự seed dữ liệu và tự vật chất hóa. Demo thành công khi benchmark cho thấy native mart/MV tăng tốc đáng kể so với scan Iceberg trực tiếp, đồng thời dữ liệu khớp với nguồn Iceberg — đúng vai trò "BI serving / query acceleration trên nền Iceberg DTM" mà R&D đề xuất.
