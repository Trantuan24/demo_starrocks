#!/bin/bash
# ════════════════════════════════════════════════════════════
# init_demo_b.sh  (Phương án B)
# Chạy trong mysql:8.0 container sau khi StarRocks + Iceberg REST + MinIO sẵn sàng.
#   1. Tạo Iceberg external catalog (REST)
#   2. Seed bảng Iceberg DTM (~1M dòng)
#   3. Tạo native serving mart + async MV
# ════════════════════════════════════════════════════════════

set -e

STARROCKS_HOST="${STARROCKS_HOST:-starrocks}"
STARROCKS_PORT="${STARROCKS_PORT:-9030}"
MINIO_USER="${MINIO_USER:-minio}"
MINIO_PASSWORD="${MINIO_PASSWORD:-minio12345}"
MYSQL_CMD="mysql -h ${STARROCKS_HOST} -P ${STARROCKS_PORT} -uroot --connect-timeout=10"

echo "=========================================="
echo "Demo B Initialization (Iceberg serving mart)"
echo "  StarRocks: ${STARROCKS_HOST}:${STARROCKS_PORT}"
echo "=========================================="

# ── 1. Đợi StarRocks FE ──
echo ""
echo "[1/5] Waiting for StarRocks FE..."
for i in $(seq 1 40); do
    if $MYSQL_CMD -e "SELECT 1" > /dev/null 2>&1; then
        echo "      OK: StarRocks FE is ready!"
        break
    fi
    echo "      Attempt $i/40: FE not ready, waiting 5s..."
    sleep 5
    if [ "$i" -eq 40 ]; then
        echo "      ERROR: StarRocks FE did not become ready in time."
        exit 1
    fi
done

# ── 2. Đợi StarRocks BE alive ──
echo ""
echo "[2/5] Waiting for StarRocks BE to register..."
for i in $(seq 1 24); do
    ALIVE_COUNT=$($MYSQL_CMD -e "SHOW BACKENDS;" 2>/dev/null | grep -ci "true") || ALIVE_COUNT=0
    if [ "$ALIVE_COUNT" -gt 0 ]; then
        echo "      OK: StarRocks BE is alive! ($ALIVE_COUNT backend)"
        break
    fi
    echo "      Attempt $i/24: BE not alive yet, waiting 10s..."
    sleep 10
    if [ "$i" -eq 24 ]; then
        echo "      WARNING: BE not confirmed alive, proceeding anyway..."
    fi
done

# ── 3. Tạo Iceberg external catalog (thay credential vào SQL) ──
echo ""
echo "[3/5] Creating Iceberg REST external catalog..."
SQL_CATALOG=$(sed -e "s/__MINIO_USER__/${MINIO_USER}/g" \
                  -e "s/__MINIO_PASSWORD__/${MINIO_PASSWORD}/g" \
                  /sql/B_01_iceberg_catalog.sql)
echo "$SQL_CATALOG" | grep -v '\\G' | $MYSQL_CMD
echo "      OK: catalog iceberg_dtm created."

# Sanity check: StarRocks reach được Iceberg REST + MinIO
echo "      Checking catalog reachability..."
$MYSQL_CMD -e "SHOW DATABASES FROM iceberg_dtm;" || {
    echo "      ERROR: cannot list databases from iceberg_dtm catalog."
    echo "      Check iceberg-rest (8181) and minio (9000) connectivity."
    exit 1
}

# ── 4. Seed Iceberg DTM ──
echo ""
echo "[4/5] Seeding Iceberg DTM (fact_sales ~1M rows). This can take a while..."
grep -v '\\G' /sql/B_02_seed_dtm.sql | $MYSQL_CMD
echo "      OK: DTM seeded."

# ── 5. Native mart + Materialized View ──
echo ""
echo "[5/5] Building native serving mart + materialized view..."
grep -v '\\G' /sql/B_03_native_and_mv.sql | $MYSQL_CMD
echo "      OK: serving.ads_revenue_daily + serving.mv_revenue_daily built."

echo ""
echo "=========================================="
echo "Demo B Initialization COMPLETE!"
echo ""
echo "  External catalog : iceberg_dtm (REST -> MinIO)"
echo "  DTM table        : iceberg_dtm.dtm.fact_sales"
echo "  Native mart      : serving.ads_revenue_daily"
echo "  Materialized View: serving.mv_revenue_daily"
echo ""
echo "  Next: python scripts/benchmark_b.py     (so sánh external vs native/MV)"
echo "        .\\scripts\\demo.ps1 setup-b        (đăng ký Superset)"
echo "        .\\scripts\\demo.ps1 dashboard-b    (tạo dashboard)"
echo "=========================================="
