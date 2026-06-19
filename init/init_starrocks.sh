#!/bin/bash
# ════════════════════════════════════════════════════════════
# init_starrocks.sh
# Chạy trong mysql:8.0 container sau khi StarRocks healthy
# Tạo database, table, và Routine Load
# ════════════════════════════════════════════════════════════

set -e

STARROCKS_HOST="${STARROCKS_HOST:-starrocks}"
STARROCKS_PORT="${STARROCKS_PORT:-9030}"
MYSQL_CMD="mysql -h ${STARROCKS_HOST} -P ${STARROCKS_PORT} -uroot --connect-timeout=10"

echo "=========================================="
echo "StarRocks Initialization"
echo "  Host: ${STARROCKS_HOST}:${STARROCKS_PORT}"
echo "=========================================="

# ──────────────────────────────────────────────────────────
# Bước 1: Đợi StarRocks FE sẵn sàng
# ──────────────────────────────────────────────────────────
echo ""
echo "[1/4] Waiting for StarRocks FE..."
for i in $(seq 1 40); do
    if $MYSQL_CMD -e "SELECT 1" > /dev/null 2>&1; then
        echo "      ✅ StarRocks FE is ready!"
        break
    fi
    echo "      ⏳ Attempt $i/40: FE not ready, waiting 5s..."
    sleep 5
    if [ "$i" -eq 40 ]; then
        echo "      ❌ ERROR: StarRocks FE did not become ready in time."
        exit 1
    fi
done

# ──────────────────────────────────────────────────────────
# Bước 2: Đợi StarRocks BE alive
# Trong allin1 image, BE cần thêm 30-60s sau khi FE ready
# ──────────────────────────────────────────────────────────
echo ""
echo "[2/4] Waiting for StarRocks BE to register..."
for i in $(seq 1 24); do
    # Đếm số dòng có 'true' trong SHOW BACKENDS (cột Alive)
    ALIVE_COUNT=$($MYSQL_CMD -e "SHOW BACKENDS;" 2>/dev/null | grep -ci "true" || echo 0)
    if [ "$ALIVE_COUNT" -gt 0 ]; then
        echo "      ✅ StarRocks BE is alive! ($ALIVE_COUNT backend registered)"
        break
    fi
    echo "      ⏳ Attempt $i/24: BE not alive yet, waiting 10s..."
    sleep 10
    if [ "$i" -eq 24 ]; then
        echo "      ⚠️  WARNING: BE not confirmed alive, proceeding anyway..."
        echo "      (Check 'SHOW BACKENDS' manually after startup)"
    fi
done

# ──────────────────────────────────────────────────────────
# Bước 3: Tạo Database và Table
# ──────────────────────────────────────────────────────────
echo ""
echo "[3/4] Creating database and table..."
$MYSQL_CMD < /sql/01_create_database_and_table.sql
echo "      ✅ Database 'demo' and table 'rt_sales_events' created!"

# ──────────────────────────────────────────────────────────
# Bước 4: Tạo Routine Load
# Stop existing nếu có (idempotent re-run)
# ──────────────────────────────────────────────────────────
echo ""
echo "[4/4] Creating Routine Load..."

# Check nếu đã tồn tại thì stop trước
EXISTING=$($MYSQL_CMD -e "SHOW ROUTINE LOAD FROM demo;" 2>/dev/null | grep -c "rl_sales_events" || echo 0)
if [ "$EXISTING" -gt 0 ]; then
    echo "      Found existing Routine Load, stopping first..."
    $MYSQL_CMD demo -e "STOP ROUTINE LOAD FOR rl_sales_events;" 2>/dev/null || true
    sleep 3
fi

$MYSQL_CMD < /sql/02_create_routine_load.sql 2>/dev/null || {
    # Remove \G from file (not valid in non-interactive mode) and retry
    grep -v "\\\\G" /sql/02_create_routine_load.sql | $MYSQL_CMD
}

# Verify Routine Load state
sleep 3
echo ""
echo "      Verifying Routine Load..."
RL_STATE=$($MYSQL_CMD demo -e "SHOW ROUTINE LOAD FROM demo;" 2>/dev/null | grep "rl_sales_events" | awk '{print $6}' || echo "UNKNOWN")
echo "      State: $RL_STATE"

echo ""
echo "=========================================="
echo "✅ StarRocks Initialization COMPLETE!"
echo ""
echo "  Database : demo"
echo "  Table    : rt_sales_events"
echo "  Load Job : rl_sales_events"
echo ""
echo "  Next: wait for producer to send events"
echo "  Then: SELECT COUNT(*) FROM demo.rt_sales_events;"
echo "=========================================="
