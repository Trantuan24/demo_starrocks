#!/bin/bash
# ════════════════════════════════════════════════════════════
# entrypoint.sh - Superset startup script
# Chạy init DB, tạo admin user, load examples, start server
# ════════════════════════════════════════════════════════════

set -e

echo "=========================================="
echo "Superset Initialization"
echo "=========================================="

# ─── Tạo thư mục home ─────────────────────────────────────
mkdir -p /app/superset_home
export SUPERSET_HOME=/app/superset_home

# ─── Init Superset metadata DB ────────────────────────────
echo "[1/4] Upgrading Superset metadata DB..."
superset db upgrade

# ─── Tạo admin user ───────────────────────────────────────
echo "[2/4] Creating admin user..."
superset fab create-admin \
    --username  "${ADMIN_USERNAME:-admin}" \
    --firstname "Admin" \
    --lastname  "User" \
    --email     "${ADMIN_EMAIL:-admin@demo.local}" \
    --password  "${ADMIN_PASSWORD:-admin}" \
    2>/dev/null || echo "  (Admin user already exists, skipping)"

# ─── Init roles và permissions ────────────────────────────
echo "[3/4] Initializing roles..."
superset init

# ─── Start Superset ───────────────────────────────────────
echo "[4/4] Starting Superset on port 8088..."
echo ""
echo "=========================================="
echo "✅ Superset is starting!"
echo ""
echo "  URL     : http://localhost:8088"
echo "  Username: ${ADMIN_USERNAME:-admin}"
echo "  Password: ${ADMIN_PASSWORD:-admin}"
echo ""
echo "  After startup, run:"
echo "  python scripts/setup_superset.py"
echo "  to auto-register StarRocks connection"
echo "=========================================="

exec superset run \
    --host 0.0.0.0 \
    --port 8088 \
    --with-threads
