"""
superset_config.py
"""

import os

# ─── Fix: pymysql thay thế MySQLdb ────────────────────────
# SQLAlchemy dùng mysql:// → tìm MySQLdb → không có → crash
# pymysql.install_as_MySQLdb() làm pymysql đóng vai MySQLdb
import pymysql
pymysql.install_as_MySQLdb()

# Secret key (override qua env var trong production)
SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY",
    "demo_secret_key_starrocks_2026"
)

# SQLite database cho Superset metadata (demo only)
# Production: dùng PostgreSQL
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# ─── Cache: Disable hoàn toàn cho realtime demo ───────────
# NullCache → mọi query đều đi thẳng xuống StarRocks
# Production: dùng Redis cache với TTL phù hợp
CACHE_CONFIG = {
    "CACHE_TYPE": "NullCache",
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "NullCache",
}

EXPLORE_FORM_DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "NullCache",
}

# ─── CSRF ─────────────────────────────────────────────────
# Tắt CSRF để setup_superset.py có thể gọi API
# Production: bật lại và dùng CSRF token đúng cách
WTF_CSRF_ENABLED = False

# ─── Feature flags ────────────────────────────────────────
FEATURE_FLAGS = {
    "DASHBOARD_AUTO_REFRESH_MODE": True,    # Cho phép auto-refresh dashboard
    "ENABLE_TEMPLATE_PROCESSING": True,     # SQL template
}

# ─── Row limit ────────────────────────────────────────────
ROW_LIMIT = 50_000
VIZ_ROW_LIMIT = 10_000

# ─── Dashboard auto-refresh ───────────────────────────────
# Các interval cho phép (giây)
DASHBOARD_AUTO_REFRESH_INTERVALS = [
    [0,    "Don't refresh"],
    [10,   "10 seconds"],
    [30,   "30 seconds"],
    [60,   "1 minute"],
    [300,  "5 minutes"],
    [1800, "30 minutes"],
    [3600, "1 hour"],
]
