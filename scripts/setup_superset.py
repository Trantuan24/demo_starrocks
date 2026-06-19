#!/usr/bin/env python3
"""
setup_superset.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Auto-register StarRocks database connection trong Superset
qua REST API.

Chạy sau khi tất cả services đã up:
    python scripts/setup_superset.py

Sau khi chạy:
    1. Mở http://localhost:8088
    2. Vào SQL Lab → chọn database "StarRocks Demo"
    3. Query: SELECT COUNT(*) FROM rt_sales_events
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import sys
import io
import time
import json
import urllib.request
import urllib.error
from urllib.parse import urlencode

# Fix Windows terminal encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

SUPERSET_URL  = "http://localhost:8088"
ADMIN_USER    = "admin"
ADMIN_PASS    = "admin"

# StarRocks is MySQL-compatible. Use mysql+pymysql for this demo because the
# starrocks SQLAlchemy dialect can fail during Superset dataset reflection on
# some versions.
STARROCKS_URI = "mysql+pymysql://root:@starrocks:9030/demo"
DB_NAME       = "StarRocks Demo"


def post_json(url: str, data: dict, headers: dict = None) -> dict:
    """Simple HTTP POST helper (no requests library needed)."""
    body    = json.dumps(data).encode("utf-8")
    req     = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)

    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_json(url: str, headers: dict = None) -> dict:
    req = urllib.request.Request(url, method="GET")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def wait_for_superset(max_wait: int = 120):
    """Đợi Superset sẵn sàng."""
    print(f"Waiting for Superset at {SUPERSET_URL}...")
    for i in range(max_wait // 5):
        try:
            urllib.request.urlopen(f"{SUPERSET_URL}/health", timeout=5)
            print("✅ Superset is ready!")
            return
        except Exception:
            print(f"  ⏳ Not ready yet ({i*5}s/{max_wait}s)...")
            time.sleep(5)
    raise TimeoutError("Superset did not become ready in time")


def get_csrf_token(session_cookie: str) -> str:
    """Lấy CSRF token."""
    try:
        req = urllib.request.Request(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
        req.add_header("Cookie", session_cookie)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            return data.get("result", "")
    except Exception:
        return ""


def login() -> tuple[str, str]:
    """Login và trả về (access_token, session_cookie)."""
    print(f"Logging in as '{ADMIN_USER}'...")

    # JWT login (v1 API)
    try:
        resp = post_json(
            f"{SUPERSET_URL}/api/v1/security/login",
            {
                "username": ADMIN_USER,
                "password": ADMIN_PASS,
                "provider": "db",
                "refresh": True,
            }
        )
        token = resp.get("access_token", "")
        if token:
            print("✅ Login successful (JWT)")
            return token, ""
    except Exception as e:
        print(f"  JWT login failed: {e}")

    raise RuntimeError("Could not login to Superset")


def find_db_id(headers: dict) -> int | None:
    """Tìm database connection đã tồn tại."""
    try:
        data = get_json(f"{SUPERSET_URL}/api/v1/database/?q=(filters:!())", headers=headers)
        for db in data.get("result", []):
            if db.get("database_name") == DB_NAME:
                db_id = db["id"]
                print(f"  Found existing database connection '{DB_NAME}' (id={db_id})")
                return db_id
        return None
    except Exception:
        return None


def delete_database_connection(db_id: int, headers: dict):
    """Xóa connection cũ để đảm bảo URI luôn đúng."""
    print(f"Deleting existing database connection (id={db_id})...")
    req = urllib.request.Request(
        f"{SUPERSET_URL}/api/v1/database/{db_id}",
        headers=headers,
        method="DELETE",
    )
    with urllib.request.urlopen(req, timeout=15):
        pass


def create_database_connection(headers: dict) -> dict:
    """Tạo StarRocks database connection trong Superset."""
    print(f"Creating database connection '{DB_NAME}'...")

    payload = {
        "database_name":     DB_NAME,
        "sqlalchemy_uri":    STARROCKS_URI,
        "expose_in_sqllab":  True,
        "allow_run_async":   False,
        "allow_ctas":        False,
        "allow_cvas":        False,
        "allow_dml":         False,
        "extra": json.dumps({
            "metadata_params": {},
            "engine_params": {},
            "metadata_cache_timeout": {},
        }),
    }

    resp = post_json(
        f"{SUPERSET_URL}/api/v1/database/",
        payload,
        headers=headers
    )
    return resp


def test_connection(db_id: int, headers: dict) -> bool:
    """Test database connection."""
    print(f"Testing connection (id={db_id})...")
    try:
        req  = urllib.request.Request(
            f"{SUPERSET_URL}/api/v1/database/{db_id}/connection",
            method="GET"
        )
        for k, v in headers.items():
            req.add_header(k, v)
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.getcode() == 200
    except Exception:
        return False


def main():
    print()
    print("=" * 55)
    print("🔧 Setup: Auto-register StarRocks in Superset")
    print("=" * 55)

    # 1. Đợi Superset
    wait_for_superset()

    # 2. Login
    access_token, _ = login()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type":  "application/json",
        "Referer":       SUPERSET_URL,
    }

    # 3. Recreate connection để loại bỏ URI cũ/sai nếu đã tồn tại
    existing_db_id = find_db_id(headers)
    if existing_db_id:
        delete_database_connection(existing_db_id, headers)

    # 4. Tạo mới
    try:
        result = create_database_connection(headers)
        db_id  = result.get("id") or result.get("result", {}).get("id")
        if db_id:
            print(f"✅ Database connection created (id={db_id})")
        else:
            print(f"⚠️  Response: {json.dumps(result, indent=2)}")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"❌ Failed to create connection: HTTP {e.code}")
        print(f"   Response: {body}")
        print()
        print("💡 Manual setup:")
        print(f"   1. Open http://localhost:8088")
        print(f"   2. Settings → Database Connections → + Database")
        print(f"   3. Use SQLAlchemy URI:")
        print(f"   {STARROCKS_URI}")
        sys.exit(1)

    # 5. Summary
    print()
    print("=" * 55)
    print("✅ Superset is configured!")
    print()
    print("  Open   : http://localhost:8088")
    print("  Login  : admin / admin")
    print()
    print("  SQL Lab → Database: StarRocks Demo")
    print("  Try query:")
    print("    SELECT COUNT(*) FROM rt_sales_events;")
    print()
    print("  Create charts from table:")
    print("    demo.rt_sales_events")
    print("=" * 55)
    print()


if __name__ == "__main__":
    main()
