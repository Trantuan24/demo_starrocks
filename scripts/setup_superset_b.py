#!/usr/bin/env python3
"""
setup_superset_b.py  (Phương án B)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Auto-register connection StarRocks (database `serving` - native mart)
trong Superset qua REST API.

Chạy sau khi demo B đã up và init xong:
    python scripts/setup_superset_b.py

Sau đó: SQL Lab -> database "StarRocks Serving (B)" -> query serving.ads_revenue_daily
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import sys
import io
import time
import json
import urllib.request
import urllib.error

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

SUPERSET_URL = "http://localhost:8088"
ADMIN_USER = "admin"
ADMIN_PASS = "admin"

# Native mart sống trong default_catalog.serving. MySQL-compatible URI ổn định
# với Superset hơn dialect starrocks khi reflect dataset.
STARROCKS_URI = "mysql+pymysql://root:@starrocks:9030/serving"
DB_NAME = "StarRocks Serving (B)"


def post_json(url, data, headers=None):
    body = json.dumps(data).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def get_json(url, headers=None):
    req = urllib.request.Request(url, method="GET")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def wait_for_superset(max_wait=120):
    print(f"Waiting for Superset at {SUPERSET_URL}...")
    for i in range(max_wait // 5):
        try:
            urllib.request.urlopen(f"{SUPERSET_URL}/health", timeout=5)
            print("OK: Superset is ready!")
            return
        except Exception:
            print(f"  Not ready yet ({i*5}s/{max_wait}s)...")
            time.sleep(5)
    raise TimeoutError("Superset did not become ready in time")


def login():
    print(f"Logging in as '{ADMIN_USER}'...")
    resp = post_json(
        f"{SUPERSET_URL}/api/v1/security/login",
        {"username": ADMIN_USER, "password": ADMIN_PASS, "provider": "db", "refresh": True},
    )
    token = resp.get("access_token", "")
    if not token:
        raise RuntimeError("Could not login to Superset")
    print("OK: Login successful (JWT)")
    return token


def find_db_id(headers):
    try:
        data = get_json(f"{SUPERSET_URL}/api/v1/database/?q=(filters:!())", headers=headers)
        for db in data.get("result", []):
            if db.get("database_name") == DB_NAME:
                print(f"  Found existing connection '{DB_NAME}' (id={db['id']})")
                return db["id"]
    except Exception:
        pass
    return None


def delete_db(db_id, headers):
    print(f"Deleting existing connection (id={db_id})...")
    req = urllib.request.Request(
        f"{SUPERSET_URL}/api/v1/database/{db_id}", headers=headers, method="DELETE"
    )
    with urllib.request.urlopen(req, timeout=15):
        pass


def create_db(headers):
    print(f"Creating database connection '{DB_NAME}'...")
    payload = {
        "database_name": DB_NAME,
        "sqlalchemy_uri": STARROCKS_URI,
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "extra": json.dumps({"metadata_params": {}, "engine_params": {}, "metadata_cache_timeout": {}}),
    }
    return post_json(f"{SUPERSET_URL}/api/v1/database/", payload, headers=headers)


def main():
    print()
    print("=" * 55)
    print("Setup: register StarRocks Serving (B) in Superset")
    print("=" * 55)

    wait_for_superset()
    token = login()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Referer": SUPERSET_URL,
    }

    existing = find_db_id(headers)
    if existing:
        delete_db(existing, headers)

    try:
        result = create_db(headers)
        db_id = result.get("id") or result.get("result", {}).get("id")
        print(f"OK: Database connection created (id={db_id})")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"FAILED to create connection: HTTP {e.code}")
        print(f"   Response: {body}")
        print("\nManual setup:")
        print(f"   Settings -> Database Connections -> + Database")
        print(f"   SQLAlchemy URI: {STARROCKS_URI}")
        sys.exit(1)

    print()
    print("=" * 55)
    print("Superset configured for Demo B!")
    print()
    print("  Open  : http://localhost:8088   (admin/admin)")
    print("  SQL Lab -> Database: StarRocks Serving (B)")
    print("  Try   : SELECT * FROM ads_revenue_daily LIMIT 10;")
    print("=" * 55)
    print()


if __name__ == "__main__":
    main()
