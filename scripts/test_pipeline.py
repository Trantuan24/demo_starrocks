#!/usr/bin/env python3
"""
test_pipeline.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Full end-to-end pipeline test: Kafka → StarRocks → Superset

Chạy từ host machine sau khi tất cả services đã up:
    python scripts/test_pipeline.py

Requirements:
    pip install -r scripts/requirements-test.txt

Port mapping (host):
    Kafka    : localhost:9094  (external listener)
    StarRocks: localhost:9030  (MySQL query port)
    Superset : localhost:8088
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import json
import subprocess
import sys
import io
import time
from datetime import datetime

# Fix Windows terminal encoding (cp1252 → utf-8)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


# ─── Helpers ──────────────────────────────────────────────────────────────────
RESULTS = []

RESET   = "\033[0m"
GREEN   = "\033[92m"
RED     = "\033[91m"
YELLOW  = "\033[93m"
CYAN    = "\033[96m"
BOLD    = "\033[1m"


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def info(msg: str):
    print(f"  {CYAN}ℹ {RESET} {msg}")


def warn(msg: str):
    print(f"  {YELLOW}⚠ {RESET} {msg}")


def run_test(name: str, func):
    """Execute một test case và ghi nhận kết quả."""
    print(f"\n[{ts()}] {BOLD}TEST:{RESET} {name}")
    try:
        result = func()
        if result is True or result is None:
            print(f"  {GREEN}✅ PASS{RESET}")
            RESULTS.append((name, True, None))
        else:
            print(f"  {RED}❌ FAIL{RESET}: {result}")
            RESULTS.append((name, False, str(result)))
    except AssertionError as e:
        print(f"  {RED}❌ FAIL{RESET}: {e}")
        RESULTS.append((name, False, str(e)))
    except Exception as e:
        print(f"  {RED}❌ ERROR{RESET}: {type(e).__name__}: {e}")
        RESULTS.append((name, False, f"{type(e).__name__}: {e}"))


# ─── Test 1: Kafka broker reachable ───────────────────────────────────────────
def test_kafka_broker():
    from kafka import KafkaAdminClient
    client = KafkaAdminClient(
        bootstrap_servers="localhost:9094",
        request_timeout_ms=5000,
        client_id="test-admin"
    )
    client.close()
    info("Kafka broker reachable at localhost:9094")


# ─── Test 2: Kafka topic exists ───────────────────────────────────────────────
def test_kafka_topic_exists():
    from kafka import KafkaAdminClient
    client = KafkaAdminClient(
        bootstrap_servers="localhost:9094",
        request_timeout_ms=5000
    )
    topics = client.list_topics()
    client.close()
    assert "sales_events" in topics, \
        f"Topic 'sales_events' not found. Available topics: {list(topics)}"
    info(f"Topic 'sales_events' exists")


# ─── Test 3: Kafka has messages ───────────────────────────────────────────────
def test_kafka_has_messages():
    messages = []

    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            "sales_events",
            bootstrap_servers="localhost:9094",
            auto_offset_reset="earliest",
            consumer_timeout_ms=8000,
            enable_auto_commit=False,
            group_id=f"test-group-{int(time.time())}",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        try:
            for msg in consumer:
                messages.append(msg.value)
                if len(messages) >= 5:
                    break
        finally:
            try:
                consumer.close()
            except ValueError:
                pass
    except Exception as e:
        warn(f"Host KafkaConsumer failed ({type(e).__name__}: {e}); falling back to Kafka CLI in container")
        cmd = [
            "docker", "compose", "exec", "-T", "kafka",
            "kafka-console-consumer",
            "--bootstrap-server", "kafka:9092",
            "--topic", "sales_events",
            "--from-beginning",
            "--max-messages", "5",
            "--timeout-ms", "10000",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        output = "\n".join(
            line for line in proc.stdout.splitlines()
            if line.strip().startswith("{")
        )
        messages = [json.loads(line) for line in output.splitlines()]

    assert len(messages) > 0, \
        "No messages found in Kafka topic 'sales_events'. Is producer running?"
    info(f"Found {len(messages)} messages. Sample: {messages[0]}")

    # Validate message schema
    required_fields = {"event_time", "order_id", "province", "product", "amount", "payment_method"}
    sample = messages[0]
    missing = required_fields - set(sample.keys())
    assert not missing, f"Message missing fields: {missing}"
    info("Message schema validated ✓")


# ─── Test 4: StarRocks FE connection ──────────────────────────────────────────
def test_starrocks_connection():
    import pymysql
    conn = pymysql.connect(
        host="localhost",
        port=9030,
        user="root",
        password="",
        connect_timeout=10,
        charset="utf8mb4",
    )
    cursor = conn.cursor()
    cursor.execute("SELECT VERSION()")
    version = cursor.fetchone()[0]
    conn.close()
    info(f"StarRocks version: {version}")


# ─── Test 5: StarRocks BE alive ───────────────────────────────────────────────
def test_starrocks_be_alive():
    import pymysql
    conn = pymysql.connect(host="localhost", port=9030, user="root", password="")
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SHOW BACKENDS")
    backends = cursor.fetchall()
    conn.close()

    assert len(backends) > 0, "No backends found in StarRocks"

    # Tìm backend alive
    alive = []
    for b in backends:
        # Column name có thể là 'Alive' hoặc 'alive'
        alive_val = b.get("Alive") or b.get("alive") or ""
        if str(alive_val).lower() == "true":
            alive.append(b)

    assert len(alive) > 0, \
        f"No alive backends. Backends: {backends}\n" \
        "StarRocks BE might still be starting up. Wait 60-90s and retry."
    info(f"{len(alive)}/{len(backends)} backend(s) alive")


# ─── Test 6: StarRocks database và table tồn tại ─────────────────────────────
def test_starrocks_schema():
    import pymysql
    conn = pymysql.connect(host="localhost", port=9030, user="root", password="")
    cursor = conn.cursor()

    # Check database
    cursor.execute("SHOW DATABASES")
    databases = [r[0] for r in cursor.fetchall()]
    assert "demo" in databases, \
        f"Database 'demo' not found. Available: {databases}"
    info("Database 'demo' exists")

    # Check table
    cursor.execute("USE demo")
    cursor.execute("SHOW TABLES")
    tables = [r[0] for r in cursor.fetchall()]
    assert "rt_sales_events" in tables, \
        f"Table 'rt_sales_events' not found. Tables: {tables}"
    info("Table 'rt_sales_events' exists")

    # Check columns
    cursor.execute("DESCRIBE rt_sales_events")
    cols = [r[0] for r in cursor.fetchall()]
    expected_cols = {"event_time", "order_id", "province", "product", "amount", "payment_method", "ingest_time"}
    missing_cols = expected_cols - set(cols)
    assert not missing_cols, f"Missing columns: {missing_cols}"
    info(f"Table schema OK: {sorted(cols)}")
    conn.close()


# ─── Test 7: Routine Load state = RUNNING ────────────────────────────────────
def test_routine_load_running():
    import pymysql
    conn = pymysql.connect(host="localhost", port=9030, user="root", password="", database="demo")
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    cursor.execute("SHOW ROUTINE LOAD FROM demo")
    rows = cursor.fetchall()
    conn.close()

    rl_row = None
    for row in rows:
        name = row.get("Name") or row.get("name") or ""
        if "rl_sales_events" in name:
            rl_row = row
            break

    assert rl_row is not None, \
        "Routine Load 'rl_sales_events' not found. " \
        "Check starrocks-init container logs."

    state = rl_row.get("State") or rl_row.get("state") or "UNKNOWN"
    info(f"Routine Load state: {state}")

    if state == "PAUSED":
        err = rl_row.get("ErrorLogUrls") or rl_row.get("ReasonOfStateChanged") or ""
        warn(f"Load is PAUSED. Reason: {err}")

    assert state == "RUNNING", \
        f"Routine Load state is '{state}', expected 'RUNNING'. " \
        f"Check: SHOW ROUTINE LOAD FROM demo\\G"


# ─── Test 8: Data đang chảy vào StarRocks ────────────────────────────────────
def test_data_flowing():
    import pymysql
    conn = pymysql.connect(host="localhost", port=9030, user="root", password="", database="demo")
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM rt_sales_events")
    count_t0 = cursor.fetchone()[0]
    info(f"Row count at T=0: {count_t0:,}")

    assert count_t0 > 0, \
        "Table is empty! Producer might not be running or Routine Load just started. " \
        "Wait 10-20 seconds and retry."

    print(f"  ⏳ Waiting 10 seconds to check growth...")
    time.sleep(10)

    cursor.execute("SELECT COUNT(*) FROM rt_sales_events")
    count_t10 = cursor.fetchone()[0]
    conn.close()

    delta = count_t10 - count_t0
    info(f"Row count at T+10s: {count_t10:,} (+{delta} rows)")

    assert delta > 0, \
        f"Row count did not increase ({count_t0} → {count_t10}). " \
        "Check if producer is running: docker logs demo_producer"

    rate = delta / 10
    info(f"Ingest rate: ~{rate:.1f} rows/second")


# ─── Test 9: Aggregate query works ───────────────────────────────────────────
def test_aggregate_query():
    import pymysql
    conn = pymysql.connect(host="localhost", port=9030, user="root", password="", database="demo")
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    # Query 1: Revenue by province
    cursor.execute("""
        SELECT
            province,
            COUNT(*)        AS order_count,
            SUM(amount)     AS total_revenue
        FROM rt_sales_events
        GROUP BY province
        ORDER BY total_revenue DESC
        LIMIT 5
    """)
    rows = cursor.fetchall()
    assert len(rows) > 0, "Aggregate by province returned empty"
    info("Revenue by province (top 5):")
    for r in rows:
        print(f"    {r['province']:<20} | {r['order_count']:>5} orders | {float(r['total_revenue']):>15,.0f} VND")

    # Query 2: Revenue by minute (last 5 minutes)
    cursor.execute("""
        SELECT
            date_trunc('minute', event_time) AS minute_time,
            COUNT(*)                         AS order_count,
            SUM(amount)                      AS revenue
        FROM rt_sales_events
        WHERE event_time >= NOW() - INTERVAL 10 MINUTE
        GROUP BY minute_time
        ORDER BY minute_time DESC
        LIMIT 5
    """)
    rows = cursor.fetchall()
    info(f"Revenue by minute (last 10 min): {len(rows)} data points found")
    conn.close()


# ─── Test 10: Superset accessible ────────────────────────────────────────────
def test_superset_accessible():
    import urllib.request
    import urllib.error

    url = "http://localhost:8088/health"
    try:
        req = urllib.request.urlopen(url, timeout=10)
        status = req.getcode()
        assert status == 200, f"Health check returned status {status}"
        info(f"Superset health endpoint OK (HTTP {status})")
    except urllib.error.HTTPError as e:
        # Some Superset versions return non-200 for /health but are still running
        if e.code in (302, 401, 403):
            info(f"Superset running (redirected to login, HTTP {e.code})")
        else:
            raise AssertionError(f"Superset returned HTTP {e.code}")
    except urllib.error.URLError as e:
        raise AssertionError(
            f"Cannot reach Superset at {url}: {e.reason}\n"
            "Is Superset still initializing? Wait 60-90s and retry."
        )


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print()
    print("═" * 60)
    print(f"{BOLD}🚀 Pipeline Test: Kafka → StarRocks → Superset{RESET}")
    print("═" * 60)

    tests = [
        ("Kafka broker reachable (localhost:9094)",  test_kafka_broker),
        ("Kafka topic 'sales_events' exists",        test_kafka_topic_exists),
        ("Kafka has messages",                        test_kafka_has_messages),
        ("StarRocks FE connection (localhost:9030)",  test_starrocks_connection),
        ("StarRocks BE alive",                        test_starrocks_be_alive),
        ("StarRocks DB & table schema",               test_starrocks_schema),
        ("Routine Load state = RUNNING",              test_routine_load_running),
        ("Data flowing into StarRocks (10s check)",   test_data_flowing),
        ("Aggregate query works",                     test_aggregate_query),
        ("Superset accessible (localhost:8088)",      test_superset_accessible),
    ]

    for name, func in tests:
        run_test(name, func)

    # ─── Summary ──────────────────────────────────────────
    print()
    print("═" * 60)
    passed = sum(1 for _, ok, _ in RESULTS if ok)
    failed = sum(1 for _, ok, _ in RESULTS if not ok)
    total  = len(RESULTS)

    print(f"{BOLD}Results: {passed}/{total} passed{RESET}", end="")
    if failed > 0:
        print(f" | {RED}{failed} failed{RESET}")
    else:
        print(f" | {GREEN}All tests passed! 🎉{RESET}")

    if failed > 0:
        print(f"\n{RED}Failed tests:{RESET}")
        for name, ok, err in RESULTS:
            if not ok:
                print(f"  ❌ {name}")
                print(f"     → {err}")

    print("═" * 60)

    # ─── Next steps ───────────────────────────────────────
    if passed == total:
        print(f"\n{GREEN}✅ Pipeline is healthy!{RESET}")
        print("\nNext steps:")
        print("  1. Open Superset: http://localhost:8088")
        print("     Login: admin / admin")
        print("  2. Auto-setup StarRocks connection:")
        print("     python scripts/setup_superset.py")
        print("  3. Create dashboard from table:")
        print("     demo.rt_sales_events")
    else:
        print(f"\n{YELLOW}⚠  Some tests failed. See troubleshooting in README.md{RESET}")

    print()
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
