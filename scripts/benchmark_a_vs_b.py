#!/usr/bin/env python3
"""
benchmark_a_vs_b.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
So sánh CÙNG một câu hỏi BI trên 3 cách thực thi:

  (A) Direct Iceberg   : đọc THẲNG iceberg_dtm.dtm.fact_sales (MV-rewrite TẮT)
                         -> Phương án A: scan Parquet trên MinIO mỗi lần
  (B1) MV auto-rewrite : VẪN viết query trỏ Iceberg, nhưng bật rewrite
                         -> StarRocks tự đọc serving.mv_revenue_daily
  (B2) Native mart     : query thẳng serving.ads_revenue_daily (pre-aggregated)

Mỗi query chạy nhiều lần lấy median. In bảng + speedup so với A.

Chạy từ host (sau khi demo B đã up + setup-b):
    python scripts/benchmark_a_vs_b.py
    (cần: pip install -r scripts/requirements-test.txt)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import io
import sys
import time
import statistics

import pymysql

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

HOST = "localhost"
PORT = 9030
USER = "root"
PASSWORD = ""
RUNS = 7  # số lần chạy mỗi biến thể để lấy median

GREEN = "\033[92m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Query "trỏ Iceberg" (dùng cho A và B1) và query "trỏ native mart" (B2)
QUERIES = [
    (
        "Q1 Revenue by province",
        """SELECT province, SUM(amount) AS revenue, COUNT(*) AS orders
           FROM iceberg_dtm.dtm.fact_sales
           GROUP BY province ORDER BY revenue DESC""",
        """SELECT province, SUM(revenue) AS revenue, SUM(order_count) AS orders
           FROM default_catalog.serving.ads_revenue_daily
           GROUP BY province ORDER BY revenue DESC""",
    ),
    (
        "Q2 Revenue last 30 days",
        """SELECT report_date, SUM(amount) AS revenue
           FROM iceberg_dtm.dtm.fact_sales
           WHERE report_date >= date_sub(CURRENT_DATE(), INTERVAL 30 DAY)
           GROUP BY report_date ORDER BY report_date""",
        """SELECT report_date, SUM(revenue) AS revenue
           FROM default_catalog.serving.ads_revenue_daily
           WHERE report_date >= date_sub(CURRENT_DATE(), INTERVAL 30 DAY)
           GROUP BY report_date ORDER BY report_date""",
    ),
    (
        "Q3 Top products",
        """SELECT product, SUM(amount) AS revenue
           FROM iceberg_dtm.dtm.fact_sales
           GROUP BY product ORDER BY revenue DESC LIMIT 10""",
        """SELECT product, SUM(revenue) AS revenue
           FROM default_catalog.serving.ads_revenue_daily
           GROUP BY product ORDER BY revenue DESC LIMIT 10""",
    ),
]


def connect():
    return pymysql.connect(
        host=HOST, port=PORT, user=USER, password=PASSWORD,
        connect_timeout=10, charset="utf8mb4",
    )


def time_query(cursor, sql, runs=RUNS):
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        cursor.execute(sql)
        cursor.fetchall()
        times.append((time.perf_counter() - t0) * 1000.0)
    return statistics.median(times)


def main():
    print()
    print("=" * 74)
    print(f"{BOLD}Benchmark A (đọc thẳng Iceberg) vs B (MV-rewrite / Native mart){RESET}")
    print(f"  mỗi biến thể chạy {RUNS} lần, lấy median; warm-up 1 lần trước")
    print("=" * 74)

    conn = connect()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM iceberg_dtm.dtm.fact_sales")
    dtm_rows = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM default_catalog.serving.ads_revenue_daily")
    mart_rows = cur.fetchone()[0]
    print(f"\n  Iceberg DTM (fact_sales)       : {dtm_rows:,} rows  (nguồn lake)")
    print(f"  Native mart (ads_revenue_daily): {mart_rows:,} rows  (pre-aggregated)")

    print()
    print(f"  {'Query':<24} {'A direct(ms)':>13} {'B1 MV(ms)':>11} {'B2 native(ms)':>14} "
          f"{'MV x':>7} {'nat x':>7}")
    print(f"  {'-'*24} {'-'*13} {'-'*11} {'-'*14} {'-'*7} {'-'*7}")

    mv_speedups, nat_speedups = [], []
    for label, ice_sql, nat_sql in QUERIES:
        # (A) đọc thẳng Iceberg: TẮT rewrite
        cur.execute("SET enable_materialized_view_rewrite = false")
        cur.execute(ice_sql); cur.fetchall()                 # warm-up
        a_ms = time_query(cur, ice_sql)

        # (B1) cùng query trỏ Iceberg nhưng BẬT rewrite -> tự dùng MV
        cur.execute("SET enable_materialized_view_rewrite = true")
        cur.execute(ice_sql); cur.fetchall()                 # warm-up
        b1_ms = time_query(cur, ice_sql)

        # (B2) query thẳng native mart
        cur.execute(nat_sql); cur.fetchall()                 # warm-up
        b2_ms = time_query(cur, nat_sql)

        mv_x = a_ms / b1_ms if b1_ms > 0 else float("inf")
        nat_x = a_ms / b2_ms if b2_ms > 0 else float("inf")
        mv_speedups.append(mv_x); nat_speedups.append(nat_x)

        print(f"  {label:<24} {a_ms:>13.1f} {b1_ms:>11.1f} {b2_ms:>14.1f} "
              f"{GREEN}{mv_x:>6.1f}x{RESET} {GREEN}{nat_x:>6.1f}x{RESET}")

    print(f"  {'-'*24} {'-'*13} {'-'*11} {'-'*14} {'-'*7} {'-'*7}")
    print(f"  {BOLD}{'Average':<24}{RESET} {'':>13} {'':>11} {'':>14} "
          f"{GREEN}{statistics.mean(mv_speedups):>6.1f}x{RESET} "
          f"{GREEN}{statistics.mean(nat_speedups):>6.1f}x{RESET}")

    # ── Xác nhận A thực sự đọc thẳng Iceberg (plan có IcebergScanNode) ──
    print()
    print(f"  {CYAN}Xác nhận Phương án A đọc THẲNG Iceberg (rewrite off){RESET}")
    cur.execute("SET enable_materialized_view_rewrite = false")
    cur.execute("""EXPLAIN SELECT province, SUM(amount)
                   FROM iceberg_dtm.dtm.fact_sales GROUP BY province""")
    plan_a = "\n".join(str(r[0]) for r in cur.fetchall())
    print(f"    {'IcebergScanNode' in plan_a and GREEN+'OK: plan có IcebergScanNode -> scan Parquet MinIO'+RESET or YELLOW+'không thấy IcebergScanNode'+RESET}")

    print()
    print(f"  {CYAN}Xác nhận Phương án B auto query-rewrite sang MV (rewrite on){RESET}")
    cur.execute("SET enable_materialized_view_rewrite = true")
    cur.execute("""EXPLAIN SELECT province, SUM(amount)
                   FROM iceberg_dtm.dtm.fact_sales GROUP BY province""")
    plan_b = "\n".join(str(r[0]) for r in cur.fetchall())
    if "mv_revenue_daily" in plan_b:
        print(f"    {GREEN}OK: query trỏ Iceberg đã tự rewrite sang serving.mv_revenue_daily{RESET}")
    else:
        print(f"    {YELLOW}Plan chưa rewrite sang MV (mart native vẫn nhanh như bảng trên){RESET}")

    conn.close()

    print()
    print("=" * 74)
    print(f"{GREEN}Kết luận:{RESET}")
    print(f"  A (đọc thẳng Iceberg) = chậm nhất, nhưng KHÔNG cần vật chất hóa, data luôn mới.")
    print(f"  B = nhanh hơn ~{statistics.mean(nat_speedups):.1f}x nhờ MV/native mart, đổi lại data trễ theo refresh.")
    print("=" * 74)
    print()


if __name__ == "__main__":
    main()
