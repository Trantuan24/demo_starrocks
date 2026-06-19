#!/usr/bin/env python3
"""
benchmark_b.py  (Phương án B)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
So sánh latency cùng một câu hỏi BI trên:
    (A) External Iceberg  : iceberg_dtm.dtm.fact_sales      (scan MinIO mỗi lần)
    (B) Native mart       : serving.ads_revenue_daily       (columnar local, pre-aggregated)

Chạy mỗi query nhiều lần, lấy min/median, in bảng so sánh + speedup.
Đồng thời:
    - Kiểm tra data parity giữa external và native.
    - Xác nhận Materialized View query-rewrite qua EXPLAIN.

Chạy từ host sau khi demo B đã up:
    python scripts/benchmark_b.py
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
RUNS = 5  # số lần chạy mỗi query để lấy median

GREEN = "\033[92m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
BOLD = "\033[1m"
RESET = "\033[0m"

# (label, external query, native query)
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
    """Chạy query `runs` lần, trả (median_ms, min_ms, rows)."""
    times = []
    rows = 0
    for _ in range(runs):
        t0 = time.perf_counter()
        cursor.execute(sql)
        result = cursor.fetchall()
        dt = (time.perf_counter() - t0) * 1000.0
        times.append(dt)
        rows = len(result)
    return statistics.median(times), min(times), rows


def main():
    print()
    print("=" * 64)
    print(f"{BOLD}Benchmark Phuong an B: External Iceberg vs Native mart{RESET}")
    print(f"  (mỗi query chạy {RUNS} lần, lấy median; warm-up 1 lần trước)")
    print("=" * 64)

    conn = connect()
    cur = conn.cursor()

    # Thông tin nguồn dữ liệu
    cur.execute("SELECT COUNT(*) FROM iceberg_dtm.dtm.fact_sales")
    dtm_rows = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM default_catalog.serving.ads_revenue_daily")
    mart_rows = cur.fetchone()[0]
    print(f"\n  Iceberg DTM (fact_sales)      : {dtm_rows:,} rows")
    print(f"  Native mart (ads_revenue_daily): {mart_rows:,} rows")

    # Header
    print()
    print(f"  {'Query':<26} {'External (ms)':>14} {'Native (ms)':>13} {'Speedup':>9}")
    print(f"  {'-'*26} {'-'*14:>14} {'-'*13:>13} {'-'*9:>9}")

    speedups = []
    for label, ext_sql, nat_sql in QUERIES:
        # warm-up (loại ảnh hưởng cache lạnh / compile plan lần đầu)
        cur.execute(ext_sql); cur.fetchall()
        cur.execute(nat_sql); cur.fetchall()

        ext_med, ext_min, _ = time_query(cur, ext_sql)
        nat_med, nat_min, _ = time_query(cur, nat_sql)
        speedup = ext_med / nat_med if nat_med > 0 else float("inf")
        speedups.append(speedup)

        color = GREEN if speedup >= 1.5 else YELLOW
        print(f"  {label:<26} {ext_med:>14.1f} {nat_med:>13.1f} "
              f"{color}{speedup:>8.1f}x{RESET}")

    avg_speedup = statistics.mean(speedups)
    print(f"  {'-'*26} {'-'*14:>14} {'-'*13:>13} {'-'*9:>9}")
    print(f"  {BOLD}{'Average speedup':<26}{RESET} "
          f"{'':>14} {'':>13} {GREEN}{avg_speedup:>8.1f}x{RESET}")

    # ── Data parity check ──
    print()
    print(f"  {CYAN}Data parity check (external vs native){RESET}")
    cur.execute("SELECT CAST(SUM(amount) AS DECIMAL(38,2)), COUNT(*) FROM iceberg_dtm.dtm.fact_sales")
    ext_rev, ext_cnt = cur.fetchone()
    cur.execute("SELECT CAST(SUM(revenue) AS DECIMAL(38,2)), SUM(order_count) FROM default_catalog.serving.ads_revenue_daily")
    nat_rev, nat_cnt = cur.fetchone()
    rev_ok = abs(float(ext_rev) - float(nat_rev)) < 1.0
    cnt_ok = int(ext_cnt) == int(nat_cnt)
    print(f"    Total revenue : external={float(ext_rev):,.0f}  native={float(nat_rev):,.0f}  "
          f"{GREEN+'MATCH'+RESET if rev_ok else YELLOW+'DIFF'+RESET}")
    print(f"    Order count   : external={int(ext_cnt):,}  native={int(nat_cnt):,}  "
          f"{GREEN+'MATCH'+RESET if cnt_ok else YELLOW+'DIFF'+RESET}")

    # ── Materialized View query-rewrite ──
    print()
    print(f"  {CYAN}Materialized View query-rewrite{RESET}")
    cur.execute("""EXPLAIN SELECT province, SUM(amount) AS revenue
                   FROM iceberg_dtm.dtm.fact_sales GROUP BY province""")
    plan = "\n".join(str(r[0]) for r in cur.fetchall())
    if "mv_revenue_daily" in plan:
        print(f"    {GREEN}MV được dùng tự động{RESET}: query gốc trên Iceberg đã rewrite "
              f"sang serving.mv_revenue_daily")
    else:
        print(f"    {YELLOW}MV chưa được rewrite trong plan này{RESET} "
              f"(có thể do điều kiện rewrite/refresh; mart native vẫn tăng tốc như trên).")

    conn.close()

    print()
    print("=" * 64)
    print(f"{GREEN}Kết luận:{RESET} native mart/MV tăng tốc query BI ~{avg_speedup:.1f}x "
          f"so với scan Iceberg trực tiếp.")
    print("Tiếp theo: .\\scripts\\demo.ps1 setup-b  ;  .\\scripts\\demo.ps1 dashboard-b")
    print("=" * 64)
    print()


if __name__ == "__main__":
    main()
