param(
    [Parameter(Position = 0)]
    [ValidateSet(
        "up", "down", "clean", "status", "logs", "producer", "superset",
        "starrocks", "routine-load", "count", "mysql", "test", "setup",
        "dashboard",
        # Demo B (Iceberg serving mart)
        "benchmark", "setup-b", "dashboard-b", "mysql-b", "count-b",
        # Demo A (đọc thẳng External Catalog Iceberg)
        "query-a", "benchmark-ab"
    )]
    [string]$Command = "status",

    # Target profile cho 'up': c (Kafka realtime) | b (Iceberg mart) | all
    [Parameter(Position = 1)]
    [ValidateSet("c", "b", "all")]
    [string]$Target = "c"
)

$ErrorActionPreference = "Stop"

function Invoke-Compose {
    docker compose @args
}

# Profile args theo target
function Get-ProfileArgs([string]$t) {
    switch ($t) {
        "c"   { return @("--profile", "c") }
        "b"   { return @("--profile", "b") }
        "all" { return @("--profile", "b", "--profile", "c") }
    }
}

switch ($Command) {
    "up" {
        $p = Get-ProfileArgs $Target
        Invoke-Compose @p up -d --build
        Write-Host ""
        Write-Host "Profile '$Target' starting."
        if ($Target -eq "c" -or $Target -eq "all") {
            Write-Host "  Demo C: .\scripts\demo.ps1 test ; setup ; dashboard"
        }
        if ($Target -eq "b" -or $Target -eq "all") {
            Write-Host "  Demo B: .\scripts\demo.ps1 benchmark ; setup-b ; dashboard-b"
        }
    }
    "down" {
        # Bao gồm cả 2 profile để dọn sạch container của B lẫn C
        Invoke-Compose --profile b --profile c down
    }
    "clean" {
        Invoke-Compose --profile b --profile c down -v --remove-orphans
    }
    "status" {
        Invoke-Compose --profile b --profile c ps
    }
    "logs" {
        Invoke-Compose logs -f --tail=120
    }
    "producer" {
        Invoke-Compose logs -f --tail=120 producer
    }
    "superset" {
        Invoke-Compose logs -f --tail=120 superset
    }
    "starrocks" {
        Invoke-Compose logs -f --tail=120 starrocks
    }

    # ── Demo C ──
    "routine-load" {
        Invoke-Compose exec -T starrocks mysql -h 127.0.0.1 -P 9030 -u root -D demo -e "SHOW ROUTINE LOAD FROM demo\G"
    }
    "count" {
        Invoke-Compose exec -T starrocks mysql -h 127.0.0.1 -P 9030 -u root -D demo -e "SELECT COUNT(*) AS total_events FROM rt_sales_events;"
    }
    "mysql" {
        Invoke-Compose exec starrocks mysql -h 127.0.0.1 -P 9030 -u root demo
    }
    "test" {
        python scripts/test_pipeline.py
    }
    "setup" {
        python scripts/setup_superset.py
    }
    "dashboard" {
        python scripts/create_dashboard.py
    }

    # ── Demo B ──
    "benchmark" {
        python scripts/benchmark_b.py
    }
    "setup-b" {
        python scripts/setup_superset_b.py
    }
    "dashboard-b" {
        python scripts/create_dashboard_b.py
    }
    "mysql-b" {
        Invoke-Compose exec starrocks mysql -h 127.0.0.1 -P 9030 -u root serving
    }
    "count-b" {
        Invoke-Compose exec -T starrocks mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT COUNT(*) AS dtm_rows FROM iceberg_dtm.dtm.fact_sales; SELECT COUNT(*) AS mart_rows FROM serving.ads_revenue_daily;"
    }

    # ── Demo A (đọc thẳng External Catalog Iceberg) ──
    "query-a" {
        Get-Content sql/A_01_direct_query.sql | Invoke-Compose exec -T starrocks mysql -h 127.0.0.1 -P 9030 -u root
    }
    "benchmark-ab" {
        python scripts/benchmark_a_vs_b.py
    }
}
