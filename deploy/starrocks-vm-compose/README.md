# StarRocks VM Compose

Deploy StarRocks `4.1.1` on a single VM using Docker Compose with separated FE and BE services.

This is intended for POC/dev or a small single-node VM deployment. It is not HA production because FE and BE are on one VM.

## Requirements

- CPU exposes `sse4_2`, `avx2`, `bmi2`.
- Docker and Docker Compose are installed.
- Ports are free: `8030`, `9020`, `9030`, `9010`, `8040`, `8060`, `9050`, `9060`.
- Recommended free disk: at least `40G`, better `60G+`.

## Quick Start

```bash
cd ~/tuantd-dgs/starrocks-vm-compose
chmod +x scripts/*.sh
bash scripts/00-precheck.sh
bash scripts/01-pull-images.sh
bash scripts/02-up.sh
bash scripts/03-add-backend.sh
bash scripts/04-check.sh
```

## Connect

```bash
mysql -h 127.0.0.1 -P 9030 -uroot
```

## Smoke Test

```sql
CREATE DATABASE IF NOT EXISTS sr_test;
CREATE TABLE IF NOT EXISTS sr_test.t1 (
  id INT,
  name VARCHAR(50)
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO sr_test.t1 VALUES (1, 'ok');
SELECT * FROM sr_test.t1;
```

## Stop

```bash
bash scripts/90-down.sh
```

## Delete Data

```bash
bash scripts/99-clean-data.sh
```
