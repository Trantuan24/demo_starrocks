# StarRocks VM Compose

Deploy a minimal StarRocks `4.1.1` cluster on one VM using Docker Compose.

This setup runs:

- `1 FE` container: `starrocks-fe`
- `1 BE` container: `starrocks-be`
- Deployment mode: basic shared-nothing StarRocks layout
- Data path: `./data/fe` and `./data/be`

This is suitable for POC, dev, and integration testing. It is not HA production because FE and BE are both on one VM.

## Requirements

- Linux VM with Docker and Docker Compose v2.
- CPU supports `sse4_2`, `avx2`, and `bmi2`.
- Recommended free disk: at least `40G`, better `60G+`.
- Required ports must be free: `8030`, `9010`, `9020`, `9030`, `8040`, `8060`, `9050`, `9060`.

Check before deploy:

```bash
bash scripts/00-precheck.sh
```

## Ports

FE ports:

- `8030`: Web UI / HTTP API, open with browser.
- `9030`: MySQL protocol, use with `mysql`, DBeaver, DataGrip, Trino connector, etc.
- `9020`: FE RPC.
- `9010`: FE edit log / metadata communication.

BE ports:

- `8040`: BE HTTP endpoint.
- `8060`: BE BRPC.
- `9050`: BE heartbeat service.
- `9060`: BE service port.

Do not open `9030` in a browser. It is not HTTP and Chrome will show `ERR_INVALID_HTTP_RESPONSE`.

## Deploy

From this folder on the VM:

```bash
chmod +x scripts/*.sh

bash scripts/00-precheck.sh
bash scripts/01-pull-images.sh
bash scripts/02-up.sh
bash scripts/03-add-backend.sh
bash scripts/04-check.sh
```

Expected result from `04-check.sh`:

- FE has `Role: LEADER`.
- FE has `Alive: true`.
- BE has `Alive: true`.
- BE has `StatusCode: OK`.
- `SELECT 1` returns `1`.

## Connect

From the VM:

```bash
mysql -h 127.0.0.1 -P9030 -uroot
```

From another machine that can reach the VM:

```bash
mysql -h <VM_IP> -P9030 -uroot
```

Example for the tested VM:

```bash
mysql -h 10.168.6.106 -P9030 -uroot
```

Web UI:

```text
http://<VM_IP>:8030
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

## Check Status

```bash
docker compose ps
bash scripts/04-check.sh
```

Direct SQL check without host mysql client:

```bash
docker run --rm --network starrocks-net mysql:8 \
  mysql -h starrocks-fe -P9030 -uroot -e "SHOW FRONTENDS; SHOW BACKENDS;"
```

## Logs

```bash
bash scripts/05-logs.sh
```

Or:

```bash
docker logs --tail=200 starrocks-fe
docker logs --tail=200 starrocks-be
```

## Stop

Stop containers but keep data:

```bash
bash scripts/90-down.sh
```

Start again:

```bash
bash scripts/02-up.sh
bash scripts/04-check.sh
```

## Delete Data

Only use this during initial testing when you intentionally want to wipe all StarRocks data:

```bash
bash scripts/99-clean-data.sh
```

This deletes:

- `./data/fe`
- `./data/be`

Do not run it after real data has been loaded.

## Troubleshooting

If containers restart and logs only show `tini-static` help text, check that `docker-compose.yml` has explicit commands:

```yaml
command: ["/opt/starrocks/fe/bin/start_fe.sh"]
command: ["/opt/starrocks/be/bin/start_be.sh"]
```

Do not add `--console` for image `4.1.1`; the start scripts in this image do not accept that option.

If `99-clean-data.sh` reports `Permission denied`, it means files were created by the container user. The script retries with `sudo` for the local `./data` directory.
