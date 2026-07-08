#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
MYSQL_CMD="mysql -h 127.0.0.1 -P 9030 -uroot"
if ! command -v mysql >/dev/null 2>&1; then
  MYSQL_CMD="docker run --rm --network starrocks-net mysql:8 mysql -h starrocks-fe -P 9030 -uroot"
fi

docker compose ps
$MYSQL_CMD -e 'SHOW FRONTENDS\G'
$MYSQL_CMD -e 'SHOW BACKENDS\G'
$MYSQL_CMD -e 'SELECT 1;'
