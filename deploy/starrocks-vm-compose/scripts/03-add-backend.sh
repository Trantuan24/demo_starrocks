#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

MYSQL_CMD="mysql -h 127.0.0.1 -P 9030 -uroot"
if ! command -v mysql >/dev/null 2>&1; then
  echo "mysql client not found on host; using mysql:8 client container"
  MYSQL_CMD="docker run --rm --network starrocks-net mysql:8 mysql -h starrocks-fe -P 9030 -uroot"
fi

echo "Waiting for FE query service..."
for i in $(seq 1 60); do
  if $MYSQL_CMD -e 'SELECT 1;' >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

set +e
$MYSQL_CMD -e "ALTER SYSTEM ADD BACKEND 'starrocks-be:9050';"
rc=$?
set -e

if [ "$rc" -ne 0 ]; then
  echo "ALTER SYSTEM ADD BACKEND returned non-zero. This is OK if backend already exists. Current backends:"
fi

$MYSQL_CMD -e 'SHOW BACKENDS\G'
