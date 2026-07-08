#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p data/fe/meta data/fe/log data/be/storage data/be/log

docker compose up -d starrocks-fe

echo "Waiting for FE MySQL port 9030..."
for i in $(seq 1 60); do
  if docker exec starrocks-fe bash -lc 'exec 3<>/dev/tcp/127.0.0.1/9030' >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

docker compose up -d starrocks-be

echo "Containers:"
docker compose ps
