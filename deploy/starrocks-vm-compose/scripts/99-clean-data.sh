#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
docker compose down
if ! rm -rf data/fe data/be 2>/tmp/starrocks-clean-data.err; then
  echo "Normal delete failed, retrying with sudo because container-created files may be owned by root."
  cat /tmp/starrocks-clean-data.err
  sudo rm -rf data/fe data/be
fi
mkdir -p data/fe/meta data/fe/log data/be/storage data/be/log
