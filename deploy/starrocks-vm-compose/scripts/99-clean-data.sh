#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
docker compose down
rm -rf data/fe data/be
mkdir -p data/fe/meta data/fe/log data/be/storage data/be/log
