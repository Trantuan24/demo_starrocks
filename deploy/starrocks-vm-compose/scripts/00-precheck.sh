#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== system =="
hostname -I || true
df -h . /
free -h
ulimit -n

echo "== cpu flags =="
grep -m1 flags /proc/cpuinfo | grep -o 'sse4_2\|avx2\|bmi2' | sort -u

echo "== ports =="
ss -lntp | grep -E ':8030|:9020|:9030|:9010|:8040|:8060|:9050|:9060' || true

echo "== docker =="
docker --version
docker compose version
docker system df
