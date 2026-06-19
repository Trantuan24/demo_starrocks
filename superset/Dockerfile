# ════════════════════════════════════════════════════════════
# Superset Dockerfile
# Base: apache/superset + StarRocks SQLAlchemy dialect
# ════════════════════════════════════════════════════════════

FROM apache/superset:4.1.1

USER root

# Cài StarRocks SQLAlchemy dialect và MySQL driver
# starrocks: dialect chính thức từ StarRocks
# pymysql: fallback MySQL-compatible driver
RUN pip install --no-cache-dir \
    starrocks \
    pymysql \
    sqlalchemy-utils

# Copy config và entrypoint
COPY superset_config.py /app/pythonpath/superset_config.py
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Superset image chạy bằng user 'superset'
USER superset

ENTRYPOINT ["/entrypoint.sh"]
CMD []
