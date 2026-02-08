#!/bin/bash
set -euo pipefail

python - <<'PY'
import os
from urllib.parse import urlparse
import psycopg2

url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/chief_of_staff")
parsed = urlparse(url)

dbname = parsed.path.lstrip("/") or "chief_of_staff"
user = parsed.username or "postgres"
password = parsed.password or "postgres"
host = parsed.hostname or "localhost"
port = parsed.port or 5432

conn = psycopg2.connect(
    dbname="postgres",
    user=user,
    password=password,
    host=host,
    port=port,
)
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
if not cur.fetchone():
    cur.execute(f'CREATE DATABASE "{dbname}"')
cur.close()
conn.close()
PY

python -m app.db.init_db
