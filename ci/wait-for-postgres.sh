#!/usr/bin/env bash
# Polls the docker-compose `postgres` service until it accepts TCP logins
# as etl_app. That role comes from db/postgres-init, so success means the
# init scripts have finished (the image's temporary init server does not
# listen on TCP). Exits 1 on timeout.

set -e

echo "waiting for postgres (etl_app login over TCP)..."
for i in $(seq 1 60); do
    if docker exec -e PGPASSWORD=etl_app etl-postgres \
        psql -h 127.0.0.1 -U etl_app -d etl_catalog -tAc 'SELECT 1' > /dev/null 2>&1; then
        echo "postgres ready (${i}s)"
        exit 0
    fi
    sleep 1
done

echo "postgres failed to come up in 60s"
docker compose ps
docker compose logs postgres | tail -50
exit 1
