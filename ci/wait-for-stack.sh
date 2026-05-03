#!/usr/bin/env bash
# Polls the docker-compose stack until Postgres + Temporal are ready.
# Exits 0 when both are responsive; exits 1 on timeout.

set -e

echo "waiting for postgres on 127.0.0.1:5432..."
for i in $(seq 1 60); do
    # Prefer host pg_isready (CI installs it); fall back to docker exec
    # when running locally on macOS without postgresql-client.
    if pg_isready -h 127.0.0.1 -p 5432 -U etl 2>/dev/null; then
        echo "postgres ready (${i}s)"
        break
    fi
    if docker exec etl-postgres pg_isready -U etl > /dev/null 2>&1; then
        echo "postgres ready via docker exec (${i}s)"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "postgres failed to come up in 60s"
        docker compose ps
        exit 1
    fi
    sleep 1
done

echo "waiting for temporal on 127.0.0.1:7233..."
for i in $(seq 1 120); do
    # gRPC port open is necessary but not sufficient — namespace registration
    # takes another ~10s after the port opens. Probe via tctl namespace list.
    if nc -z 127.0.0.1 7233 2>/dev/null; then
        if docker exec etl-temporal tctl --address temporal:7233 namespace list \
            > /dev/null 2>&1; then
            echo "temporal ready (${i}s)"
            exit 0
        fi
    fi
    if [ "$i" -eq 120 ]; then
        echo "temporal failed to come up in 120s"
        docker compose ps
        docker compose logs temporal | tail -50
        exit 1
    fi
    sleep 1
done

exit 1
