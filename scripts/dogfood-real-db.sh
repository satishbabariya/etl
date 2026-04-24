#!/usr/bin/env bash
# Replicate a developer-chosen Postgres table to local Parquet using
# the cursor-incremental pipeline. Not a test — a demo.
#
# Usage:
#   scripts/dogfood-real-db.sh <pg-url> <schema.table> <cursor-column> [cursor-kind]
# Example:
#   scripts/dogfood-real-db.sh \
#     'postgres://me:pw@localhost:5432/mydb' public.events updated_at timestamp_tz

set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "usage: $0 <pg-url> <schema.table> <cursor-column> [cursor-kind=timestamp_tz]" >&2
  exit 1
fi

SOURCE_URL="$1"
QUALIFIED="$2"
CURSOR_COL="$3"
CURSOR_KIND="${4:-timestamp_tz}"
SCHEMA="${QUALIFIED%.*}"
TABLE="${QUALIFIED#*.}"

WORKDIR="./data/dogfood"
mkdir -p "$WORKDIR"

export DATABASE_URL="${DATABASE_URL:-postgres://etl:etl@localhost:5432/etl_catalog}"
export TEMPORAL_ADDRESS="${TEMPORAL_ADDRESS:-127.0.0.1:7233}"
export TEMPORAL_NAMESPACE="${TEMPORAL_NAMESPACE:-default}"
export TEMPORAL_TASK_QUEUE="${TEMPORAL_TASK_QUEUE:-pipeline-default}"

echo "→ source:  $SOURCE_URL"
echo "→ catalog: $DATABASE_URL"
echo "→ target:  $WORKDIR/<pipeline_uuid>/"

# 1. Pre-flight row count (requires psql on PATH).
if command -v psql >/dev/null; then
  SOURCE_COUNT=$(psql "$SOURCE_URL" -At -c "SELECT COUNT(*) FROM $QUALIFIED" || echo "?")
  echo "→ source rows: $SOURCE_COUNT"
else
  echo "→ source rows: (psql not installed, skipping pre-flight count)"
  SOURCE_COUNT="?"
fi

# 2. Write a one-shot YAML spec.
YAML_FILE="$(mktemp -t dogfood.XXXXX.yaml)"
trap "rm -f $YAML_FILE" EXIT
cat > "$YAML_FILE" <<YAML
apiVersion: platform/v1
kind: Connection
metadata:
  name: dogfood-source
spec:
  connector_ref: postgres@0.1.0
  config:
    url: "$SOURCE_URL"
---
apiVersion: platform/v1
kind: Pipeline
metadata:
  name: dogfood-${TABLE}
spec:
  source:
    type: postgres
    schema: $SCHEMA
    table: $TABLE
    cursor_column: $CURSOR_COL
    cursor_kind: $CURSOR_KIND
    pk_columns: [id]
  destination:
    type: local_parquet
    base_path: "$WORKDIR"
  batch_size: 1000
  evolution_policy: propagate_additive
YAML

# 3. Build CLI + worker if needed.
cargo build --bin platform --bin worker >/dev/null 2>&1

# 4. Launch the worker if not already running.
if ! pgrep -f 'target/debug/worker' >/dev/null; then
  echo "→ starting worker"
  ./target/debug/worker &
  WORKER_PID=$!
  trap "rm -f $YAML_FILE; kill $WORKER_PID 2>/dev/null || true" EXIT
  sleep 3
fi

echo "→ applying pipeline spec"
./target/debug/platform apply -f "$YAML_FILE"

# 5. Run the pipeline.
./target/debug/platform pipeline run dogfood-${TABLE} || {
  # pipeline run uses the name, which current CLI requires as the pipeline_id.
  # Fall back to looking up the id:
  PIPELINE_ID=$(docker exec etl-postgres psql -U etl -d etl_catalog -At -c \
    "SELECT pipeline_id FROM pipelines WHERE name='dogfood-${TABLE}' ORDER BY created_at DESC LIMIT 1")
  ./target/debug/platform pipeline run "pipe-${PIPELINE_ID}"
}

# 6. Poll for completion.
DEADLINE=$(( $(date +%s) + 180 ))
while true; do
  PIPELINE_ID=$(docker exec etl-postgres psql -U etl -d etl_catalog -At -c \
    "SELECT pipeline_id FROM pipelines WHERE name='dogfood-${TABLE}' ORDER BY created_at DESC LIMIT 1" 2>/dev/null || true)
  if [[ -z "$PIPELINE_ID" ]]; then
    sleep 2
    continue
  fi
  STATUS_JSON=$(./target/debug/platform pipeline status "pipe-${PIPELINE_ID}" 2>/dev/null || echo "{}")
  STATUS=$(echo "$STATUS_JSON" | grep latest_run_status || true)
  echo "   $STATUS"
  if echo "$STATUS" | grep -qE 'completed'; then break; fi
  if echo "$STATUS" | grep -qE 'failed'; then
    echo "!! run failed — see worker logs"
    exit 2
  fi
  if [[ $(date +%s) -gt $DEADLINE ]]; then
    echo "!! timeout"
    exit 2
  fi
  sleep 3
done

# 7. Summarise the Parquet output.
PARQUET_COUNT=$(find "$WORKDIR" -name '*.parquet' | wc -l | tr -d ' ')
TOTAL_BYTES=$(du -sh "$WORKDIR" 2>/dev/null | awk '{print $1}' || echo "?")
echo "✔ landed $PARQUET_COUNT parquet files, $TOTAL_BYTES total, for $SOURCE_COUNT source rows"
echo "   output: $WORKDIR/"
