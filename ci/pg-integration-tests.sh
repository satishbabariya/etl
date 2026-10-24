#!/usr/bin/env bash
# Runs the integration-test binaries that need only Postgres (no Temporal,
# Vault or MySQL). Used by .github/workflows/pg-integration-tests.yml and
# `make pg-integration`.
#
# Expects the docker-compose `postgres` service (wal_level=logical plus the
# db/postgres-init scripts), a prior `cargo build --workspace`, jq, and:
#   DATABASE_URL            superuser catalog URL (etl)
#   DATABASE_URL_APP        non-superuser RLS URL (etl_app)
#   ETL_INTEGRATION_PG_URL  URL for the postgres_loader* / pg_cdc tests
#
# Several of these tests print "SKIP ..." and pass when Postgres is
# unreachable, so this script fails if any SKIP line shows up.

set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL must be set}"
: "${DATABASE_URL_APP:?DATABASE_URL_APP must be set}"
: "${ETL_INTEGRATION_PG_URL:?ETL_INTEGRATION_PG_URL must be set}"
command -v jq > /dev/null || { echo "jq is required"; exit 1; }

# Not included:
# - tenant_lifecycle, schema_evolution, incremental_sync, transforms_*,
#   durability_midbatch, cdc_*: spawn the worker, need Temporal.
# - connector_lifecycle, typescript_connector_lifecycle: build a new
#   connector crate / npm package, need network; no Postgres involved.
# - vault_e2e, mysql_*, stripe_*, snowflake_loader, *_wasm_e2e,
#   wasm_connector, pg_loader_real_e2e: need Vault, MySQL, external
#   accounts or the full e2e stack (see e2e-tests.yml).
TESTS=(
  tenant_api_isolation
  rls_cross_tenant
  metering_events
  audit_chain
  audit_corruption
  audit_retention
  audit_secret_read
  audit_tenant_override
  revocation
  refresh_rotate
  sealed_keys
  tenant_suspend
  auth_jwt
  oidc_jwks
  health_endpoints
  dsl_apply
  secrets_e2e
  postgres_loader
  postgres_loader_cdc
  postgres_loader_multi_table
  postgres_loader_schema_evolution
  pg_cdc_advance_slot
  pg_cdc_peek_window
  native_cdc_destinations
  pg_cdc_toast
  snowflake_loader_mock
  rbac_matrix
  tenant_create_unique
)

root="$(cd "$(dirname "$0")/.." && pwd)"
manifest="$(mktemp)"
log="$(mktemp)"
test_home="$(mktemp -d)"
trap 'rm -rf "$manifest" "$log" "$test_home"' EXIT

test_flags=()
for t in "${TESTS[@]}"; do
  test_flags+=(--test "$t")
done

# Compile once, then run the binaries directly rather than via `cargo test`.
# Most of these tests shell out to `cargo build`; under `cargo test` that
# inner build inherits CARGO_MANIFEST_DIR / CARGO_PKG_*, which ring's build
# script tracks, so the inner and outer builds would keep invalidating each
# other and recompile ring and everything above it for every binary.
cargo test -p integration-tests --locked --no-run \
  --message-format=json-render-diagnostics "${test_flags[@]}" \
  | jq -r 'select(.reason == "compiler-artifact" and .profile.test and .executable != null)
           | "\(.target.name) \(.executable)"' > "$manifest"

# auth_jwt, oidc_jwks and audit_tenant_override rewrite
# ~/.etl/credentials.json; point HOME at a throwaway dir so a local run
# leaves the developer's login alone. Keep cargo/rustup where they are.
export CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"
export RUSTUP_HOME="${RUSTUP_HOME:-$HOME/.rustup}"

failed=()
for t in "${TESTS[@]}"; do
  bin="$(awk -v t="$t" '$1 == t { print $2 }' "$manifest")"
  if [ -z "$bin" ]; then
    echo "no test executable found for $t"
    failed+=("$t")
    continue
  fi
  echo "::group::$t"
  # --include-ignored: most of these are #[ignore = "requires docker ..."].
  # --nocapture: surface SKIP lines from passing tests.
  # --test-threads=1: tests share catalog state (truncate_all_for_tests).
  # cwd matches what `cargo test` would use (the package root).
  if ! (cd "$root/tests/integration" && HOME="$test_home" "$bin" \
      --include-ignored --nocapture --test-threads=1) 2>&1 | tee -a "$log"; then
    failed+=("$t")
  fi
  echo "::endgroup::"
done

status=0
if [ "${#failed[@]}" -gt 0 ]; then
  echo "FAILED test binaries: ${failed[*]}"
  status=1
fi
if grep -nE '(^|[^A-Za-z_])SKIP[ :]' "$log"; then
  echo "Some tests skipped instead of running (see SKIP lines above); is Postgres reachable?"
  status=1
fi
exit "$status"
