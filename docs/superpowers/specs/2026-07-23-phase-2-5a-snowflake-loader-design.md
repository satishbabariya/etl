# Phase 2.5.a: Snowflake Destination Loader — Design

**Status:** Shipped 2026-07-25 (branch `phase-2-5a-snowflake-loader`).
**Plan:** `docs/superpowers/plans/2026-07-23-phase-2-5a-snowflake-loader.md`
**RFC:** RFC-0009 (Destination Loaders — Snowflake section).
**Precedent:** `docs/superpowers/specs/2026-05-21-phase-2-4a-postgres-loader-design.md` (the PG loader MVP shape this mirrors).

## 1. Scope

**In:**
- Third loader after `LocalParquetLoader` and `PostgresLoader`. Lives at `crates/worker/src/loaders/snowflake/` (module dir — the PG loader grew to 1,477 lines as a single file; we split client/SQL/loader from day one).
- Transport: **Snowflake SQL API v2** (`POST /api/v2/statements`) over reqwest with **key-pair JWT auth** (RS256). No Snowflake driver crate.
- Two delivery patterns, chosen by `pk_columns`:
  - Append (`pk_columns: []`) ⇒ multi-row `INSERT INTO ... VALUES`.
  - Upsert (`pk_columns: [...]`) ⇒ `MERGE INTO target USING (SELECT ... FROM VALUES ...) ON <pk> WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT`.
- Idempotency via a destination-side `"_ETL_LOADED_BATCHES"` log table (same design as the PG loader): check log → single multi-statement transactional request `BEGIN; <dml>; <log insert>; COMMIT`.
- Auto-`CREATE TABLE IF NOT EXISTS` for the target table on first non-empty batch.
- Type mapping covering the Arrow types our sources emit today (same set as the PG loader).
- Activity dispatch: `load_batch` routes `DestinationSpec::Snowflake(_)` to the new loader.
- Credentials: `private_key_env` — the spec names an **environment variable** holding the PKCS#8 PEM private key. No key material in the catalog (invariant #6: secrets never enter the catalog). This is deliberately stricter than the PG loader's inline-URL MVP shortcut.

**Out (deferred):**
- Stage + `COPY INTO` fast path (RFC-9's preferred bulk strategy; the `LOAD HISTORY FILES=` native idempotency comes with it). MVP renders SQL literals — fine at dogfood batch sizes, exactly like the PG loader's deferred `COPY FROM STDIN`.
- CDC `_cdc.op`-aware apply (MERGE with `WHEN MATCHED THEN DELETE`). Follows as 2-5b, mirroring 2-4b.
- Mid-run schema evolution (2-5c/d territory).
- Multi-table routing by `LoadId.stream_name` (single target table per pipeline for MVP; `stream_name` is still recorded in the idempotency log key).
- VARIANT/JSON semantic mapping, `NUMERIC`/decimal types.
- Secret-ref (RFC-11) indirection beyond the env-var scheme; Vault-resolved keys land with the loader-side secrets wiring.
- Warehouse sizing/auto-suspend management — we rely on Snowflake auto-resume; documented for operators.

## 2. Why SQL API v2 + reqwest instead of a driver crate

- The community crates (`snowflake-api`, `snowflake-connector-rs`) pull heavy dependency trees (object_store, AWS SDKs) for capabilities the MVP doesn't use (PUT to stage), and none is mature enough to bet the loader on.
- The workspace already carries everything needed: `reqwest` (rustls), `jsonwebtoken` 10 (RS256), `rsa` 0.9 (fingerprint), `sha2`, `base64` — the auth crate uses the identical stack.
- A plain HTTP client gives us a **wiremock test seam**: the full loader logic (DDL, idempotency short-circuit, transactional multi-statement, MERGE text) is integration-tested without a Snowflake account. The repo already uses exactly this pattern for Stripe (`stripe_e2e.rs`).
- Real-account verification is a separate env-gated test (`SNOWFLAKE_TEST_*`), matching the `VAULT_ADDR`-gated `vault_e2e.rs` convention and RFC-9's "destination tests against real accounts, not mocks" intent.

## 3. Auth: key-pair JWT

Snowflake SQL API accepts `Authorization: Bearer <jwt>` + `X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT` where the JWT is:

- `iss` = `<ACCOUNT>.<USER>.SHA256:<b64(sha256(der(public_key)))>`
- `sub` = `<ACCOUNT>.<USER>`
- account and user uppercased; account identifier without region suffix uses the part before the first `.`
- RS256-signed with the account's registered private key, ≤1h expiry (we mint 55-min tokens per client instance, re-minted per `load` call — no caching complexity).

`public_key_fingerprint` is derived from the private PEM via `rsa`: parse PKCS#8 → `to_public_key().to_public_key_der()` → SHA-256 → base64.

## 4. Idempotency strategy

Same contract as the PG loader, adapted to Snowflake's request model:

```sql
CREATE TABLE IF NOT EXISTS "<db>"."<schema>"."_ETL_LOADED_BATCHES" (
    TENANT_ID   TEXT   NOT NULL,
    PIPELINE_ID TEXT   NOT NULL,
    RUN_ID      TEXT   NOT NULL,
    STREAM_NAME TEXT   NOT NULL,
    BATCH_SEQ   BIGINT NOT NULL,
    ROWS_LOADED BIGINT NOT NULL,
    LOADED_AT   TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (TENANT_ID, PIPELINE_ID, RUN_ID, STREAM_NAME, BATCH_SEQ)
)
```

`load()` flow:
1. `SELECT ROWS_LOADED FROM _ETL_LOADED_BATCHES WHERE <full LoadId tuple>` — hit ⇒ short-circuit, `rows_loaded = 0`, no writes.
2. One multi-statement request (`MULTI_STATEMENT_COUNT = n`): `BEGIN; <INSERT or MERGE>; <log INSERT>; COMMIT;`.

The check and the write are separate requests (unlike the PG loader's single transaction), but Temporal serializes retries of a given activity, so there is no concurrent-writer race for a `LoadId`. Crash before COMMIT ⇒ nothing landed (transaction), retry re-runs. Crash after ⇒ log row exists, retry no-ops. Snowflake declares PKs but does not enforce them — the log-check discipline, not the constraint, provides the dedup.

## 5. SQL rendering

No bind parameters in MVP — batches are rendered as SQL literals by a unit-tested renderer (`sql.rs`):

| Arrow | Snowflake type (DDL) | Literal form |
|---|---|---|
| `Int64`/`Int32`/`Int16` | `BIGINT`/`INTEGER`/`SMALLINT` | bare digits |
| `Utf8`, `LargeUtf8` | `TEXT` | `'...'` with `''` escaping |
| `Boolean` | `BOOLEAN` | `TRUE`/`FALSE` |
| `Float64`/`Float32` | `DOUBLE`/`FLOAT` | bare, `'NaN'::DOUBLE` for NaN |
| `Timestamp(µs, Some(_))` | `TIMESTAMP_TZ` | `'2026-01-02T03:04:05.000006Z'::TIMESTAMP_TZ` |
| `Timestamp(µs, None)` | `TIMESTAMP_NTZ` | `'...'::TIMESTAMP_NTZ` |
| `Date32` | `DATE` | `'2026-01-02'::DATE` |
| `Binary`, `LargeBinary` | `BINARY` | `TO_BINARY('<hex>', 'HEX')` |
| `Time64(µs)` | `TIME` | `'03:04:05.000006'::TIME` |
| anything else | error `unsupported` | — |

Identifiers are always double-quoted, case preserved; names containing `"`, NUL, or control characters are rejected (same validation rule as the PG loader's `resolve_target_table`).

Upsert MERGE shape (per batch):

```sql
MERGE INTO "DB"."SCH"."T" AS t
USING (SELECT * FROM VALUES (...),(...) AS v(c1, c2, ...)) AS s
ON t."pk" = s."pk"
WHEN MATCHED THEN UPDATE SET t."c2" = s."c2", ...
WHEN NOT MATCHED THEN INSERT ("c1","c2",...) VALUES (s."c1", s."c2", ...)
```

MVP assumes unique PKs within a batch (same assumption the PG loader's `ON CONFLICT DO UPDATE` makes); the RFC-9 `ROW_NUMBER()` tie-breaker comes with the CDC-apply follow-up.

## 6. Client model

`SnowflakeClient` (`client.rs`): base URL `https://<account>.snowflakecomputing.com`, overridable via `ETL_SNOWFLAKE_BASE_URL` (the wiremock seam). `execute(sql)` posts a single statement and returns the JSON body; `execute_multi(stmts)` joins with `;` and sets `MULTI_STATEMENT_COUNT`. A `202 Accepted` response (async execution) is polled at `GET /api/v2/statements/<handle>` every 500 ms until terminal, bounded by a 60 s deadline. Non-2xx surfaces Snowflake's `message` field in the error.

Every request carries `database`, `schema`, `warehouse` (and `role` when set) in the body, so statements never depend on session defaults.

## 7. Spec shape

```jsonc
"destination": {
  "type": "snowflake",
  "account": "xy12345",            // account identifier (locator or org-account)
  "user": "ETL_LOADER",
  "private_key_env": "SNOWFLAKE_PRIVATE_KEY", // env var holding PKCS#8 PEM
  "warehouse": "ETL_WH",
  "database": "ANALYTICS",
  "schema": "RAW",
  "table": "customers",
  "role": "ETL_ROLE",              // optional
  "pk_columns": ["id"]             // empty ⇒ append
}
```

## 8. Tests

- Unit (in `crates/worker/src/loaders/snowflake/`): type mapping, literal rendering + escaping, identifier validation, DDL/INSERT/MERGE/log SQL builders, JWT claims + fingerprint (against a generated throwaway RSA key), client request shape + 202 polling (wiremock as a worker dev-dependency).
- Integration (`tests/integration/tests/snowflake_loader_mock.rs`, wiremock): loader end-to-end against a fake SQL API — append flow, upsert flow, idempotent replay short-circuit, transactional statement ordering.
- Real account (`tests/integration/tests/snowflake_loader.rs`): gated on `SNOWFLAKE_TEST_ACCOUNT/_USER/_PRIVATE_KEY/_WAREHOUSE/_DATABASE/_SCHEMA`; skips with a message otherwise. Append, upsert, idempotent retry — the same trio the PG loader proved first.

## 9. Next steps after MVP

1. **CDC op-aware MERGE** (`WHEN MATCHED AND s."_cdc.op"='d' THEN DELETE`) — 2-5b, mirroring 2-4b.
2. **Stage + COPY INTO fast path** with `LOAD HISTORY` idempotency — the RFC-9 bulk strategy.
3. **Schema evolution** (additive `ALTER TABLE ADD COLUMN`) — mirroring 2-4d.
4. **Multi-table routing** by `stream_name` — mirroring 2-4c.
5. **VARIANT** mapping for `json`-annotated columns.
6. **Secret-ref private keys** once RFC-11 wiring lands loader-side.
