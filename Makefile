.PHONY: stack stack-down stack-logs lib-tests build e2e pg-stack pg-integration

stack:
	docker compose up -d postgres temporal-postgres temporal vault
	./ci/wait-for-stack.sh

stack-down:
	docker compose down

stack-logs:
	docker compose logs -f --tail=100

pg-stack:
	docker compose up -d postgres
	./ci/wait-for-postgres.sh

lib-tests:
	cargo test --workspace --lib

build:
	cargo build --workspace

e2e: export ETL_CDC_CONTINUE_AS_NEW_AFTER = 3
e2e: stack build
	cargo test -p integration-tests --test mysql_cdc_wasm_e2e -- --ignored --nocapture --test-threads=1
	cargo test -p integration-tests --test postgres_cdc_wasm_e2e -- --ignored --nocapture --test-threads=1
	cargo test -p integration-tests --test wasm_connector -- --ignored --nocapture --test-threads=1
	cargo test -p integration-tests --test mysql_cdc_e2e -- --ignored --nocapture --test-threads=1
	cargo test -p integration-tests --test cdc_insert_update_delete -- --ignored --nocapture --test-threads=1
	cargo test -p integration-tests --test pg_loader_real_e2e -- --ignored --nocapture --test-threads=1

pg-integration: pg-stack build
	DATABASE_URL=postgres://etl:etl@localhost:5432/etl_catalog \
	DATABASE_URL_APP=postgres://etl_app:etl_app@localhost:5432/etl_catalog \
	ETL_INTEGRATION_PG_URL=postgres://etl:etl@localhost:5432/etl_catalog \
	./ci/pg-integration-tests.sh
