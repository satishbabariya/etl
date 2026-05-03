.PHONY: stack stack-down stack-logs lib-tests build e2e

stack:
	docker compose up -d postgres temporal-postgres temporal vault
	./ci/wait-for-stack.sh

stack-down:
	docker compose down

stack-logs:
	docker compose logs -f --tail=100

lib-tests:
	cargo test --workspace --lib

build:
	cargo build --workspace

e2e: stack build
	cargo test -p integration-tests --test mysql_cdc_wasm_e2e -- --ignored --nocapture --test-threads=1
	cargo test -p integration-tests --test postgres_cdc_wasm_e2e -- --ignored --nocapture --test-threads=1
	cargo test -p integration-tests --test wasm_connector -- --ignored --nocapture --test-threads=1
	cargo test -p integration-tests --test mysql_cdc_e2e -- --ignored --nocapture --test-threads=1
