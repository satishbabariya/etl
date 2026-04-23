# upper-case-scalar — Phase I.5 reference scalar UDF

Uppercases each input string. Exercises the `platform:udf/scalar-udf`
world with tight capabilities (log only).

```bash
cd examples/upper-case-scalar
cargo build --release
cargo run --bin platform -- connector build examples/upper-case-scalar --kind scalar
```
