//! Reference scalar UDF: uppercase each input string.

wit_bindgen::generate!({
    path: "../../crates/connector-sdk/wit-scalar",
    world: "scalar-udf",
});

use platform::udf::host::{log, LogLevel};

struct Component;

export!(Component);

impl Guest for Component {
    fn apply_scalar(input: Vec<String>) -> Result<Vec<String>, String> {
        log(LogLevel::Info, &format!("upper-case-scalar: {} rows", input.len()));
        Ok(input.into_iter().map(|s| s.to_uppercase()).collect())
    }
}
