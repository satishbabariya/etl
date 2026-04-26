pub struct AuditWriter;

#[derive(thiserror::Error, Debug)]
pub enum ChainError {
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error("hash mismatch at row id={0}")]
    HashMismatch(i64),
}
