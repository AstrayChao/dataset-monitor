pub mod mongodb;
pub mod duckdb;


use anyhow::Result;

pub async fn init_duckdb(path: &str) -> Result<()> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let db = duckdb::DuckDB::new(path).await?;
    Ok(())
}