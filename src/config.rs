use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub centers: Vec<Center>,
    pub mongodb: MongoDBConfig,
    pub duckdb: DuckDBConfig,
    pub monitor: MonitorConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Center {
    pub name: String,
    pub secret_key: String,
    pub url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MongoDBConfig {
    pub uri: String,
    pub database: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DuckDBConfig {
    pub path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MonitorConfig {
    pub fetch_interval_days: u32,
    pub check_interval_days: u32,
    pub http_timeout_secs: u64,
    pub max_concurrent: usize,
    pub retry_times: u32,
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config = serde_yaml::from_str(&content)?;
        Ok(config)
    }
}