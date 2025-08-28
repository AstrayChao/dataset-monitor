use anyhow::Result;
use duckdb::{params, Connection};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::models::MonitorRecord;

pub struct DuckDB {
    conn: Arc<Mutex<Connection>>,
}

impl DuckDB {
    pub async fn new(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS dataset_monitor (
                id VARCHAR PRIMARY KEY,
                raw_id VARCHAR NOT NULL,
                url VARCHAR NOT NULL,
                name VARCHAR,
                center_name VARCHAR NOT NULL,
                date_published VARCHAR,
                sync_date TIMESTAMP NOT NULL,
                check_time TIMESTAMP NOT NULL,
                status INTEGER DEFAULT 0,
                error_msg VARCHAR,
            )",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_center ON dataset_monitor (center_name)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_check_time ON dataset_monitor (check_time)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_status ON dataset_monitor (status)",
            [],
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub async fn insert_records(&self, records: &[MonitorRecord]) -> Result<()> {
        let conn = self.conn.lock().await;
        let tx = conn.unchecked_transaction()?;

        for record in records {
            tx.execute(
                "INSERT OR REPLACE INTO dataset_monitor
                (id, raw_id, url, name, center_name, date_published, sync_date, check_time, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    &record.id,
                    &record.raw_id,
                    &record.url,
                    &record.name,
                    &record.center_name,
                    &record.date_published,
                    &record.sync_date.to_rfc3339(),
                    &record.check_time.to_rfc3339(),
                    &record.status
                ],
            )?;
        }

        tx.commit()?;
        info!("插入 {} 条监测记录", records.len());
        Ok(())
    }

    pub async fn update_status(&self, records: &[MonitorRecord]) -> Result<()> {
        let conn = self.conn.lock().await;
        let tx = conn.unchecked_transaction()?;

        for record in records {
            tx.execute(
                "UPDATE dataset_monitor
                SET status = ?, error_msg = ?, check_time = ?
                WHERE raw_id = ? AND check_time = ?",
                params![
                    &record.status,
                    &record.error_msg,
                    &record.check_time.to_rfc3339(),
                    &record.raw_id,
                    &record.check_time.to_rfc3339()
                ],
            )?;
        }

        tx.commit()?;
        Ok(())
    }

    // 分析查询方法
    pub async fn get_weekly_stats(&self, center_name: Option<&str>) -> Result<Vec<(String, f64)>> {
        let conn = self.conn.lock().await;

        let query = if let Some(name) = center_name {
            format!(
                "SELECT
                    DATE_TRUNC('week', check_time) as week,
                    COUNT(CASE WHEN status = 200 THEN 1 END) * 100.0 / COUNT(*) as success_rate
                FROM dataset_monitor
                WHERE center_name = '{}'
                GROUP BY week
                ORDER BY week DESC",
                name
            )
        } else {
            "SELECT
                DATE_TRUNC('week', check_time) as week,
                COUNT(CASE WHEN status = 200 THEN 1 END) * 100.0 / COUNT(*) as success_rate
            FROM dataset_monitor
            GROUP BY week
            ORDER BY week DESC".to_string()
        };

        let mut stmt = conn.prepare(&query)?;
        let results = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
        })?;

        Ok(results.filter_map(Result::ok).collect())
    }
}