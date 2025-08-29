use anyhow::Result;
use duckdb::{params, Connection};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::models::{ErrorCategoryStats, HealthCheck, MonitorRecord, NetworkIssueTrend, ProblematicUrl, UrlHealthReport};

pub struct DuckDB {
    conn: Arc<Mutex<Connection>>,
}

impl DuckDB {
    pub async fn new(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS dataset_monitor (
                id VARCHAR,
                raw_id VARCHAR NOT NULL,
                url VARCHAR NOT NULL,
                name VARCHAR,
                center_name VARCHAR NOT NULL,
                date_published VARCHAR,
                sync_date TIMESTAMP NOT NULL,
                check_time TIMESTAMP NOT NULL,
               -- HTTP响应信息
                status_code INTEGER,
                status_text VARCHAR,

                -- 错误信息
                error_category VARCHAR,
                error_msg TEXT,
                error_detail TEXT,

                -- 请求元数据
                response_time_ms BIGINT,

                -- 诊断信息
                is_likely_local_issue BOOLEAN DEFAULT FALSE,
                headers TEXT,
                -- 添加创建时间和更新时间
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            )",
            [],
        )?;
        let indices = vec![
            "CREATE INDEX IF NOT EXISTS idx_center ON dataset_monitor (center_name)",
            "CREATE INDEX IF NOT EXISTS idx_check_time ON dataset_monitor (check_time)",
            "CREATE INDEX IF NOT EXISTS idx_status_code ON dataset_monitor (status_code)",
            "CREATE INDEX IF NOT EXISTS idx_error_category ON dataset_monitor (error_category)",
            "CREATE INDEX IF NOT EXISTS idx_is_local ON dataset_monitor (is_likely_local_issue)",
            "CREATE INDEX IF NOT EXISTS idx_url ON dataset_monitor (url)",
            "CREATE INDEX IF NOT EXISTS idx_raw_id ON dataset_monitor (raw_id)",
            "CREATE INDEX IF NOT EXISTS idx_response_time ON dataset_monitor (response_time_ms)",
        ];

        for index_sql in indices {
            conn.execute(index_sql, [])?;
        }
        info!("DuckDB 初始化完成，数据库路径: {}", path);
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub async fn insert_records(&self, records: &[MonitorRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let conn = self.conn.lock().await;
        let mut appender = conn.appender("dataset_monitor")?;
        for record in records {
            let error_category_str = record.error_category
                .as_ref()
                .map(|e| serde_json::to_string(e).unwrap_or_default());
            appender.append_row(params![
                        &record.id,
                        &record.url,
                        &record.name,
                        &record.center_name,
                        &record.date_published,
                        &record.sync_date.to_rfc3339(),
                        &record.check_time.to_rfc3339(),
                        &record.status_code,
                        &record.status_text,
                        &error_category_str,
                        &record.error_msg,
                        &record.error_detail,
                        &record.response_time_ms.map(|t| t as i64),
                        &record.is_likely_local_issue,
                        &record.headers,
                        // created_at 使用默认值 CURRENT_TIMESTAMP
                        &chrono::Utc::now().to_rfc3339(),
                        &chrono::Utc::now().to_rfc3339()  // updated_at
                    ])?
        }
        appender.flush()?;
        info!("插入 {} 条监测记录", records.len());
        Ok(())
    }

    pub async fn update_status(&self, records: &[MonitorRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let conn = self.conn.lock().await;
        let tx = conn.unchecked_transaction()?;
        {
            tx.execute(
                "CREATE TEMPORARY TABLE temp_updates (
                    id VARCHAR,
                    status_code INTEGER,
                    status_text VARCHAR,
                    error_category VARCHAR,
                    error_msg TEXT,
                    error_detail TEXT,
                    response_time_ms BIGINT,
                    is_likely_local_issue BOOLEAN,
                    headers TEXT,
                    check_time TIMESTAMP
                )",
                [],
            )?;
            let mut appender = tx.appender("temp_updates")?;
            for record in records {
                let error_category_str = record.error_category
                    .as_ref()
                    .map(|e| serde_json::to_string(e).unwrap_or_default());

                appender.append_row(params![
                    &record.id,
                    &record.status_code,
                    &record.status_text,
                    &error_category_str,
                    &record.error_msg,
                    &record.error_detail,
                    &record.response_time_ms.map(|t| t as i64),
                    &record.is_likely_local_issue,
                    &record.headers,
                    &record.check_time.to_rfc3339()
                ])?;
            }
            appender.flush()?;
            tx.execute(
                "UPDATE dataset_monitor AS m
                SET
                    status_code = t.status_code,
                    status_text = t.status_text,
                    error_category = t.error_category,
                    error_msg = t.error_msg,
                    error_detail = t.error_detail,
                    response_time_ms = t.response_time_ms,
                    is_likely_local_issue = t.is_likely_local_issue,
                    headers = t.headers,
                    check_time = t.check_time,
                    updated_at = CURRENT_TIMESTAMP
                FROM temp_updates AS t
                WHERE m.id = t.id",
                [],
            )?;
            tx.execute("DROP TABLE temp_updates", [])?;
        }
        tx.commit()?;
        info!("批量更新 {} 条记录状态", records.len());
        Ok(())
    }

    // 获取问题URL列表
    pub async fn get_problematic_urls(
        &self,
        center_name: Option<&str>,
        min_failure_rate: f64,
    ) -> Result<Vec<ProblematicUrl>> {
        let conn = self.conn.lock().await;

        let where_clause = if let Some(name) = center_name {
            format!("WHERE h.center_name = '{}'", name)
        } else {
            String::new()
        };

        let query = format!(
            "WITH url_stats AS (
                SELECT
                    h.url,
                    h.center_name,
                    m.name,
                    COUNT(*) as total_checks,
                    SUM(CASE WHEN h.status_code != 200 OR h.status_code IS NULL THEN 1 ELSE 0 END) as failed_checks,
                    AVG(h.response_time_ms) as avg_response_time,
                    MAX(h.check_time) as last_check,
                    m.error_msg as last_error
                FROM dataset_monitor_history h
                JOIN dataset_monitor m ON h.id = m.id
                {}
                GROUP BY h.url, h.center_name, m.name, m.error_msg
                HAVING (failed_checks * 100.0 / total_checks) >= {}
            )
            SELECT * FROM url_stats
            ORDER BY (failed_checks * 100.0 / total_checks) DESC
            LIMIT 100",
            where_clause, min_failure_rate
        );

        let mut stmt = conn.prepare(&query)?;
        let results = stmt.query_map([], |row| {
            let total_checks: i32 = row.get(3)?;
            let failed_checks: i32 = row.get(4)?;
            let failure_rate = if total_checks > 0 {
                (failed_checks as f64 / total_checks as f64) * 100.0
            } else {
                0.0
            };

            Ok(ProblematicUrl {
                url: row.get(0)?,
                center_name: row.get(1)?,
                name: row.get(2)?,
                total_checks,
                failed_checks,
                failure_rate,
                avg_response_time_ms: row.get(5)?,
                last_check: row.get(6)?,
                last_error: row.get(7)?,
            })
        })?;

        Ok(results.filter_map(Result::ok).collect())
    }
}