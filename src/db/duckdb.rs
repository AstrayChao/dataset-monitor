use anyhow::Result;
use duckdb::{params, Connection};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::models::{ErrorCategoryStats, HealthCheck, MonitorRecord, NetworkIssueTrend, ProblematicUrl, UrlHealthReport};

pub struct DuckDB {
    pub conn: Arc<Mutex<Connection>>,
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
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;
        let indices = vec![
            // 主键索引
            "CREATE INDEX IF NOT EXISTS idx_id ON dataset_monitor (id)",

            // URL相关索引（频繁查询）
            "CREATE INDEX IF NOT EXISTS idx_url ON dataset_monitor (url)",
            "CREATE INDEX IF NOT EXISTS idx_raw_id ON dataset_monitor (raw_id)",

            // 核心分析维度的组合索引（按使用频率和重要性排序）

            // 1. 时间+状态码（最核心的健康检查分析）
            "CREATE INDEX IF NOT EXISTS idx_check_time_status ON dataset_monitor (check_time, status_code)",

            // 2. 时间+数据中心（按数据中心分析）
            "CREATE INDEX IF NOT EXISTS idx_check_time_center ON dataset_monitor (check_time, center_name)",

            // 3. 时间+本地问题标识（区分本地/远程问题）
            "CREATE INDEX IF NOT EXISTS idx_check_time_local_issue ON dataset_monitor (check_time, is_likely_local_issue)",

            // 4. 时间+错误分类（错误类型分析）
            "CREATE INDEX IF NOT EXISTS idx_check_time_error_category ON dataset_monitor (check_time, error_category)",

            // 5. 多维度组合索引（复杂分析查询）
            "CREATE INDEX IF NOT EXISTS idx_time_center_status ON dataset_monitor (check_time, center_name, status_code)",
            "CREATE INDEX IF NOT EXISTS idx_center_status_local ON dataset_monitor (center_name, status_code, is_likely_local_issue)",

            // 单列索引（用于特定查询和辅助过滤）
            "CREATE INDEX IF NOT EXISTS idx_check_time ON dataset_monitor (check_time)",
            "CREATE INDEX IF NOT EXISTS idx_status_code ON dataset_monitor (status_code)",
            "CREATE INDEX IF NOT EXISTS idx_center_name ON dataset_monitor (center_name)",
            "CREATE INDEX IF NOT EXISTS idx_is_likely_local_issue ON dataset_monitor (is_likely_local_issue)",
            "CREATE INDEX IF NOT EXISTS idx_error_category ON dataset_monitor (error_category)",
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
        let tx = conn.unchecked_transaction()?;
        {
            let mut appender = conn.appender("dataset_monitor")?;
            for record in records {
                let error_category_str = record.error_category.clone();
                let created_at_str = record.created_at.as_ref().map(|dt| dt.to_rfc3339());
                let updated_at_str = record.updated_at.as_ref().map(|dt| dt.to_rfc3339());

                appender.append_row(params![
                            &record.id,
                            &record.raw_id,
                            &record.url,
                            &record.name,
                            &record.center_name,
                            &record.date_published,
                            &record.check_time.to_rfc3339(),
                            &record.status_code,
                            &record.status_text,
                            &error_category_str,
                            &record.error_msg,
                            &record.error_detail,
                            &record.response_time_ms.map(|t| t as i64),
                            &record.is_likely_local_issue,
                            &record.headers,
                            &created_at_str,
                            &updated_at_str
                        ])?
            }
            appender.flush()?;
        }
        tx.commit()?;
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
                let error_category_str = record.error_category.clone();

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