use crate::db::duckdb::DuckDB;
use crate::models::ProblematicUrl;
use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Deserialize)]
pub struct TimeRangeQuery {
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct StatsQuery {
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub center_name: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TimeStats {
    pub hour: String,
    pub total: i32,
    pub success: i32,
    pub failed: i32,
    pub success_rate: f64,
}

#[derive(Debug, Serialize)]
pub struct StatusCodeStats {
    pub status_code: Option<i32>,
    pub count: i32,
    pub percentage: f64,
}

#[derive(Debug, Serialize)]
pub struct CenterStats {
    pub center_name: String,
    pub total_checks: i32,
    pub success_count: i32,
    pub error_count: i32,
    pub success_rate: f64,
}

#[derive(Debug, Serialize)]
pub struct ProblemTypeStats {
    pub error_category: Option<String>,
    pub count: i32,
    pub local_issues: i32,
    pub remote_issues: i32,
}

#[derive(Clone)]
pub struct ApiState {
    pub duckdb: Arc<Mutex<DuckDB>>, // 使用Mutex包装DuckDB
}

pub fn create_router(duckdb: Arc<Mutex<DuckDB>>) -> Router {
    let state = ApiState {
        duckdb
    };
    Router::new()
        .route("/api/health", get(health_check))
        .route("/api/stats/overview", get(get_overview_stats))
        .route("/api/stats/time-range", get(get_time_range_stats))
        .route("/api/stats/status-code", get(get_status_code_stats))
        .route("/api/stats/center", get(get_center_stats))
        .route("/api/stats/problem-type", get(get_problem_type_stats))
        .with_state(state)
}

async fn health_check() -> Result<Json<serde_json::Value>, StatusCode> {
    Ok(Json(serde_json::json!({
        "status": "ok",
        "timestamp": Utc::now().to_rfc3339()
    })))
}

async fn get_overview_stats(
    State(state): State<ApiState>,
    Query(query): Query<StatsQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let db = state.duckdb.lock().await;

    let where_clause = build_where_clause(&query.start_time, &query.end_time, query.center_name.as_deref());

    let query_str = format!(
        "SELECT
            COUNT(*) as total_checks,
            SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN status_code != 200 OR status_code IS NULL THEN 1 ELSE 0 END) as error_count,
            AVG(response_time_ms) as avg_response_time
        FROM dataset_monitor {}",
        where_clause
    );

    let conn = db.conn.lock().await;
    let mut stmt = conn.prepare(&query_str)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut rows = stmt.query([])
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Some(row) = rows.next()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)? {
        let total_checks: i32 = row.get(0).unwrap_or(0);
        let success_count: i32 = row.get(1).unwrap_or(0);
        let error_count: i32 = row.get(2).unwrap_or(0);
        let avg_response_time: f64 = row.get(3).unwrap_or(0.0);

        let success_rate = if total_checks > 0 {
            (success_count as f64 / total_checks as f64) * 100.0
        } else {
            0.0
        };

        Ok(Json(serde_json::json!({
            "total_checks": total_checks,
            "success_count": success_count,
            "error_count": error_count,
            "success_rate": success_rate,
            "avg_response_time_ms": avg_response_time
        })))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn get_time_range_stats(
    State(state): State<ApiState>,
    Query(query): Query<TimeRangeQuery>,
) -> Result<Json<Vec<TimeStats>>, StatusCode> {
    let db = state.duckdb.lock().await;

    let where_clause = build_where_clause(&query.start_time, &query.end_time, None);

    let query_str = format!(
        "SELECT
            strftime('%Y-%m-%d %H:00:00', check_time) as hour,
            COUNT(*) as total,
            SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END) as success,
            SUM(CASE WHEN status_code != 200 OR status_code IS NULL THEN 1 ELSE 0 END) as failed
        FROM dataset_monitor
        {}
        GROUP BY strftime('%Y-%m-%d %H:00:00', check_time)
        ORDER BY hour",
        where_clause
    );

    let conn = db.conn.lock().await;
    let mut stmt = conn.prepare(&query_str)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let rows = stmt.query_map([], |row| {
        let total: i32 = row.get(1)?;
        let success: i32 = row.get(2)?;
        let failed: i32 = row.get(3)?;
        let success_rate = if total > 0 {
            (success as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        Ok(TimeStats {
            hour: row.get(0)?,
            total,
            success,
            failed,
            success_rate,
        })
    }).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let stats: Vec<TimeStats> = rows.collect::<Result<Vec<_>, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(stats))
}

async fn get_status_code_stats(
    State(state): State<ApiState>,
    Query(query): Query<StatsQuery>,
) -> Result<Json<Vec<StatusCodeStats>>, StatusCode> {
    let db = state.duckdb.lock().await;

    let where_clause = build_where_clause(&query.start_time, &query.end_time, query.center_name.as_deref());

    let query_str = format!(
        "SELECT
            status_code,
            COUNT(*) as count
        FROM dataset_monitor
        {}
        GROUP BY status_code
        ORDER BY count DESC",
        where_clause
    );

    let conn = db.conn.lock().await;
    let mut stmt = conn.prepare(&query_str)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let rows = stmt.query_map([], |row| {
        Ok((row.get(0)?, row.get(1)?))
    }).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut total_count = 0i32;
    let mut stats_data = Vec::new();

    for row in rows {
        if let Ok((status_code, count)) = row {
            total_count += count;
            stats_data.push((status_code, count));
        }
    }

    let stats: Vec<StatusCodeStats> = stats_data.into_iter().map(|(status_code, count)| {
        let percentage = if total_count > 0 {
            (count as f64 / total_count as f64) * 100.0
        } else {
            0.0
        };

        StatusCodeStats {
            status_code,
            count,
            percentage,
        }
    }).collect();

    Ok(Json(stats))
}

async fn get_center_stats(
    State(state): State<ApiState>,
    Query(query): Query<StatsQuery>,
) -> Result<Json<Vec<CenterStats>>, StatusCode> {
    let db = state.duckdb.lock().await;

    let where_clause = build_where_clause(&query.start_time, &query.end_time, query.center_name.as_deref());

    let query_str = format!(
        "SELECT
            center_name,
            COUNT(*) as total_checks,
            SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN status_code != 200 OR status_code IS NULL THEN 1 ELSE 0 END) as error_count
        FROM dataset_monitor
        {}
        GROUP BY center_name
        ORDER BY total_checks DESC",
        where_clause
    );

    let conn = db.conn.lock().await;
    let mut stmt = conn.prepare(&query_str)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let rows = stmt.query_map([], |row| {
        let total_checks: i32 = row.get(1)?;
        let success_count: i32 = row.get(2)?;
        let error_count: i32 = row.get(3)?;
        let success_rate = if total_checks > 0 {
            (success_count as f64 / total_checks as f64) * 100.0
        } else {
            0.0
        };

        Ok(CenterStats {
            center_name: row.get(0)?,
            total_checks,
            success_count,
            error_count,
            success_rate,
        })
    }).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let stats: Vec<CenterStats> = rows.collect::<Result<Vec<_>, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(stats))
}

async fn get_problem_type_stats(
    State(state): State<ApiState>,
    Query(query): Query<StatsQuery>,
) -> Result<Json<Vec<ProblemTypeStats>>, StatusCode> {
    let db = state.duckdb.lock().await;

    let where_clause = build_where_clause(&query.start_time, &query.end_time, query.center_name.as_deref());

    let query_str = format!(
        "SELECT
            error_category,
            COUNT(*) as count,
            SUM(CASE WHEN is_likely_local_issue = true THEN 1 ELSE 0 END) as local_issues,
            SUM(CASE WHEN is_likely_local_issue = false AND error_category IS NOT NULL THEN 1 ELSE 0 END) as remote_issues
        FROM dataset_monitor
        {}
        GROUP BY error_category
        ORDER BY count DESC",
        where_clause
    );

    let conn = db.conn.lock().await;
    let mut stmt = conn.prepare(&query_str)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let rows = stmt.query_map([], |row| {
        Ok(ProblemTypeStats {
            error_category: row.get(0)?,
            count: row.get(1)?,
            local_issues: row.get(2)?,
            remote_issues: row.get(3)?,
        })
    }).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let stats: Vec<ProblemTypeStats> = rows.collect::<Result<Vec<_>, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(stats))
}

async fn get_problematic_urls(
    State(state): State<ApiState>,
    Query(query): Query<StatsQuery>,
) -> Result<Json<Vec<ProblematicUrl>>, StatusCode> {
    let db = state.duckdb.lock().await;

    // 这里可以复用你已有的 get_problematic_urls 方法
    // 为了简化，我们直接查询
    let where_clause = if let Some(center_name) = &query.center_name {
        format!("WHERE center_name = '{}' AND (status_code != 200 OR status_code IS NULL)", center_name)
    } else {
        "WHERE status_code != 200 OR status_code IS NULL".to_string()
    };

    let time_conditions = build_time_conditions(&query.start_time, &query.end_time);
    let full_where = if time_conditions.is_empty() {
        where_clause
    } else {
        format!("{} AND {}", where_clause, time_conditions)
    };

    let query_str = format!(
        "SELECT
            url,
            center_name,
            name,
            COUNT(*) as total_checks,
            SUM(CASE WHEN status_code != 200 OR status_code IS NULL THEN 1 ELSE 0 END) as failed_checks,
            AVG(response_time_ms) as avg_response_time,
            MAX(check_time) as last_check,
            MAX(error_msg) as last_error
        FROM dataset_monitor
        {}
        GROUP BY url, center_name, name
        HAVING failed_checks > 0
        ORDER BY failed_checks DESC
        LIMIT 100",
        full_where
    );

    let conn = db.conn.lock().await;
    let mut stmt = conn.prepare(&query_str)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let rows = stmt.query_map([], |row| {
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
    }).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let urls: Vec<ProblematicUrl> = rows.collect::<Result<Vec<_>, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(urls))
}

fn build_where_clause(
    start_time: &Option<DateTime<Utc>>,
    end_time: &Option<DateTime<Utc>>,
    center_name: Option<&str>,
) -> String {
    let mut conditions = Vec::new();

    if let Some(start) = start_time {
        conditions.push(format!("check_time >= '{}'", start.to_rfc3339()));
    }

    if let Some(end) = end_time {
        conditions.push(format!("check_time <= '{}'", end.to_rfc3339()));
    }

    if let Some(center) = center_name {
        conditions.push(format!("center_name = '{}'", center));
    }

    if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    }
}

fn build_time_conditions(
    start_time: &Option<DateTime<Utc>>,
    end_time: &Option<DateTime<Utc>>,
) -> String {
    let mut conditions = Vec::new();

    if let Some(start) = start_time {
        conditions.push(format!("check_time >= '{}'", start.to_rfc3339()));
    }

    if let Some(end) = end_time {
        conditions.push(format!("check_time <= '{}'", end.to_rfc3339()));
    }

    conditions.join(" AND ")
}
