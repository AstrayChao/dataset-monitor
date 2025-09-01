use chrono::{DateTime, Utc};
use mongodb::bson::oid::ObjectId;
use mongodb::bson::Bson;
use serde::{Deserialize, Serialize};
use std::error::Error;

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthResponse {
    pub ticket: Ticket,
    #[serde(rename = "serviceList")]
    pub service_list: Vec<Service>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Ticket {
    pub expires: i64,
    pub token: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Service {
    pub name: String,
    pub version: String,
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Dataset {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub _id: Option<ObjectId>,
    #[serde(rename = "@id", default)]
    pub raw_id: String,
    #[serde(rename = "casdc_id", default)]
    pub casdc_id: Option<String>,
    #[serde(rename = "@type", default)]
    pub data_type: Option<Bson>,
    #[serde(rename = "schema:url")]
    pub url: Option<Bson>,
    #[serde(rename = "schema:name", default)]
    pub name: Option<Bson>,
    #[serde(rename = "schema:datePublished", default)]
    pub date_published: Option<Bson>,
    #[serde(rename = "syncDate", default)]
    pub sync_date: Option<DateTime<Utc>>,
    #[serde(rename = "centerName", default)]
    pub center_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorRecord {
    pub id: String,
    pub raw_id: Option<String>,
    pub url: String,
    pub name: Option<String>,
    pub center_name: String,
    pub date_published: Option<String>,
    pub sync_date: DateTime<Utc>,
    pub check_time: DateTime<Utc>,

    // HTTP响应信息
    pub status_code: Option<i32>,
    pub status_text: Option<String>,

    // 错误信息
    pub error_category: Option<ErrorCategory>,
    pub error_msg: Option<String>,
    pub error_detail: Option<String>,

    // 请求元数据
    pub response_time_ms: Option<u64>,
    // 诊断信息
    pub is_likely_local_issue: bool,
    pub headers: Option<String>,
}

/// 错误分类枚举
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ErrorCategory {
    /// 网络连接问题（本地网络问题）
    NetworkConnection,
    /// DNS解析失败
    DnsResolution,
    /// 连接超时
    Timeout,
    /// SSL/TLS证书问题
    SslCertificate,
    /// 服务器拒绝连接
    ConnectionRefused,
    /// 服务器错误（5xx）
    ServerError,
    /// 客户端错误（4xx）
    ClientError,
    /// 重定向过多
    TooManyRedirects,
    /// 请求被取消
    RequestCanceled,
    /// 未知错误
    Unknown,
}

impl ErrorCategory {
    /// 根据reqwest错误判断错误类别
    pub fn from_request_error(e: &reqwest::Error) -> Self {
        if e.is_timeout() {
            ErrorCategory::Timeout
        } else if e.is_connect() {
            // 连接错误，可能是网络问题或服务器拒绝
            if let Some(source) = e.source() {
                let error_str = source.to_string().to_lowercase();
                if error_str.contains("connection refused") {
                    ErrorCategory::ConnectionRefused
                } else if error_str.contains("dns") || error_str.contains("resolve") {
                    ErrorCategory::DnsResolution
                } else if error_str.contains("network unreachable") ||
                    error_str.contains("no route to host") {
                    ErrorCategory::NetworkConnection
                } else {
                    ErrorCategory::NetworkConnection
                }
            } else {
                ErrorCategory::NetworkConnection
            }
        } else if e.is_redirect() {
            ErrorCategory::TooManyRedirects
        } else if e.is_request() {
            ErrorCategory::RequestCanceled
        } else if let Some(source) = e.source() {
            let error_str = source.to_string().to_lowercase();
            if error_str.contains("ssl") || error_str.contains("tls") ||
                error_str.contains("certificate") {
                ErrorCategory::SslCertificate
            } else {
                ErrorCategory::Unknown
            }
        } else {
            ErrorCategory::Unknown
        }
    }

    /// 判断是否可能是本地网络问题
    pub fn is_likely_local_issue(&self) -> bool {
        matches!(self,
            ErrorCategory::NetworkConnection |
            ErrorCategory::DnsResolution |
            ErrorCategory::Timeout |
            ErrorCategory::RequestCanceled
        )
    }
}

// 辅助结构体定义
#[derive(Debug)]
pub struct ErrorCategoryStats {
    pub category: String,
    pub count: i32,
    pub avg_response_time_ms: Option<f64>,
    pub max_response_time_ms: Option<i64>,
    pub local_issues: i32,
}

#[derive(Debug)]
pub struct UrlHealthReport {
    pub url: String,
    pub total_checks: usize,
    pub successful_checks: usize,
    pub availability: f64,
    pub avg_response_time_ms: f64,
    pub recent_checks: Vec<HealthCheck>,
}

#[derive(Debug)]
pub struct HealthCheck {
    pub check_time: String,
    pub status_code: Option<i32>,
    pub error_category: Option<String>,
    pub response_time_ms: Option<i64>,
    pub is_likely_local_issue: bool,
}

#[derive(Debug)]
pub struct NetworkIssueTrend {
    pub hour: String,
    pub total_checks: i32,
    pub local_issues: i32,
    pub remote_issues: i32,
    pub local_issue_rate: f64,
}

#[derive(Debug)]
pub struct ProblematicUrl {
    pub url: String,
    pub center_name: String,
    pub name: Option<String>,
    pub total_checks: i32,
    pub failed_checks: i32,
    pub failure_rate: f64,
    pub avg_response_time_ms: Option<f64>,
    pub last_check: String,
    pub last_error: Option<String>,
}
#[derive(Debug)]
pub struct ResponseInfo {
    pub(crate) status_code: i32,
    pub(crate) status_text: String,
    pub(crate) headers: Option<String>,
}

#[derive(Debug)]
pub struct CheckError {
    pub(crate) category: ErrorCategory,
    pub(crate) message: String,
    pub(crate) detail: String,
    pub(crate) status_code: Option<i32>,
}
impl Dataset {
    pub fn extract_url(&self) -> Option<String> {
        match &self.url {
            Some(Bson::String(s)) => Some(s.clone()),
            _ => None,
        }
    }

    pub fn extract_name(&self) -> String {
        match &self.name {
            Some(Bson::String(s)) => s.clone(),
            Some(Bson::Document(doc)) => {
                doc.get("@value")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string()
            }
            Some(Bson::Array(arr)) => {
                arr.first()
                    .and_then(|item| match item {
                        Bson::String(s) => Some(s.clone()),
                        Bson::Document(d) => d.get("@value")
                            .and_then(|v| v.as_str().map(String::from)),
                        _ => None,
                    })
                    .unwrap_or_else(|| "unknown".to_string())
            }
            _ => "Unknown".to_string(),
        }
    }

    pub fn extract_date_published(&self) -> String {
        match &self.date_published {
            Some(Bson::String(s)) => s.clone(),
            Some(Bson::DateTime(dt)) => dt.to_string(),
            _ => "unknown".to_string(),
        }
    }
}