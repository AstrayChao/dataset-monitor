use crate::config::Config;
use crate::db::duckdb::DuckDB;
use crate::db::mongodb::MongoDB;
use crate::models::{CheckError, Dataset, ErrorCategory, MonitorRecord, ResponseInfo};
use anyhow::Result;
use chrono::Utc;
use futures::{stream, StreamExt};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{info, warn};

pub struct DataMonitor {
    config: Arc<Config>,
    client: reqwest::Client,
}

impl DataMonitor {
    pub fn new(config: Arc<Config>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.monitor.http_timeout_secs))
            .redirect(reqwest::redirect::Policy::limited(10))
            .danger_accept_invalid_hostnames(true)
            .danger_accept_invalid_certs(true)
            .build()
            .expect("failed to build http client");
        Self { config, client }
    }

    pub async fn check_all_urls(&self) -> Result<()> {
        info!("开始数据监测任务");
        let mongo = MongoDB::new(&self.config.mongodb).await?;

        let mut all_datasets = Vec::new();
        let duckdb_ = DuckDB::new(&self.config.duckdb.path).await?;
        for center in &self.config.centers {
            let datasets = mongo.get_datasets(&center.name).await?;
            info!("数据中心 {} 有 {} 个数据集", center.name, datasets.len());
            all_datasets.extend(datasets);
        }
        info!("总共需要监测 {} 个数据集", all_datasets.len());

        let records: Vec<MonitorRecord> = all_datasets
            .into_iter()
            .filter_map(|ds| self.dataset_to_record(ds))
            .collect();

        info!("有效URL数量: {}", records.len());
        duckdb_.insert_records(&records).await?;

        // 并发监测URL
        let results = stream::iter(records)
            .map(|record| self.process_record(record))
            .buffer_unordered(self.config.monitor.max_concurrent)
            .collect::<Vec<_>>()
            .await;

        duckdb_.update_status(&results).await?;
        let success_count = results.iter()
            .filter(|r| r.status_code == Some(200))
            .count();
        let local_issue_count = results.iter()
            .filter(|r| r.is_likely_local_issue)
            .count();
        let remote_issue_count = results.iter()
            .filter(|r| r.error_category.is_some() && !r.is_likely_local_issue)
            .count();
        info!(
            "监测完成: 成功 {}/{}, 本地网络问题 {}, 远程问题 {}",
            success_count,
            results.len(),
            local_issue_count,
            remote_issue_count
        );
        // 如果本地网络问题过多，发出警告
        if local_issue_count > results.len() / 10 {
            warn!("检测到大量本地网络问题 ({}/{}), 请检查网络连接",
                  local_issue_count, results.len());
        }
        Ok(())
    }
    async fn process_record(&self, mut record: MonitorRecord) -> MonitorRecord {
        let start_time = std::time::Instant::now();
        info!("开始检查URL: {}", &record.url);
        let check_result = self.check_url(&self.client, &record.url).await;

        record.response_time_ms = Some(start_time.elapsed().as_millis() as u64);
        record.check_time = Utc::now();

        self.handle_check_result(&mut record, check_result);
        info!("完成检查URL: {}, 状态码: {:?}", record.url, record.status_code);
        record
    }
    fn handle_check_result(&self, record: &mut MonitorRecord, check_result: Result<ResponseInfo, CheckError>) {
        match check_result {
            Ok(response_info) => {
                record.status_code = Some(response_info.status_code);
                record.status_text = Some(response_info.status_text);
                record.headers = response_info.headers;
                record.error_category = None;
                record.error_msg = None;
                record.error_detail = None;
                record.is_likely_local_issue = false;
            }
            Err(e) => {
                record.status_code = e.status_code;
                record.error_category = Some(e.category.to_string());
                record.error_msg = Some(e.message);
                record.error_detail = Some(e.detail);
                record.is_likely_local_issue = e.category.is_likely_local_issue();
            }
        }
    }
    fn dataset_to_record(&self, dataset: Dataset) -> Option<MonitorRecord> {
        let url = dataset.extract_url()?;
        Some(MonitorRecord {
            id: dataset._id.map(|id| id.to_string()).unwrap_or_default(),
            raw_id: Option::from(dataset.raw_id.clone()),
            url,
            name: Option::from(dataset.extract_name()),
            center_name: dataset.center_name.clone().unwrap_or_else(|| "Unknown".to_string()),
            date_published: Option::from(dataset.extract_date_published()),
            check_time: Utc::now(),
            status_code: None,
            status_text: None,
            error_category: None,
            error_msg: None,
            error_detail: None,
            response_time_ms: None,
            is_likely_local_issue: false,
            headers: None,
            created_at: None,
            updated_at: None,
        })
    }

    async fn check_url(&self, client: &reqwest::Client, url: &str) -> Result<ResponseInfo, CheckError> {
        match client.head(url).header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
                              AppleWebKit/537.36 (KHTML, like Gecko) \
                              Chrome/127.0.0.0 Safari/537.36")
            .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
            .header("Accept-Language", "en-US,en;q=0.5")
            .header("Connection", "keep-alive")
            .send().await {
            Ok(response) => {
                let status = response.status();
                let status_code = status.as_u16();
                let status_text = status.canonical_reason()
                    .unwrap_or("Unknown")
                    .to_string();
                let headers = response.headers()
                    .iter()
                    .map(|(k, v)| format!("{}: {:?}", k, v))
                    .collect::<Vec<_>>()
                    .join(", ");
                if status.is_success() {
                    Ok(ResponseInfo {
                        status_code,
                        status_text,
                        headers: Some(headers),
                    })
                } else if status.is_server_error() {
                    Err(CheckError {
                        category: ErrorCategory::ServerError,
                        message: format!("服务器错误: {}", status),
                        detail: format!("状态码: {}, 原因: {}", status_code, status_text),
                        status_code: Some(status_code),
                    })
                } else if status.is_client_error() {
                    Err(CheckError {
                        category: ErrorCategory::ClientError,
                        message: format!("客户端错误: {}", status),
                        detail: format!("状态码: {}, 原因: {}", status_code, status_text),
                        status_code: Some(status_code),
                    })
                } else {
                    Ok(ResponseInfo {
                        status_code,
                        status_text,
                        headers: Some(headers),
                    })
                }
            }
            Err(e) => {
                let category = ErrorCategory::from_request_error(&e);
                let detail = format!(
                    "错误详情: {}\n错误链: {:?}\n是否超时: {}\n是否连接错误: {}\n是否重定向错误: {}",
                    e,
                    e.source().map(|s| s.to_string()).unwrap_or_default(),
                    e.is_timeout(),
                    e.is_connect(),
                    e.is_redirect()
                );
                Err(CheckError {
                    category,
                    message: e.to_string(),
                    detail,
                    status_code: e.status().map(|s| s.as_u16()),
                })
            }
        }
    }
}