use crate::config::Config;
use crate::db::duckdb::DuckDB;
use crate::db::mongodb::MongoDB;
use crate::models::{Dataset, MonitorRecord};
use anyhow::Result;
use chrono::Utc;
use futures::{stream, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

pub struct DataMonitor {
    config: Arc<Config>,
    client: reqwest::Client,
}

impl DataMonitor {
    pub fn new(config: Arc<Config>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.monitor.http_timeout_secs))
            .redirect(reqwest::redirect::Policy::limited(4))
            .build()
            .expect("failed to build http client");
        Self { config, client }
    }

    pub async fn check_all_urls(&self) -> Result<()> {
        info!("开始数据监测任务");
        let mongo = MongoDB::new(&self.config.mongodb).await?;

        let mut all_datasets = Vec::new();

        for center in &self.config.centers {
            let datasets = mongo.get_datasets(&center.name).await?;
            info!("数据中心 {} 有 {} 个数据集", center.name, datasets.len());
            all_datasets.extend(datasets);
        }
        info!("总共需要监测 {} 个数据集", all_datasets.len());

        // 转换为监测记录
        let records: Vec<MonitorRecord> = all_datasets
            .into_iter()
            .filter_map(|ds| self.dataset_to_record(ds))
            .collect();

        info!("有效URL数量: {}", records.len());

        let duckdb = DuckDB::new(&self.config.duckdb.path).await?;
        duckdb.insert_records(&records).await?;

        // 并发监测URL
        let results = stream::iter(records)
            .map(|mut record| {
                let client = self.client.clone();
                async move {
                    match self.check_url(&client, &record.url).await {
                        Ok(status) => {
                            record.status = status;
                            record.check_time = Utc::now();
                        }
                        Err(e) => {
                            record.status = -1;
                            record.check_time = Utc::now();
                            record.error_msg = Some(e.to_string());
                        }
                    }
                    record
                }
            })
            .buffer_unordered(self.config.monitor.max_concurrent)
            .collect::<Vec<_>>()
            .await;

        // 更新状态
        duckdb.update_status(&results).await?;

        let success_count = results.iter().filter(|r| r.status == 200).count();
        info!("监测完成: 成功 {}{}", success_count, results.len());

        Ok(())
    }

    fn dataset_to_record(&self, dataset: Dataset) -> Option<MonitorRecord> {
        let url = dataset.extract_url()?;

        Some(MonitorRecord {
            id: dataset._id.map(|id| id.to_string()).unwrap_or_default(),
            raw_id: dataset.raw_id.clone(),
            url,
            name: dataset.extract_name(),
            center_name: dataset.center_name.clone()?,
            date_published: dataset.extract_date_published(),
            sync_date: dataset.sync_date?,
            check_time: Utc::now(),
            status: 0,
            error_msg: None,
        })
    }

    async fn check_url(&self, client: &reqwest::Client, url: &str) -> Result<i32> {
        let mut retries = 0;

        loop {
            match client.head(url).send().await {
                Ok(response) => {
                    return Ok(response.status().as_u16() as i32);
                }
                Err(e) if retries < self.config.monitor.retry_times => {
                    warn!("请求失败, 重试 {}/{}: {}", retries + 1, self.config.monitor.retry_times, e);
                    retries += 1;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}